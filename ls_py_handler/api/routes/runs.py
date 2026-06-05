import asyncio
import uuid
from typing import Any, Dict, List, Optional

import asyncpg
import orjson
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import UUID4, BaseModel, Field

from ls_py_handler.config.settings import settings

router = APIRouter(prefix="/runs", tags=["runs"])


class Run(BaseModel):
    id: Optional[UUID4] = Field(default_factory=uuid.uuid4)
    trace_id: UUID4
    name: str
    inputs: Dict[str, Any] = {}
    outputs: Dict[str, Any] = {}
    metadata: Dict[str, Any] = {}


async def get_db_conn(request: Request):
    """Borrow a database connection from the shared pool."""
    pool = request.app.state.db_pool
    conn = await pool.acquire()
    try:
        yield conn
    finally:
        await pool.release(conn)


async def get_s3_client(request: Request):
    """Return the shared S3 client."""
    yield request.app.state.s3


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_runs(
    runs: List[Run],
    db: asyncpg.Connection = Depends(get_db_conn),
    s3: Any = Depends(get_s3_client),
):
    """
    Create new runs in batch.

    Takes a JSON array of Run objects, uploads them to MinIO,
    and stores references to certain fields in PostgreSQL.
    """
    if not runs:
        raise HTTPException(status_code=400, detail="No runs provided")

    batch_id = str(uuid.uuid4())
    run_dicts = [run.model_dump() for run in runs]
    batch_data = orjson.dumps(run_dicts)
    object_key = f"batches/{batch_id}.json"

    await s3.put_object(
        Bucket=settings.S3_BUCKET_NAME,
        Key=object_key,
        Body=batch_data,
        ContentType="application/json",
    )

    records = []

    for i, run in enumerate(runs):
        run_dict = run_dicts[i]

        field_refs = {}
        for field in ["inputs", "outputs", "metadata"]:
            field_json_data = orjson.dumps(run_dict.get(field, {}))
            field_start_in_run = batch_data.find(field_json_data)

            if field_start_in_run != -1:
                field_start = field_start_in_run
                field_end = field_start + len(field_json_data)
                field_refs[
                    field
                ] = f"s3://{settings.S3_BUCKET_NAME}/{object_key}#{field_start}:{field_end}/{field}"
            else:
                field_refs[field] = ""

        records.append(
            (
                run.id,
                run.trace_id,
                run.name,
                field_refs["inputs"],
                field_refs["outputs"],
                field_refs["metadata"],
            )
        )

    async with db.transaction():
        await db.executemany(
            """
            INSERT INTO runs (id, trace_id, name, inputs, outputs, metadata)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            records,
        )

    return {"status": "created", "run_ids": [str(run.id) for run in runs]}


@router.get("/{run_id}", status_code=status.HTTP_200_OK)
async def get_run(
    run_id: UUID4,
    db: asyncpg.Connection = Depends(get_db_conn),
    s3: Any = Depends(get_s3_client),
):
    """
    Get a run by its ID.
    """
    # Fetch the run from the PG
    row = await db.fetchrow(
        """
        SELECT id, trace_id, name, inputs, outputs, metadata
        FROM runs
        WHERE id = $1
        """,
        run_id,
    )

    if not row:
        raise HTTPException(status_code=404, detail=f"Run with ID {run_id} not found")

    run_data = dict(row)

    def parse_s3_ref(ref):
        if not ref or not ref.startswith("s3://"):
            return None, None, None, None

        parts = ref.split("/")
        bucket = parts[2]
        key = "/".join(parts[3:]).split("#")[0]

        if "#" in ref:
            offset_part = ref.split("#")[1]
            if ":" in offset_part and "/" in offset_part:
                offsets, field = offset_part.split("/")
                start_offset, end_offset = map(int, offsets.split(":"))
                return bucket, key, (start_offset, end_offset), field

        return bucket, key, None, None

    async def fetch_from_s3(ref):
        if not ref or not ref.startswith("s3://"):
            return {}

        bucket, key, offsets, field = parse_s3_ref(ref)
        if not bucket or not key or not offsets:
            return {}

        start_offset, end_offset = offsets
        byte_range = f"bytes={start_offset}-{end_offset-1}"

        try:
            response = await s3.get_object(Bucket=bucket, Key=key, Range=byte_range)
            async with response["Body"] as stream:
                data = await stream.read()
            try:
                return orjson.loads(data)
            except Exception as parse_error:
                print(f"Error parsing JSON fragment: {parse_error}")
                print(f"Problematic data: {data}")
                return {}

        except Exception as e:
            print(f"Error fetching S3 object with range: {e}")
            return {}

    inputs, outputs, metadata = await asyncio.gather(
        fetch_from_s3(run_data["inputs"]),
        fetch_from_s3(run_data["outputs"]),
        fetch_from_s3(run_data["metadata"]),
    )

    return {
        "id": str(run_data["id"]),
        "trace_id": str(run_data["trace_id"]),
        "name": run_data["name"],
        "inputs": inputs,
        "outputs": outputs,
        "metadata": metadata,
    }
