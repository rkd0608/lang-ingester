import asyncio
import uuid
from typing import Any, Dict, List, Optional

import asyncpg
import orjson
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import UUID4, BaseModel, Field

from ls_py_handler.config.settings import settings

router = APIRouter(prefix="/runs", tags=["runs"])
TRACKED_REF_FIELDS = {"inputs", "outputs", "metadata"}


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


def serialize_run_with_offsets(run: Run) -> tuple[dict[str, str], bytes, dict[str, tuple[int, int]]]:
    """Serialize a run once and record the offsets of the JSON field values."""
    run_dict = run.model_dump(mode="json")
    field_bytes = {field: orjson.dumps(value) for field, value in run_dict.items()}

    run_buffer = bytearray(b"{")
    field_offsets: dict[str, tuple[int, int]] = {}

    for index, field in enumerate(run_dict):
        if index > 0:
            run_buffer.extend(b",")

        run_buffer.extend(orjson.dumps(field))
        run_buffer.extend(b":")
        field_start = len(run_buffer)
        run_buffer.extend(field_bytes[field])
        field_end = len(run_buffer)

        if field in TRACKED_REF_FIELDS:
            field_offsets[field] = (field_start, field_end)

    run_buffer.extend(b"}")

    return run_dict, bytes(run_buffer), field_offsets


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
    object_key = f"batches/{batch_id}.json"
    batch_buffer = bytearray(b"[")
    records = []

    for index, run in enumerate(runs):
        run_dict, run_bytes, field_offsets = serialize_run_with_offsets(run)

        if index > 0:
            batch_buffer.extend(b",")

        run_start = len(batch_buffer)
        batch_buffer.extend(run_bytes)

        field_refs = {}
        for field, (field_start, field_end) in field_offsets.items():
            absolute_start = run_start + field_start
            absolute_end = run_start + field_end
            field_refs[field] = (
                f"s3://{settings.S3_BUCKET_NAME}/{object_key}"
                f"#{absolute_start}:{absolute_end}/{field}"
            )

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

    batch_buffer.extend(b"]")
    batch_data = bytes(batch_buffer)

    await s3.put_object(
        Bucket=settings.S3_BUCKET_NAME,
        Key=object_key,
        Body=batch_data,
        ContentType="application/json",
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
