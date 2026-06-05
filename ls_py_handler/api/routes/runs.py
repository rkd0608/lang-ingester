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


def serialize_run(run: Run) -> bytes:
    """Serialize a run as one JSON object."""
    return orjson.dumps(run.model_dump(mode="json"))


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_runs(
    runs: List[Run],
    db: asyncpg.Connection = Depends(get_db_conn),
    s3: Any = Depends(get_s3_client),
):
    """
    Create new runs in batch.

    Takes a JSON array of Run objects, uploads them to MinIO as NDJSON,
    and stores one object-range index row per run in PostgreSQL.
    """
    if not runs:
        raise HTTPException(status_code=400, detail="No runs provided")

    batch_id = str(uuid.uuid4())
    object_key = f"batches/{batch_id}.ndjson"
    batch_buffer = bytearray()
    records = []

    for run in runs:
        run_bytes = serialize_run(run)

        run_start = len(batch_buffer)
        batch_buffer.extend(run_bytes)
        run_end = len(batch_buffer)
        batch_buffer.extend(b"\n")

        records.append(
            (
                run.id,
                run.trace_id,
                run.name,
                object_key,
                run_start,
                run_end,
            )
        )

    batch_data = bytes(batch_buffer)

    await s3.put_object(
        Bucket=settings.S3_BUCKET_NAME,
        Key=object_key,
        Body=batch_data,
        ContentType="application/x-ndjson",
    )

    async with db.transaction():
        await db.executemany(
            """
            INSERT INTO runs (
                id,
                trace_id,
                name,
                object_key,
                object_start,
                object_end
            )
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
        SELECT
            id,
            trace_id,
            name,
            object_key,
            object_start,
            object_end
        FROM runs
        WHERE id = $1
        """,
        run_id,
    )

    if not row:
        raise HTTPException(status_code=404, detail=f"Run with ID {run_id} not found")

    run_data = dict(row)
    byte_range = f"bytes={run_data['object_start']}-{run_data['object_end'] - 1}"
    response = await s3.get_object(
        Bucket=settings.S3_BUCKET_NAME,
        Key=run_data["object_key"],
        Range=byte_range,
    )
    async with response["Body"] as stream:
        data = await stream.read()
    return orjson.loads(data)
