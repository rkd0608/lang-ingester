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


async def get_redis_client(request: Request):
    """Return the shared Redis client."""
    yield request.app.state.redis


def serialize_run(run: Run) -> bytes:
    """Serialize a run as one JSON object."""
    return orjson.dumps(run.model_dump(mode="json"))


def run_cache_key(run_id: UUID4) -> str:
    """Build the Redis cache key for a run."""
    return f"{settings.REDIS_CACHE_KEY_PREFIX}:{run_id}"


def should_cache_payload(payload: bytes) -> bool:
    """Return whether a serialized run payload should be cached."""
    return len(payload) <= settings.REDIS_CACHE_MAX_PAYLOAD_BYTES


async def upload_multipart_batch(
    runs: List[Run],
    object_key: str,
    s3: Any,
) -> List[tuple[UUID4, UUID4, str, str, int, int]]:
    """Upload a batch object via multipart upload and return DB index records."""
    records = []
    current_offset = 0
    part_number = 1
    part_buffer = bytearray()
    uploaded_parts = []
    upload_id = None

    async def flush_part() -> None:
        nonlocal part_number
        if not part_buffer:
            return

        response = await s3.upload_part(
            Bucket=settings.S3_BUCKET_NAME,
            Key=object_key,
            PartNumber=part_number,
            UploadId=upload_id,
            Body=bytes(part_buffer),
        )
        uploaded_parts.append(
            {
                "ETag": response["ETag"],
                "PartNumber": part_number,
            }
        )
        part_number += 1
        part_buffer.clear()

    try:
        response = await s3.create_multipart_upload(
            Bucket=settings.S3_BUCKET_NAME,
            Key=object_key,
            ContentType="application/x-ndjson",
        )
        upload_id = response["UploadId"]

        for run in runs:
            run_bytes = serialize_run(run)
            run_start = current_offset
            run_end = run_start + len(run_bytes)

            part_buffer.extend(run_bytes)
            part_buffer.extend(b"\n")
            current_offset = run_end + 1

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

            if len(part_buffer) >= settings.S3_MULTIPART_PART_SIZE_BYTES:
                await flush_part()

        await flush_part()
        await s3.complete_multipart_upload(
            Bucket=settings.S3_BUCKET_NAME,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": uploaded_parts},
        )
    except Exception:
        if upload_id is not None:
            try:
                await s3.abort_multipart_upload(
                    Bucket=settings.S3_BUCKET_NAME,
                    Key=object_key,
                    UploadId=upload_id,
                )
            except Exception:
                pass
        raise

    return records


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
    records = await upload_multipart_batch(runs, object_key, s3)

    try:
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
    except Exception:
        try:
            await s3.delete_object(Bucket=settings.S3_BUCKET_NAME, Key=object_key)
        except Exception:
            pass
        raise

    return {"status": "created", "run_ids": [str(run.id) for run in runs]}


@router.get("/{run_id}", status_code=status.HTTP_200_OK)
async def get_run(
    run_id: UUID4,
    db: asyncpg.Connection = Depends(get_db_conn),
    s3: Any = Depends(get_s3_client),
    redis: Any = Depends(get_redis_client),
):
    """
    Get a run by its ID.
    """
    if settings.REDIS_CACHE_ENABLED and redis is not None:
        try:
            cached_data = await redis.get(run_cache_key(run_id))
            if cached_data is not None:
                return orjson.loads(cached_data)
        except Exception:
            pass

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

    if settings.REDIS_CACHE_ENABLED and redis is not None and should_cache_payload(data):
        try:
            await redis.set(
                run_cache_key(run_id),
                data,
                ex=settings.REDIS_CACHE_TTL_SECONDS,
            )
        except Exception:
            pass

    return orjson.loads(data)
