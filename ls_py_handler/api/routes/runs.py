import uuid
from collections.abc import AsyncIterator
from typing import Any, Dict, Optional

import asyncpg
import ijson
import orjson
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from pydantic import UUID4, BaseModel, Field, ValidationError

from ls_py_handler.config.settings import settings
from ls_py_handler.utils.connection_utils import get_s3_client, get_db_conn, get_redis_client

router = APIRouter(prefix="/runs", tags=["runs"])


class Run(BaseModel):
    id: Optional[UUID4] = Field(default_factory=uuid.uuid4)
    trace_id: UUID4
    name: str
    inputs: Dict[str, Any] = {}
    outputs: Dict[str, Any] = {}
    metadata: Dict[str, Any] = {}


class AsyncRequestStreamReader:
    """Adapt async request stream to a file-like async reader."""

    def __init__(self, stream: AsyncIterator[bytes]):
        self._stream = stream.__aiter__()
        self._buffer = bytearray()
        self._done = False

    async def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b""

        if size < 0:
            chunks = [bytes(self._buffer)] if self._buffer else []
            self._buffer.clear()
            while not self._done:
                try:
                    chunk = await self._stream.__anext__()
                except StopAsyncIteration:
                    self._done = True
                    break
                if chunk:
                    chunks.append(chunk)
            return b"".join(chunks)

        while len(self._buffer) < size and not self._done:
            try:
                chunk = await self._stream.__anext__()
            except StopAsyncIteration:
                self._done = True
                break
            if chunk:
                self._buffer.extend(chunk)

        data = bytes(self._buffer[:size])
        del self._buffer[:size]
        return data



def serialize_run(run: Run) -> bytes:
    """Serialize a run as one JSON object."""
    return orjson.dumps(run.model_dump(mode="json"))


def run_cache_key(run_id: UUID4) -> str:
    """Build the Redis cache key for a run."""
    return f"{settings.REDIS_CACHE_KEY_PREFIX}:{run_id}"


def should_cache_payload(payload: bytes) -> bool:
    """Return whether a serialized run payload should be cached."""
    return len(payload) <= settings.REDIS_CACHE_MAX_PAYLOAD_BYTES


async def iter_runs_from_request(request: Request) -> AsyncIterator[Run]:
    """Incrementally parse a JSON-array request body into validated runs."""
    stream_reader = AsyncRequestStreamReader(request.stream())
    run_count = 0

    try:
        async for run_payload in ijson.items_async(
            stream_reader,
            "item",
            use_float=True,
        ):
            run_count += 1
            try:
                yield Run.model_validate(run_payload)
            except ValidationError as exc:
                raise RequestValidationError(exc.errors()) from exc
    except (RequestValidationError, HTTPException):
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc

    if run_count == 0:
        raise HTTPException(status_code=400, detail="No runs provided")


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_runs(
    request: Request,
    db: asyncpg.Connection = Depends(get_db_conn),
    s3: Any = Depends(get_s3_client),
):
    """
    Create new runs in batch.

    Takes a JSON array of Run objects, uploads them to MinIO as NDJSON,
    and stores one object-range index row per run in PostgreSQL.
    """
    batch_id = str(uuid.uuid4())
    object_key = f"batches/{batch_id}.ndjson"
    batch_buffer = bytearray()
    records = []

    async for run in iter_runs_from_request(request):
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

    return {"status": "created", "run_ids": [str(record[0]) for record in records]}


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
            print("Failed to retrieve cached run data")

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
            print("Failed to cache run data")

    return orjson.loads(data)
