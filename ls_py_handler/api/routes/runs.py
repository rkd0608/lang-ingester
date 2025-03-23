import orjson
import uuid
from typing import Dict, List, Optional, Any

import asyncpg
from aiobotocore.session import get_session
from fastapi import APIRouter, status, HTTPException, Depends
from pydantic import BaseModel, Field, UUID4

from ls_py_handler.config.settings import settings

router = APIRouter(prefix="/runs", tags=["runs"])


class Run(BaseModel):
    id: Optional[UUID4] = Field(default_factory=uuid.uuid4)
    trace_id: UUID4
    name: str
    inputs: Dict[str, Any] = {}
    outputs: Dict[str, Any] = {}
    metadata: Dict[str, Any] = {}


async def get_db_conn():
    """Get a database connection."""
    conn = await asyncpg.connect(
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=settings.DB_NAME,
        host=settings.DB_HOST,
        port=settings.DB_PORT
    )
    try:
        yield conn
    finally:
        await conn.close()


async def get_s3_client():
    """Get an S3 client for MinIO."""
    session = get_session()
    async with session.create_client(
        's3',
        endpoint_url=settings.S3_ENDPOINT_URL,
        aws_access_key_id=settings.S3_ACCESS_KEY,
        aws_secret_access_key=settings.S3_SECRET_KEY,
        region_name=settings.S3_REGION
    ) as client:
        yield client


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_run(
    runs: List[Run],
    db: asyncpg.Connection = Depends(get_db_conn),
    s3: Any = Depends(get_s3_client)
):
    """
    Create new runs in batch.
    
    Takes a JSON array of Run objects, uploads them to MinIO,
    and stores references in PostgreSQL.
    """
    if not runs:
        raise HTTPException(status_code=400, detail="No runs provided")
    
    # Prepare the batch for S3 upload
    batch_id = str(uuid.uuid4())
    batch_data = orjson.dumps([run.model_dump() for run in runs])
    
    # Upload batch to MinIO
    object_key = f"batches/{batch_id}.json"
    
    # Upload the batch data
    await s3.put_object(
        Bucket=settings.S3_BUCKET_NAME,
        Key=object_key,
        Body=batch_data
    )
    
    # Store references in PG
    inserted_ids = []
    for i, run in enumerate(runs):
        # Calculate offsets in the JSON array for each run
        run_json = orjson.dumps(run.model_dump())
        # Find the position of this run in the batch data
        # Note: with binary data we need to search differently
        run_str = run_json.decode('utf-8')
        batch_str = batch_data.decode('utf-8')
        start_offset = batch_str.find(run_str)
        end_offset = start_offset + len(run_str)
        
        # Store in database with S3 references
        inputs_ref = f"s3://{settings.S3_BUCKET_NAME}/{object_key}#{start_offset}:{end_offset}/inputs"
        outputs_ref = f"s3://{settings.S3_BUCKET_NAME}/{object_key}#{start_offset}:{end_offset}/outputs"
        metadata_ref = f"s3://{settings.S3_BUCKET_NAME}/{object_key}#{start_offset}:{end_offset}/metadata"

        run_id = await db.fetchval(
            """
            INSERT INTO runs (id, trace_id, name, inputs, outputs, metadata)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
            """,
            run.id, run.trace_id, run.name, inputs_ref, outputs_ref, metadata_ref
        )
        inserted_ids.append(str(run_id))
    
    return {"status": "created", "run_ids": inserted_ids}


@router.get("/{run_id}", status_code=status.HTTP_200_OK)
async def get_run(
    run_id: UUID4,
    db: asyncpg.Connection = Depends(get_db_conn),
    s3: Any = Depends(get_s3_client)
):
    """
    Get a run by its ID.
    """
    # Fetch the run from the database
    row = await db.fetchrow(
        """
        SELECT id, trace_id, name, inputs, outputs, metadata
        FROM runs
        WHERE id = $1
        """,
        run_id
    )
    
    if not row:
        raise HTTPException(status_code=404, detail=f"Run with ID {run_id} not found")
    
    # Parse S3 references
    run_data = dict(row)
    
    # Function to fetch data from S3 based on reference
    async def fetch_from_s3(ref):
        if not ref or not ref.startswith('s3://'):
            return {}
        
        # Parse the S3 reference
        parts = ref.split('/')
        bucket = parts[2]
        key = '/'.join(parts[3:]).split('#')[0]
        
        # Extract offsets and field
        offset_part = ref.split('#')[1] if '#' in ref else None
        field = offset_part.split('/')[1] if offset_part and '/' in offset_part else None
        
        # Get the object from S3
        response = await s3.get_object(Bucket=bucket, Key=key)
        async with response['Body'] as stream:
            data = await stream.read()
            json_data = orjson.loads(data)
            
            # If it's a batch, find the specific run and field
            if isinstance(json_data, list) and offset_part:
                for item in json_data:
                    if str(item.get('id')) == str(run_id):
                        return item.get(field, {}) if field else item
            
            return json_data
    
    # Fetch the actual data from MinIO
    inputs = await fetch_from_s3(run_data['inputs'])
    outputs = await fetch_from_s3(run_data['outputs'])
    metadata = await fetch_from_s3(run_data['metadata'])
    
    return {
        "id": str(run_data['id']),
        "trace_id": str(run_data['trace_id']),
        "name": run_data['name'],
        "inputs": inputs,
        "outputs": outputs,
        "metadata": metadata
    }
