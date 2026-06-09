from contextlib import AsyncExitStack, asynccontextmanager

import asyncpg
from aiobotocore.session import get_session
from fastapi import FastAPI
from redis.asyncio import Redis

from ls_py_handler.api.routes.runs import router as runs_router
from ls_py_handler.config.settings import settings

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and share application resources for the app lifetime."""
    exit_stack = AsyncExitStack()
    session = get_session()
    redis_client = None
    if settings.REDIS_CACHE_ENABLED:
        redis_client = Redis.from_url(settings.REDIS_URL)
    s3 = await exit_stack.enter_async_context(
        session.create_client(
            "s3",
            endpoint_url=settings.S3_ENDPOINT_URL,
            aws_access_key_id=settings.S3_ACCESS_KEY,
            aws_secret_access_key=settings.S3_SECRET_KEY,
            region_name=settings.S3_REGION,
        )
    )
    db_pool = await asyncpg.create_pool(
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=settings.DB_NAME,
        host=settings.DB_HOST,
        port=settings.DB_PORT,
    )

    app.state.s3 = s3
    app.state.db_pool = db_pool
    app.state.redis = redis_client

    try:
        await s3.create_bucket(Bucket=settings.S3_BUCKET_NAME)
        print(f"Created S3 bucket: {settings.S3_BUCKET_NAME}")
    except Exception:
        print("Tried to create S3 bucket, but it already exists. No action taken.")

    if redis_client is not None:
        try:
            await redis_client.ping()
            print("Redis connection established.")
        except Exception:
            print("Redis ping failed during startup. Continuing without a warm cache.")

    try:
        yield
    finally:
        if redis_client is not None:
            await redis_client.aclose()
        await db_pool.close()
        await exit_stack.aclose()


app = FastAPI(
    title=settings.APP_TITLE,
    description=settings.APP_DESCRIPTION,
    version=settings.APP_VERSION,
    lifespan=lifespan,
)

# Include routers
app.include_router(runs_router)


@app.get("/")
async def root():
    """
    Root endpoint to verify the API is running.
    """
    return {"message": settings.APP_TITLE + " API"}
