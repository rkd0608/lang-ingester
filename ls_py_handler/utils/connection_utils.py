
from fastapi import Request

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