import pytest
from httpx import AsyncClient
import uuid
from ls_py_handler.main import app


@pytest.mark.asyncio
async def test_create_run():
    """
    Test the POST /runs endpoint.
    """
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.post("/runs")
        assert response.status_code == 201
        assert "status" in response.json()
        assert response.json()["status"] == "created"


@pytest.mark.asyncio
async def test_get_run():
    """
    Test the GET /runs/{id} endpoint.
    """
    # Generate a random UUID for testing
    test_uuid = uuid.uuid4()
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get(f"/runs/{test_uuid}")
        assert response.status_code == 200
        assert "status" in response.json()
        assert response.json()["status"] == "retrieved"
        assert "id" in response.json()
        assert response.json()["id"] == str(test_uuid)
