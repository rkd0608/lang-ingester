import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from ls_py_handler.main import app
from ls_py_handler.api.routes.runs import Run


@pytest_asyncio.fixture
async def client():
    async with app.router.lifespan_context(app):
        async with AsyncClient(app=app, base_url="http://test") as client:
            yield client


@pytest.mark.asyncio
async def test_create_and_get_run(client):
    """
    Test the POST /runs endpoint to create multiple runs
    and the GET /runs/{run_id} endpoint to retrieve them.
    """
    # Create test data for multiple runs
    run1 = Run(
        trace_id=uuid.uuid4(),
        name="Test Run 1",
        inputs={"prompt": "What is the capital of France?"},
        outputs={"answer": "Paris"},
        metadata={"model": "gpt-4", "temperature": 0.7},
    )

    run2 = Run(
        trace_id=uuid.uuid4(),
        name="Test Run 2",
        inputs={"prompt": "Tell me about machine learning"},
        outputs={"answer": "Machine learning is a branch of AI..."},
        metadata={"model": "gpt-3.5-turbo", "temperature": 0.5},
    )

    run3 = Run(
        trace_id=uuid.uuid4(),
        name="Test Run 3",
        inputs={"prompt": "Python code example"},
        outputs={"code": "print('Hello, World!')"},
        metadata={"model": "codex", "temperature": 0.2},
    )

    # Create a list of runs to send in a batch
    runs = [run1, run2, run3]

    # Convert Run objects to dictionaries with string UUIDs
    run_dicts = []
    for run in runs:
        run_dict = run.model_dump()
        # Convert UUID objects to strings
        run_dict["id"] = str(run_dict["id"])
        run_dict["trace_id"] = str(run_dict["trace_id"])
        run_dicts.append(run_dict)

    # Create the runs
    response = await client.post("/runs", json=run_dicts)

    # Check response status and structure
    assert response.status_code == 201
    assert "status" in response.json()
    assert response.json()["status"] == "created"
    assert "run_ids" in response.json()

    # Get the returned run IDs
    run_ids = response.json()["run_ids"]
    assert len(run_ids) == 3

    # Verify we can retrieve each run individually
    for i, run_id in enumerate(run_ids):
        get_response = await client.get(f"/runs/{run_id}")

        # Check response status
        assert get_response.status_code == 200

        # Verify the run data matches what we sent
        run_data = get_response.json()
        assert run_data["id"] == run_id

        # Verify run name matches the original
        expected_name = runs[i].name
        assert run_data["name"] == expected_name

        # Verify inputs, outputs, and metadata match
        assert run_data["inputs"] == runs[i].inputs
        assert run_data["outputs"] == runs[i].outputs
        assert run_data["metadata"] == runs[i].metadata


@pytest.mark.asyncio
async def test_create_runs_rolls_back_transaction_on_insert_failure():
    duplicate_id = uuid.uuid4()
    runs = [
        {
            "id": str(duplicate_id),
            "trace_id": str(uuid.uuid4()),
            "name": "Test Run 1",
            "inputs": {"prompt": "Prompt 1"},
            "outputs": {"answer": "Answer 1"},
            "metadata": {"model": "gpt-4"},
        },
        {
            "id": str(duplicate_id),
            "trace_id": str(uuid.uuid4()),
            "name": "Test Run 2",
            "inputs": {"prompt": "Prompt 2"},
            "outputs": {"answer": "Answer 2"},
            "metadata": {"model": "gpt-4"},
        },
    ]

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post("/runs", json=runs)
            assert response.status_code == 500

            get_response = await client.get(f"/runs/{duplicate_id}")
            assert get_response.status_code == 404


@pytest.mark.asyncio
async def test_create_and_get_runs_with_duplicate_field_payloads(client):
    shared_inputs = {"prompt": "Repeated prompt"}
    shared_metadata = {"model": "gpt-4", "temperature": 0.7}

    run1 = Run(
        trace_id=uuid.uuid4(),
        name="Duplicate Payload Run 1",
        inputs=shared_inputs,
        outputs={"answer": "First"},
        metadata=shared_metadata,
    )
    run2 = Run(
        trace_id=uuid.uuid4(),
        name="Duplicate Payload Run 2",
        inputs=shared_inputs,
        outputs={"answer": "Second"},
        metadata=shared_metadata,
    )

    runs = [run1, run2]
    run_dicts = []
    for run in runs:
        run_dict = run.model_dump()
        run_dict["id"] = str(run_dict["id"])
        run_dict["trace_id"] = str(run_dict["trace_id"])
        run_dicts.append(run_dict)

    response = await client.post("/runs", json=run_dicts)
    assert response.status_code == 201

    for expected_run, run_id in zip(runs, response.json()["run_ids"]):
        get_response = await client.get(f"/runs/{run_id}")
        assert get_response.status_code == 200

        run_data = get_response.json()
        assert run_data["id"] == run_id
        assert run_data["name"] == expected_run.name
        assert run_data["inputs"] == expected_run.inputs
        assert run_data["outputs"] == expected_run.outputs
        assert run_data["metadata"] == expected_run.metadata


@pytest.mark.asyncio
async def test_create_runs_populates_run_slice_columns_without_legacy_refs(client):
    run = Run(
        trace_id=uuid.uuid4(),
        name="Run Slice Check",
        inputs={"prompt": "Check NDJSON storage"},
        outputs={"answer": "Stored"},
        metadata={"model": "gpt-4"},
    )

    run_dict = run.model_dump()
    run_dict["id"] = str(run_dict["id"])
    run_dict["trace_id"] = str(run_dict["trace_id"])

    response = await client.post("/runs", json=[run_dict])
    assert response.status_code == 201
    run_id = uuid.UUID(response.json()["run_ids"][0])

    async with app.state.db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT object_key, object_start, object_end
            FROM runs
            WHERE id = $1
            """,
            run_id,
        )

    assert row is not None
    assert row["object_key"].endswith(".ndjson")
    assert row["object_start"] == 0
    assert row["object_end"] > row["object_start"]
