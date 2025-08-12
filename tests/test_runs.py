import uuid
import pytest
from httpx import AsyncClient

from ls_py_handler.main import app
from ls_py_handler.api.routes.runs import Run


@pytest.mark.asyncio
async def test_create_and_get_run():
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

    async with AsyncClient(app=app, base_url="http://test") as client:
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
