import uuid
import asyncio
import random
import string
import orjson
import pytest
import pytest_asyncio
from httpx import AsyncClient

from ls_py_handler.main import app


def generate_large_string(size_kb=10):
    """Generate a random string of approximately size_kb kilobytes."""
    # 1 KB is roughly 1024 characters
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(size_kb * 1024))


def generate_large_json_dict(size_kb=10):
    """Generate a JSON-serializable dictionary of approximately size_kb kilobytes."""
    result = {}
    large_string = generate_large_string(size_kb)

    # Split the string into chunks to create dictionary entries
    chunk_size = 100
    for i in range(0, len(large_string), chunk_size):
        key = f"key_{i//chunk_size}"
        result[key] = large_string[i : i + chunk_size]

    return result


def generate_run_dict(size_kb=10):
    """Generate a dictionary for a run with large data fields, without an ID field."""
    return {
        # No id field - let server generate it
        "trace_id": str(uuid.uuid4()),
        "name": f"Benchmark Run {uuid.uuid4()}",
        "inputs": generate_large_json_dict(size_kb),
        "outputs": generate_large_json_dict(size_kb),
        "metadata": generate_large_json_dict(size_kb),
    }


def generate_batch_run_dicts(batch_size=100, size_kb=10):
    """Generate a batch of run dictionaries."""
    return [generate_run_dict(size_kb) for _ in range(batch_size)]


@pytest.fixture
def aio_benchmark(benchmark):
    """
    A fixture that allows benchmarking of async functions.

    This fixture creates a synchronous wrapper around async functions
    that can be used with pytest-benchmark.
    """

    def _wrapper(func, *args, **kwargs):
        # Create a synchronous wrapper for the async function
        def _sync_wrapper():
            # Create a new event loop for each benchmark iteration
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(func(*args, **kwargs))
            finally:
                loop.close()
                asyncio.set_event_loop(None)

        # Run the benchmark on the synchronous wrapper
        return benchmark(_sync_wrapper)

    return _wrapper


@pytest_asyncio.fixture
async def client():
    """Fixture that creates an async test client."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client


async def send_request_with_pre_serialized_json(client, serialized_json):
    """
    Send a request with pre-serialized JSON.
    This isolates the HTTP request from JSON serialization time.
    """
    # Send the request
    response = await client.post(
        "/runs", content=serialized_json, headers={"Content-Type": "application/json"}
    )
    return response


def test_create_batch_runs_500_10kb(client, aio_benchmark):
    """
    This excludes the time for run dictionary creation and JSON serialization,
    focusing only on the API call performance.
    """
    # Create the run dictionaries outside the benchmark
    run_dicts = generate_batch_run_dicts(500, 10)

    # Pre-serialize the JSON outside the benchmark
    serialized_json = orjson.dumps(run_dicts)

    # Benchmark only the HTTP request with pre-serialized JSON
    result = aio_benchmark(
        send_request_with_pre_serialized_json, client, serialized_json
    )
    assert result.status_code == 201
    assert len(result.json()["run_ids"]) == 500


def test_create_batch_runs_50_100kb(client, aio_benchmark):
    """
    This excludes the time for run dictionary creation and JSON serialization,
    focusing only on the API call performance.
    """
    # Create the run dictionaries outside the benchmark
    run_dicts = generate_batch_run_dicts(50, 100)

    # Pre-serialize the JSON outside the benchmark
    serialized_json = orjson.dumps(run_dicts)

    # Benchmark only the HTTP request with pre-serialized JSON
    result = aio_benchmark(
        send_request_with_pre_serialized_json, client, serialized_json
    )
    assert result.status_code == 201
    assert len(result.json()["run_ids"]) == 50
