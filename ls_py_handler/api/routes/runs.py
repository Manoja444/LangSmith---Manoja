import asyncio
import uuid
from typing import Any, Dict, List, Optional

import asyncpg
import orjson
from aiobotocore.session import get_session
from fastapi import APIRouter, Depends, HTTPException, status
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


async def get_db_conn():
    """Get a database connection."""
    conn = await asyncpg.connect(
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=settings.DB_NAME,
        host=settings.DB_HOST,
        port=settings.DB_PORT,
    )
    try:
        yield conn
    finally:
        await conn.close()


async def get_s3_client():
    """Get an S3 client for MinIO."""
    session = get_session()
    async with session.create_client(
        "s3",
        endpoint_url=settings.S3_ENDPOINT_URL,
        aws_access_key_id=settings.S3_ACCESS_KEY,
        aws_secret_access_key=settings.S3_SECRET_KEY,
        region_name=settings.S3_REGION,
    ) as client:
        yield client


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_runs(
    runs: List[Run],
    db: asyncpg.Connection = Depends(get_db_conn),
    s3: Any = Depends(get_s3_client),
):
    """
    Create new runs in batch.

    Takes a JSON array of Run objects, uploads them to MinIO,
    and stores references to certain fields in PostgreSQL.
    """
    if not runs:
        raise HTTPException(status_code=400, detail="No runs provided")

    # Prepare the batch for S3 upload
    batch_id = str(uuid.uuid4())

    run_dicts = [run.model_dump() for run in runs]
    batch_data = orjson.dumps(run_dicts)

    object_key = f"batches/{batch_id}.json"

    # Upload the batch data
    await s3.put_object(
        Bucket=settings.S3_BUCKET_NAME,
        Key=object_key,
        Body=batch_data,
        ContentType="application/json",
    )

    # Store references in PG
    inserted_ids = []

    for i, run in enumerate(runs):
        run_dict = run_dicts[i]

        # Calculate specific offsets for each field in the JSON using a loop
        field_refs = {}
        for field in ["inputs", "outputs", "metadata"]:
            field_json_data = orjson.dumps(run_dict.get(field, {}))
            field_start_in_run = batch_data.find(field_json_data)

            if field_start_in_run != -1:
                field_start = field_start_in_run
                field_end = field_start + len(field_json_data)
                field_refs[
                    field
                ] = f"s3://{settings.S3_BUCKET_NAME}/{object_key}#{field_start}:{field_end}/{field}"
            else:
                field_refs[field] = ""

        run_id = await db.fetchval(
            """
            INSERT INTO runs (id, trace_id, name, inputs, outputs, metadata)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
            """,
            run.id,
            run.trace_id,
            run.name,
            field_refs["inputs"],
            field_refs["outputs"],
            field_refs["metadata"],
        )
        inserted_ids.append(str(run_id))

    return {"status": "created", "run_ids": inserted_ids}


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
        SELECT id, trace_id, name, inputs, outputs, metadata
        FROM runs
        WHERE id = $1
        """,
        run_id,
    )

    if not row:
        raise HTTPException(status_code=404, detail=f"Run with ID {run_id} not found")

    run_data = dict(row)

    # Function to parse S3 reference
    def parse_s3_ref(ref):
        if not ref or not ref.startswith("s3://"):
            return None, None, None, None

        parts = ref.split("/")
        bucket = parts[2]
        key = "/".join(parts[3:]).split("#")[0]

        if "#" in ref:
            offset_part = ref.split("#")[1]
            if ":" in offset_part and "/" in offset_part:
                offsets, field = offset_part.split("/")
                start_offset, end_offset = map(int, offsets.split(":"))
                return bucket, key, (start_offset, end_offset), field

        return bucket, key, None, None

    # Function to fetch data from S3 based on reference with byte range
    async def fetch_from_s3(ref):
        if not ref or not ref.startswith("s3://"):
            return {}

        bucket, key, offsets, field = parse_s3_ref(ref)
        if not bucket or not key or not offsets:
            return {}

        start_offset, end_offset = offsets
        byte_range = f"bytes={start_offset}-{end_offset-1}"

        try:
            # Fetch only the required byte range
            response = await s3.get_object(Bucket=bucket, Key=key, Range=byte_range)
            async with response["Body"] as stream:
                data = await stream.read()
            try:
                # The data should be a valid JSON object corresponding to the field
                # (inputs, outputs, or metadata) without needing further extraction
                return orjson.loads(data)
            except Exception as parse_error:
                print(f"Error parsing JSON fragment: {parse_error}")
                print(f"Problematic data: {data}")
                return {}

        except Exception as e:
            print(f"Error fetching S3 object with range: {e}")
            return {}

    inputs, outputs, metadata = await asyncio.gather(
        fetch_from_s3(run_data["inputs"]),
        fetch_from_s3(run_data["outputs"]),
        fetch_from_s3(run_data["metadata"]),
    )

    return {
        "id": str(run_data["id"]),
        "trace_id": str(run_data["trace_id"]),
        "name": run_data["name"],
        "inputs": inputs,
        "outputs": outputs,
        "metadata": metadata,
    }
