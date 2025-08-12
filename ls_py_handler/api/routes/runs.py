import asyncio
import uuid
from typing import Any, Dict, List, Optional, Tuple
import io
import time

import asyncpg
import orjson
from aiobotocore.session import get_session
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import ORJSONResponse
from pydantic import UUID4, BaseModel, Field

from ls_py_handler.config.settings import settings

router = APIRouter(prefix="/runs", tags=["runs"])

# -------------------------
# Simple in-process cache
# -------------------------
class _CacheItem:
    __slots__ = ("value", "expires_at")
    def __init__(self, value, ttl_sec: int):
        self.value = value
        self.expires_at = time.time() + ttl_sec

_cache: Dict[str, _CacheItem] = {}
_CACHE_TTL_SEC = 30
_CACHE_MAX = 512  # basic LRU-ish cap

def _cache_get(k: str):
    it = _cache.get(k)
    if not it or it.expires_at < time.time():
        _cache.pop(k, None)
        return None
    return it.value

def _cache_put(k: str, v):
    if len(_cache) > _CACHE_MAX:
        # drop an arbitrary item (good enough for a lightweight cache)
        _cache.pop(next(iter(_cache)))
    _cache[k] = _CacheItem(v, _CACHE_TTL_SEC)


class Run(BaseModel):
    id: Optional[UUID4] = Field(default_factory=uuid.uuid4)
    trace_id: UUID4
    name: str
    inputs: Dict[str, Any] = {}
    outputs: Dict[str, Any] = {}
    metadata: Dict[str, Any] = {}


async def get_db_conn():
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
    session = get_session()
    async with session.create_client(
        "s3",
        endpoint_url=settings.S3_ENDPOINT_URL,
        aws_access_key_id=settings.S3_ACCESS_KEY,
        aws_secret_access_key=settings.S3_SECRET_KEY,
        region_name=settings.S3_REGION,
    ) as client:
        yield client


def _ndjson_bytes_and_offsets(run_dicts: List[Dict[str, Any]]) -> Tuple[bytes, List[Tuple[int, int]]]:
    """
    Build NDJSON buffer where each run is one line: b'{"..."}\n'
    Returns (buffer_bytes, [(start,end_exclusive) per run]).
    """
    buf = io.BytesIO()
    offsets: List[Tuple[int, int]] = []
    cursor = 0
    for rd in run_dicts:
        line = orjson.dumps(rd) + b"\n"
        start = cursor
        buf.write(line)
        cursor += len(line)
        offsets.append((start, cursor))  # end is exclusive
    return buf.getvalue(), offsets


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_runs(
    runs: List[Run],
    db: asyncpg.Connection = Depends(get_db_conn),
    s3: Any = Depends(get_s3_client),
):
    if not runs:
        raise HTTPException(status_code=400, detail="No runs provided")

    # 1) Prepare NDJSON batch (single serialization pass)
    batch_id = str(uuid.uuid4())
    object_key = f"batches/{batch_id}.ndjson"
    run_dicts = [r.model_dump() for r in runs]
    ndjson_bytes, offsets = _ndjson_bytes_and_offsets(run_dicts)

    # 2) Upload once to S3
    await s3.put_object(
        Bucket=settings.S3_BUCKET_NAME,
        Key=object_key,
        Body=ndjson_bytes,
        ContentType="application/x-ndjson",
    )

    # 3) Bulk insert rows with (id, trace_id, name, s3_key, byte_start, byte_end)
    #    Much faster than per-row INSERT.
    records = []
    for r, (start, end) in zip(runs, offsets):
        records.append((
            str(r.id),             # id (uuid as text; PG will cast if column is uuid)
            str(r.trace_id),
            r.name,
            object_key,
            start,
            end,
        ))

    async with db.transaction():
        await db.copy_records_to_table(
            "runs",
            records=records,
            columns=["id", "trace_id", "name", "s3_key", "byte_start", "byte_end"],
        )

    # 4) Return full runs with server-assigned IDs (already set) — no re-serialization needed
    created = [{
        "id": str(r.id),
        "trace_id": str(r.trace_id),
        "name": r.name,
        "inputs": r.inputs,
        "outputs": r.outputs,
        "metadata": r.metadata,
    } for r in runs]

    # Optional: prime cache by id for quick GETs (best effort)
    for item, (start, end) in zip(created, offsets):
        # store object_key + slice in cache so GET can short-circuit S3
        _cache_put(f"run:{item['id']}", item)

    return ORJSONResponse(content=created)


@router.get("/{run_id}", status_code=status.HTTP_200_OK)
async def get_run(
    run_id: UUID4,
    db: asyncpg.Connection = Depends(get_db_conn),
    s3: Any = Depends(get_s3_client),
):
    # 0) Cache
    ck = f"run:{run_id}"
    cached = _cache_get(ck)
    if cached:
        return ORJSONResponse(content=cached)

    # 1) Fetch the run pointer only (cheap)
    row = await db.fetchrow(
        """
        SELECT id, trace_id, name, s3_key, byte_start, byte_end
        FROM runs
        WHERE id = $1
        """,
        run_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"Run with ID {run_id} not found")

    s3_key = row["s3_key"]
    start = int(row["byte_start"])
    end = int(row["byte_end"])  # exclusive

    # 2) Single range GET for exactly this run’s JSON line
    byte_range = f"bytes={start}-{end-1}"  # Range is inclusive end
    try:
        resp = await s3.get_object(Bucket=settings.S3_BUCKET_NAME, Key=s3_key, Range=byte_range)
        async with resp["Body"] as stream:
            data = await stream.read()
        # Strip trailing newline if present (NDJSON line)
        if data.endswith(b"\n"):
            data = data[:-1]
        run_obj = orjson.loads(data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch run data: {e}")

    # 3) Normalize ids to strings for JSON
    run_obj["id"] = str(run_obj.get("id", run_id))
    run_obj["trace_id"] = str(run_obj["trace_id"])

    _cache_put(ck, run_obj)
    return ORJSONResponse(content=run_obj)
