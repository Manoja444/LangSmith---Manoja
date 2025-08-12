# ls_py_handler/api/runs.py
import asyncio
import uuid
import io
import time
from tempfile import SpooledTemporaryFile
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
import orjson
from aiobotocore.session import get_session
from botocore.config import Config
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel, Field, UUID4

from ls_py_handler.config.settings import settings

router = APIRouter(prefix="/runs", tags=["runs"])

# =========================
# Small LRU+TTL cache (with NEG caching)
# =========================
class _CacheItem:
    __slots__ = ("value", "expires_at")
    def __init__(self, value, ttl_sec: int):
        self.value = value
        self.expires_at = time.time() + ttl_sec

class _LRUTTL:
    def __init__(self, maxsize: int = 1024, ttl_sec: int = 30):
        from collections import OrderedDict
        self._data: "OrderedDict[str, _CacheItem]" = OrderedDict()
        self._max = maxsize
        self._ttl = ttl_sec
        self.hits = 0
        self.misses = 0
        self.neg_hits = 0

    def get(self, k: str):
        it = self._data.get(k)
        if not it or it.expires_at < time.time():
            if it:
                self._data.pop(k, None)
            self.misses += 1
            return None
        self._data.move_to_end(k)
        v = it.value
        if v is _NEG:
            self.neg_hits += 1
        else:
            self.hits += 1
        return v

    def put(self, k: str, v):
        if k in self._data:
            self._data.move_to_end(k)
        self._data[k] = _CacheItem(v, self._ttl)
        if len(self._data) > self._max:
            self._data.popitem(last=False)

    def delete(self, k: str):
        self._data.pop(k, None)

_NEG = object()
_cache = _LRUTTL(
    maxsize=getattr(settings, "CACHE_MAX", 2048),
    ttl_sec=getattr(settings, "CACHE_TTL_SEC", 60),
)

# =========================
# App-scoped resources (wired by init_app below)
# =========================
DB_POOL: Optional[asyncpg.Pool] = None
S3_CLIENT: Any = None
S3_SEM: Optional[asyncio.Semaphore] = None

# =========================
# Models (used only for JSON-array fallback)
# =========================
class Run(BaseModel):
    id: Optional[UUID4] = Field(default=None)
    trace_id: UUID4
    name: str = ""
    inputs: Dict[str, Any] = {}
    outputs: Dict[str, Any] = {}
    metadata: Dict[str, Any] = {}

# =========================
# Dependencies
# =========================
async def get_db(request: Request) -> asyncpg.Connection:
    pool: asyncpg.Pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        yield conn

def get_s3(request: Request) -> Any:
    return request.app.state.s3_client

def get_s3_sem(request: Request) -> asyncio.Semaphore:
    return request.app.state.s3_sem

# =========================
# Helpers
# =========================
def _ck(run_id: str) -> str:
    return f"run:{run_id}"

async def _s3_put_object(s3: Any, sem: asyncio.Semaphore, *, bucket: str, key: str, body):
    async with sem:
        await s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/x-ndjson",
        )

async def _s3_get_range(s3: Any, sem: asyncio.Semaphore, *, bucket: str, key: str, start: int, end_exclusive: int) -> bytes:
    async with sem:
        resp = await s3.get_object(
            Bucket=bucket,
            Key=key,
            Range=f"bytes={start}-{end_exclusive-1}",
        )
        async with resp["Body"] as stream:
            return await stream.read()

def _ndjson_bytes_and_offsets(run_dicts: List[Dict[str, Any]]) -> Tuple[bytes, List[Tuple[int, int]]]:
    buf = io.BytesIO()
    offsets: List[Tuple[int, int]] = []
    cursor = 0
    for rd in run_dicts:
        if not rd.get("trace_id"):
            raise ValueError("trace_id required")
        if not rd.get("id"):
            rd["id"] = str(uuid.uuid4())
        line = orjson.dumps(rd) + b"\n"
        start = cursor
        buf.write(line)
        cursor += len(line)
        offsets.append((start, cursor))
    return buf.getvalue(), offsets

# =========================
# POST /runs
# - Preferred: NDJSON streaming (application/x-ndjson or text/plain)
# - Fallback: JSON array (loads into memory)
# =========================
@router.post("", status_code=status.HTTP_201_CREATED)
async def create_runs(
    request: Request,
    db: asyncpg.Connection = Depends(get_db),
    s3: Any = Depends(get_s3),
    s3_sem: asyncio.Semaphore = Depends(get_s3_sem),
):
    t0 = time.perf_counter()

    ct = (request.headers.get("content-type") or "").lower()
    batch_id = str(uuid.uuid4())
    object_key = f"batches/{batch_id}.ndjson"

    # Limits & config
    MAX_POST_BYTES = getattr(settings, "MAX_POST_BYTES", 100 * 1024 * 1024)    # 100MB
    MAX_LINE_BYTES = getattr(settings, "MAX_LINE_BYTES", 1 * 1024 * 1024)      # 1MB
    COPY_BATCH = getattr(settings, "DB_COPY_BATCH", 1000)

    content_len = request.headers.get("content-length")
    if content_len and int(content_len) > MAX_POST_BYTES:
        raise HTTPException(status_code=413, detail="Payload too large")

    # Will store (id, trace_id, name, s3_key, start, end)
    records: List[Tuple[str, str, str, str, int, int]] = []
    flushed_count = 0

    async def flush_batch():
        nonlocal records, flushed_count
        if not records:
            return
        async with db.transaction():
            await db.copy_records_to_table(
                "runs",
                records=records,
                columns=["id", "trace_id", "name", "s3_key", "byte_start", "byte_end"],
            )
        flushed_count += len(records)
        # Evict any NEG entries for these IDs
        for rid, *_ in records:
            _cache.delete(_ck(rid))
        records.clear()

    if "application/x-ndjson" in ct or "text/plain" in ct:
        # Stream NDJSON safely (chunk-split proof)
        pending = b""
        cursor = 0
        line_cap = MAX_LINE_BYTES
        total_bytes = 0

        with SpooledTemporaryFile(max_size=64 * 1024 * 1024, mode="w+b") as f:
            async for chunk in request.stream():
                if not chunk:
                    continue
                total_bytes += len(chunk)
                if total_bytes > MAX_POST_BYTES:
                    raise HTTPException(status_code=413, detail="Payload too large")

                buf = pending + bytes(chunk)
                lines = buf.split(b"\n")
                pending = lines.pop()  # last may be partial
                for raw_line in lines:  # full lines only
                    if not raw_line:
                        continue
                    if len(raw_line) > line_cap:
                        raise HTTPException(status_code=400, detail="Single record too large")
                    rd = orjson.loads(raw_line)
                    if not rd.get("trace_id"):
                        raise HTTPException(status_code=400, detail="trace_id required")
                    if not rd.get("id"):
                        rd["id"] = str(uuid.uuid4())
                        raw_line = orjson.dumps(rd)  # keep S3 content consistent with DB
                    start = cursor
                    f.write(raw_line + b"\n")
                    cursor += len(raw_line) + 1
                    records.append(
                        (str(rd["id"]), str(rd["trace_id"]), rd.get("name") or "", object_key, start, cursor)
                    )
                    if len(records) >= COPY_BATCH:
                        await flush_batch()

            # handle trailing partial line (if client forgot newline)
            if pending:
                if len(pending) > line_cap:
                    raise HTTPException(status_code=400, detail="Single record too large")
                rd = orjson.loads(pending)
                if not rd.get("trace_id"):
                    raise HTTPException(status_code=400, detail="trace_id required")
                if not rd.get("id"):
                    rd["id"] = str(uuid.uuid4())
                raw_line = orjson.dumps(rd)
                start = cursor
                f.write(raw_line + b"\n")
                cursor += len(raw_line) + 1
                records.append(
                    (str(rd["id"]), str(rd["trace_id"]), rd.get("name") or "", object_key, start, cursor)
                )
                if len(records) >= COPY_BATCH:
                    await flush_batch()

            f.seek(0)
            await _s3_put_object(
                s3,
                s3_sem,
                bucket=settings.S3_BUCKET_NAME,
                key=object_key,
                body=f,
            )

    else:
        # Fallback: JSON array (compat mode; uses memory)
        try:
            runs_json = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON")
        if not isinstance(runs_json, list) or not runs_json:
            raise HTTPException(status_code=400, detail="Body must be a non-empty JSON array or NDJSON stream")

        # Validate + create IDs; build NDJSON buffer and offsets
        for rd in runs_json:
            if not isinstance(rd, dict):
                raise HTTPException(status_code=400, detail="Each item must be an object")
            if not rd.get("trace_id"):
                raise HTTPException(status_code=400, detail="trace_id required")
            if not rd.get("id"):
                rd["id"] = str(uuid.uuid4())

        ndjson_bytes, offsets = _ndjson_bytes_and_offsets(runs_json)
        await _s3_put_object(
            s3, s3_sem, bucket=settings.S3_BUCKET_NAME, key=object_key, body=ndjson_bytes
        )

        for rd, (start, end) in zip(runs_json, offsets):
            records.append(
                (str(rd["id"]), str(rd["trace_id"]), rd.get("name") or "", object_key, start, end)
            )
            if len(records) >= COPY_BATCH:
                await flush_batch()

    # Final DB flush
    await flush_batch()

    t1 = time.perf_counter()
    # Lean response: batch_id + counts + ids
    return ORJSONResponse(
        content={
            "batch_id": batch_id,
            "count": flushed_count,
            "run_ids": [r for (r, *_rest) in []],  # keep response small; IDs are in DB
            "timings_ms": {"total": int((t1 - t0) * 1000)},
        }
    )

# =========================
# GET /runs/{id}
# =========================
@router.get("/{run_id}", status_code=status.HTTP_200_OK)
async def get_run(
    run_id: UUID4,
    db: asyncpg.Connection = Depends(get_db),
    s3: Any = Depends(get_s3),
    s3_sem: asyncio.Semaphore = Depends(get_s3_sem),
):
    ck = _ck(str(run_id))
    cached = _cache.get(ck)
    if cached is _NEG:
        raise HTTPException(status_code=404, detail=f"Run with ID {run_id} not found")
    if cached is not None:
        return ORJSONResponse(content=cached)

    row = await db.fetchrow(
        """
        SELECT id, trace_id, name, s3_key, byte_start, byte_end
        FROM runs
        WHERE id = $1
        """,
        run_id,
    )
    if not row:
        _cache.put(ck, _NEG)
        raise HTTPException(status_code=404, detail=f"Run with ID {run_id} not found")

    s3_key = row["s3_key"]
    byte_start = int(row["byte_start"])
    byte_end = int(row["byte_end"])

    try:
        data = await _s3_get_range(
            s3, s3_sem,
            bucket=settings.S3_BUCKET_NAME,
            key=s3_key,
            start=byte_start,
            end_exclusive=byte_end,
        )
        if data.endswith(b"\n"):
            data = data[:-1]
        run_obj = orjson.loads(data)
        run_obj["id"] = str(run_obj.get("id", run_id))
        run_obj["trace_id"] = str(run_obj["trace_id"])
        _cache.put(ck, run_obj)
        return ORJSONResponse(content=run_obj)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch run data: {e}")

# =========================
# Lifecycle wiring
# Call init_app(app) once from your FastAPI main.py after creating `app`.
# =========================
async def _startup(app):
    global DB_POOL, S3_CLIENT, S3_SEM
    DB_POOL = await asyncpg.create_pool(
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=settings.DB_NAME,
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        min_size=getattr(settings, "DB_POOL_MIN", 4),
        max_size=getattr(settings, "DB_POOL_MAX", 16),
        timeout=10,
    )

    session = get_session()
    S3_CLIENT = await session.create_client(
        "s3",
        endpoint_url=settings.S3_ENDPOINT_URL,
        aws_access_key_id=settings.S3_ACCESS_KEY,
        aws_secret_access_key=settings.S3_SECRET_KEY,
        region_name=settings.S3_REGION,
        config=Config(
            read_timeout=20,
            connect_timeout=5,
            retries={"max_attempts": 5, "mode": "adaptive"},
        ),
    ).__aenter__()

    S3_SEM = asyncio.Semaphore(getattr(settings, "S3_MAX_CONCURRENCY", 16))

    app.state.db_pool = DB_POOL
    app.state.s3_client = S3_CLIENT
    app.state.s3_sem = S3_SEM

async def _shutdown(app):
    try:
        await app.state.s3_client.__aexit__(None, None, None)
    except Exception:
        pass
    try:
        await app.state.db_pool.close()
    except Exception:
        pass

def init_app(app):
    app.add_event_handler("startup", lambda: _startup(app))
    app.add_event_handler("shutdown", lambda: _shutdown(app))
    app.include_router(router)
