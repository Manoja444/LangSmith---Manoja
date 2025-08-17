# ls_py_handler/api/routes/runs.py
import asyncio
import contextlib
import io
import gzip
import time
import uuid
import logging
import os
from tempfile import SpooledTemporaryFile
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
import orjson
from aiobotocore.session import get_session
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel, Field, UUID4

from ls_py_handler.config.settings import settings

# ------------ logging ------------
logger = logging.getLogger("runs")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

# Optional Prometheus metrics (safe if lib not installed)
try:
    from prometheus_client import Counter, Histogram  # type: ignore
except Exception:  # pragma: no cover
    Counter = Histogram = None  # type: ignore

REQ_LAT = Histogram("api_latency_ms", "API latency (ms)", ["route", "method"]) if Histogram else None
CACHE_HIT = Counter("runs_cache_hit_total", "Run cache hits") if Counter else None
CACHE_MISS = Counter("runs_cache_miss_total", "Run cache misses") if Counter else None
CACHE_NEG = Counter("runs_cache_negative_total", "Run cache negative hits") if Counter else None

router = APIRouter(prefix="/runs", tags=["runs"])

# ------------ small LRU+TTL cache ------------
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
    def get(self, k: str):
        it = self._data.get(k)
        if not it or it.expires_at < time.time():
            if it:
                self._data.pop(k, None)
            return None
        self._data.move_to_end(k)
        return it.value
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

def _ck(run_id: str) -> str:
    return f"run:{run_id}"

# ------------ DB schema (create + migrate) ------------
BASE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
  id UUID PRIMARY KEY,
  trace_id UUID NOT NULL,
  name TEXT NOT NULL DEFAULT '',
  s3_key TEXT NOT NULL,
  byte_start INTEGER NOT NULL,
  byte_end INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_trace_id ON runs (trace_id);
"""

# If you REALLY want a clean reset, set env RUNS_DROP_AND_RECREATE=1
DROP_AND_RECREATE = os.getenv("RUNS_DROP_AND_RECREATE") == "1"

MIGRATIONS = [
    # add missing columns
    "ALTER TABLE runs ADD COLUMN IF NOT EXISTS name TEXT DEFAULT ''",
    "ALTER TABLE runs ADD COLUMN IF NOT EXISTS s3_key TEXT",
    "ALTER TABLE runs ADD COLUMN IF NOT EXISTS byte_start INTEGER",
    "ALTER TABLE runs ADD COLUMN IF NOT EXISTS byte_end INTEGER",
    # backfill nulls (in case old rows exist)
    "UPDATE runs SET name = COALESCE(name, '')",
    "UPDATE runs SET s3_key = COALESCE(s3_key, '')",
    "UPDATE runs SET byte_start = COALESCE(byte_start, 0)",
    "UPDATE runs SET byte_end = COALESCE(byte_end, 0)",
    # enforce NOT NULL after backfill
    "ALTER TABLE runs ALTER COLUMN name SET NOT NULL",
    "ALTER TABLE runs ALTER COLUMN s3_key SET NOT NULL",
    "ALTER TABLE runs ALTER COLUMN byte_start SET NOT NULL",
    "ALTER TABLE runs ALTER COLUMN byte_end SET NOT NULL",
    # ensure PK exists on id (if an old table lacked it)
    """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'runs'::regclass
           AND contype = 'p'
      ) THEN
        BEGIN
          ALTER TABLE runs ADD PRIMARY KEY (id);
        EXCEPTION WHEN duplicate_table THEN
          -- ignore
        END;
      END IF;
    END
    $$;
    """,
    # ensure index
    "CREATE INDEX IF NOT EXISTS idx_runs_trace_id ON runs (trace_id)",
]

async def _ensure_db_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        if DROP_AND_RECREATE:
            await conn.execute("DROP TABLE IF EXISTS runs CASCADE;")
        await conn.execute(BASE_SCHEMA_SQL)
        # run migrations one by one; ignore errors that only occur if already applied
        for sql in MIGRATIONS:
            try:
                await conn.execute(sql)
            except Exception as e:
                # log and continue; the goal is to be self-healing
                logger.debug("Migration step ignored error: %s", e)

# ------------ models ------------
class Run(BaseModel):
    id: Optional[UUID4] = Field(default=None)
    trace_id: UUID4
    name: str = ""
    inputs: Dict[str, Any] = {}
    outputs: Dict[str, Any] = {}
    metadata: Dict[str, Any] = {}

# ------------ dependencies ------------
async def get_db(request: Request) -> asyncpg.Connection:
    pool: asyncpg.Pool = request.app.state.db_pool
    try:
        conn = await asyncio.wait_for(pool.acquire(), timeout=5.0)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=503, detail="Database acquire timed out")
    try:
        yield conn
    finally:
        await pool.release(conn)

def get_s3(request: Request) -> Any:
    return request.app.state.s3_client

def get_s3_sem(request: Request) -> asyncio.Semaphore:
    return request.app.state.s3_sem

# ------------ helpers ------------
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

async def _s3_get_range(s3: Any, sem: asyncio.Semaphore, *, bucket: str, key: str, start: int, end_exclusive: int) -> bytes:
    async with sem:
        resp = await s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end_exclusive-1}")
        async with resp["Body"] as stream:
            return await stream.read()

# --- S3 upload helpers: optional gzip + automatic multipart ---
MULTIPART_THRESHOLD = getattr(settings, "S3_MULTIPART_THRESHOLD_BYTES", 8 * 1024 * 1024)  # 8MB
MULTIPART_CHUNK_SIZE = getattr(settings, "S3_MULTIPART_CHUNK_BYTES", 8 * 1024 * 1024)    # 8MB
S3_COMPRESS = getattr(settings, "S3_COMPRESS", True)

async def _s3_put_object_auto(s3: Any, sem: asyncio.Semaphore, *, bucket: str, key: str, body_file):
    body_file.seek(0, io.SEEK_END)
    body_file.seek(0)
    if S3_COMPRESS:
        with SpooledTemporaryFile(max_size=64 * 1024 * 1024, mode="w+b") as gz:
            with gzip.GzipFile(fileobj=gz, mode="wb") as gzout:
                while True:
                    chunk = body_file.read(1024 * 1024)
                    if not chunk:
                        break
                    gzout.write(chunk)
            gz.seek(0)
            return await _s3_put_object_choose(s3, sem, bucket=bucket, key=key, body_file=gz, content_encoding="gzip")
    else:
        return await _s3_put_object_choose(s3, sem, bucket=bucket, key=key, body_file=body_file, content_encoding=None)

async def _s3_put_object_choose(s3: Any, sem: asyncio.Semaphore, *, bucket: str, key: str, body_file, content_encoding: Optional[str]):
    body_file.seek(0, io.SEEK_END)
    size = body_file.tell()
    body_file.seek(0)
    if size < MULTIPART_THRESHOLD:
        return await _s3_put_object_simple(s3, sem, bucket=bucket, key=key, body=body_file, content_encoding=content_encoding)
    else:
        return await _s3_put_object_multipart(s3, sem, bucket=bucket, key=key, body_file=body_file, size=size, content_encoding=content_encoding)

async def _s3_put_object_simple(s3: Any, sem: asyncio.Semaphore, *, bucket: str, key: str, body, content_encoding: Optional[str]):
    async with sem:
        kwargs = dict(Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson")
        if content_encoding:
            kwargs["ContentEncoding"] = content_encoding
        await s3.put_object(**kwargs)

async def _s3_put_object_multipart(s3: Any, sem: asyncio.Semaphore, *, bucket: str, key: str, body_file, size: int, content_encoding: Optional[str]):
    async with sem:
        create_kwargs = dict(Bucket=bucket, Key=key)
        if content_encoding:
            create_kwargs["ContentEncoding"] = content_encoding
        mp = await s3.create_multipart_upload(**create_kwargs)

    parts = []
    part_no = 1
    while True:
        chunk = body_file.read(MULTIPART_CHUNK_SIZE)
        if not chunk:
            break
        async with sem:
            resp = await s3.upload_part(Bucket=bucket, Key=key, PartNumber=part_no, UploadId=mp["UploadId"], Body=chunk)
        parts.append({"ETag": resp["ETag"], "PartNumber": part_no})
        part_no += 1

    async with sem:
        await s3.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=mp["UploadId"], MultipartUpload={"Parts": parts}
        )

# ---- small timeout helper ----
async def _with_timeout(coro, seconds: float, msg: str):
    try:
        return await asyncio.wait_for(coro, timeout=seconds)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail=msg)

# ============ POST /runs ============
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
    logger.info("POST /runs batch_id=%s ct=%s", batch_id, ct)

    MAX_POST_BYTES = getattr(settings, "MAX_POST_BYTES", 100 * 1024 * 1024)  # 100MB
    MAX_LINE_BYTES = getattr(settings, "MAX_LINE_BYTES", 1 * 1024 * 1024)    # 1MB
    COPY_BATCH = getattr(settings, "DB_COPY_BATCH", 1000)

    content_len = request.headers.get("content-length")
    if content_len and int(content_len) > MAX_POST_BYTES:
        raise HTTPException(status_code=413, detail="Payload too large")

    records: List[Tuple[str, str, str, str, int, int]] = []
    flushed_count = 0

    async def flush_batch():
        nonlocal records, flushed_count
        if not records:
            return
        async with db.transaction():
            await _with_timeout(
                db.copy_records_to_table(
                    "runs",
                    records=records,
                    columns=["id", "trace_id", "name", "s3_key", "byte_start", "byte_end"],
                ),
                10.0,
                "Database write timed out",
            )
        flushed_count += len(records)
        for rid, *_ in records:
            _cache.delete(_ck(rid))
        records.clear()

    if "application/x-ndjson" in ct or "text/plain" in ct:
        pending = b""
        cursor = 0
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
                pending = lines.pop()
                for raw_line in lines:
                    if not raw_line:
                        continue
                    if len(raw_line) > MAX_LINE_BYTES:
                        raise HTTPException(status_code=400, detail="Single record too large")
                    rd = orjson.loads(raw_line)
                    if not rd.get("trace_id"):
                        raise HTTPException(status_code=400, detail="trace_id required")
                    if not rd.get("id"):
                        rd["id"] = str(uuid.uuid4())
                        raw_line = orjson.dumps(rd)
                    start = cursor
                    f.write(raw_line + b"\n")
                    cursor += len(raw_line) + 1
                    records.append((str(rd["id"]), str(rd["trace_id"]), rd.get("name") or "", object_key, start, cursor))
                    if len(records) >= COPY_BATCH:
                        await flush_batch()

            if pending:
                if len(pending) > MAX_LINE_BYTES:
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
                records.append((str(rd["id"]), str(rd["trace_id"]), rd.get("name") or "", object_key, start, cursor))
                if len(records) >= COPY_BATCH:
                    await flush_batch()

            f.seek(0)
            await _with_timeout(
                _s3_put_object_auto(s3, s3_sem, bucket=settings.S3_BUCKET_NAME, key=object_key, body_file=f),
                15.0,
                "S3 upload timed out",
            )

    else:
        # JSON array fallback
        try:
            runs_json = await _with_timeout(request.json(), 10.0, "Request JSON parse timed out")
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON")
        if not isinstance(runs_json, list) or not runs_json:
            raise HTTPException(status_code=400, detail="Body must be a non-empty JSON array or NDJSON stream")

        for rd in runs_json:
            if not isinstance(rd, dict):
                raise HTTPException(status_code=400, detail="Each item must be an object")
            if not rd.get("trace_id"):
                raise HTTPException(status_code=400, detail="trace_id required")
            if not rd.get("id"):
                rd["id"] = str(uuid.uuid4())

        ndjson_bytes, offsets = _ndjson_bytes_and_offsets(runs_json)
        with SpooledTemporaryFile(max_size=64 * 1024 * 1024, mode="w+b") as f2:
            f2.write(ndjson_bytes)
            f2.seek(0)
            await _with_timeout(
                _s3_put_object_auto(s3, s3_sem, bucket=settings.S3_BUCKET_NAME, key=object_key, body_file=f2),
                15.0,
                "S3 upload timed out",
            )

        for rd, (start, end) in zip(runs_json, offsets):
            records.append((str(rd["id"]), str(rd["trace_id"]), rd.get("name") or "", object_key, start, end))
            if len(records) >= COPY_BATCH:
                await flush_batch()

    await flush_batch()

    t1 = time.perf_counter()
    if REQ_LAT:
        REQ_LAT.labels(route="/runs", method="POST").observe((t1 - t0) * 1000.0)
    logger.info("POST /runs inserted=%d took_ms=%d", flushed_count, int((t1 - t0) * 1000))

    return ORJSONResponse(content={"batch_id": batch_id, "inserted": flushed_count, "timings_ms": {"total": int((t1 - t0) * 1000)}})

# ============ GET /runs/{id} ============
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
        if CACHE_NEG: CACHE_NEG.inc()
        raise HTTPException(status_code=404, detail=f"Run with ID {run_id} not found")
    if cached is not None:
        if CACHE_HIT: CACHE_HIT.inc()
        return ORJSONResponse(content=cached)

    if CACHE_MISS: CACHE_MISS.inc()

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
        if CACHE_NEG: CACHE_NEG.inc()
        raise HTTPException(status_code=404, detail=f"Run with ID {run_id} not found")

    s3_key = row["s3_key"]
    byte_start = int(row["byte_start"])
    byte_end = int(row["byte_end"])

    data = await _with_timeout(
        _s3_get_range(s3, s3_sem, bucket=settings.S3_BUCKET_NAME, key=s3_key, start=byte_start, end_exclusive=byte_end),
        10.0,
        "S3 read timed out",
    )
    if data.endswith(b"\n"):
        data = data[:-1]
    run_obj = orjson.loads(data)
    run_obj["id"] = str(run_obj.get("id", run_id))
    run_obj["trace_id"] = str(run_obj["trace_id"])
    _cache.put(ck, run_obj)
    return ORJSONResponse(content=run_obj)

# ============ Health (instant) ============
@router.get("/_health", include_in_schema=False)
async def runs_health():
    # Do NOT touch DB/S3 here; this should always return fast.
    return {"ok": True}

# ============ lifecycle wiring ============
async def _startup(app):
    # DB pool
    app.state.db_pool = await asyncpg.create_pool(
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=settings.DB_NAME,
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        min_size=getattr(settings, "DB_POOL_MIN", 4),
        max_size=getattr(settings, "DB_POOL_MAX", 16),
        timeout=10,
    )
    await _ensure_db_schema(app.state.db_pool)

    # S3 client
    session = get_session()
    cfg = Config(
        signature_version="s3v4",
        read_timeout=20,
        connect_timeout=5,
        retries={"max_attempts": 5, "mode": "adaptive"},
        s3={"addressing_style": "path"},  # MinIO compatibility
    )
    endpoint = getattr(settings, "S3_ENDPOINT_URL", None) or None
    region = getattr(settings, "S3_REGION", "us-east-1")

    s3_cm = session.create_client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=getattr(settings, "S3_ACCESS_KEY", None),
        aws_secret_access_key=getattr(settings, "S3_SECRET_KEY", None),
        region_name=region,
        config=cfg,
    )
    s3 = await s3_cm.__aenter__()  # keep the client open for app lifetime

    try:
        app.state.s3_client_cm = s3_cm
        app.state.s3_client = s3
        app.state.s3_sem = asyncio.Semaphore(getattr(settings, "S3_MAX_CONCURRENCY", 16))

        # Ensure bucket exists (avoid HeadBucket quirks)
        is_aws = not bool(endpoint)
        async def _create_bucket():
            kwargs: Dict[str, Any] = {"Bucket": settings.S3_BUCKET_NAME}
            if is_aws and region != "us-east-1":
                kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
            await s3.create_bucket(**kwargs)

        try:
            resp = await s3.list_buckets()
            names = {b["Name"] for b in resp.get("Buckets", [])}
        except ClientError as e:
            code = (e.response or {}).get("Error", {}).get("Code", "")
            if code in ("InvalidAccessKeyId", "SignatureDoesNotMatch"):
                raise RuntimeError("S3 credentials rejected; check S3_* envs") from e
            raise

        if settings.S3_BUCKET_NAME not in names:
            try:
                await _create_bucket()
            except ClientError as e:
                code = (e.response or {}).get("Error", {}).get("Code", "")
                raise RuntimeError(f"Could not create bucket '{settings.S3_BUCKET_NAME}' (code={code})") from e

        logger.info("Startup OK: DB ready; S3 bucket '%s' ready", settings.S3_BUCKET_NAME)

    except Exception:
        with contextlib.suppress(Exception):
            await s3_cm.__aexit__(None, None, None)
        raise

async def _shutdown(app):
    # Close S3 client & DB pool cleanly
    try:
        cm = getattr(app.state, "s3_client_cm", None)
        if cm:
            await cm.__aexit__(None, None, None)
    except Exception:
        pass
    try:
        pool = getattr(app.state, "db_pool", None)
        if pool:
            await pool.close()
    except Exception:
        pass

def init_app(app):
    async def on_startup():
        await _startup(app)
    async def on_shutdown():
        await _shutdown(app)
    app.add_event_handler("startup", on_startup)
    app.add_event_handler("shutdown", on_shutdown)
    app.include_router(router)
