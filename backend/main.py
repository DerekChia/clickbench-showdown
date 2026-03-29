import asyncio
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from queries import CLICKHOUSE_QUERIES, POSTGRES_QUERIES, QUERY_LABELS

# ── Config ──────────────────────────────────────────────────────────────────
CH_HOST = "clickhouse"
CH_PORT = 8123
CH_USER = "default"
CH_PASSWORD = "bench_pass"
CH_URL = f"http://{CH_HOST}:{CH_PORT}/"

PG_DSN = "postgresql://bench_user:bench_pass@postgres:5432/hits"

LOADER_URL = "http://loader:5000"

TIMEOUT_SEC = 10.0          # 10-second hard limit per query
MAX_SAMPLES = 200           # rolling window for percentile calc
NUM_QUERIES = 43

# ── State ────────────────────────────────────────────────────────────────────

def _fresh_query(idx: int) -> dict:
    return {
        "id": idx + 1,
        "label": QUERY_LABELS[idx],
        "runs": 0,
        "timeout_count": 0,
        "error_count": 0,
        "_times": deque(maxlen=MAX_SAMPLES),
        "last_ms": None,
        "p50_ms": None,
        "p90_ms": None,
        "p99_ms": None,
        "status": "pending",   # pending | running | ok | timeout | error
    }


state: dict = {
    "running": False,
    "current_db": None,   # "clickhouse" | "postgres" | None
    "loader": {
        "clickhouse_rows": 0,
        "postgres_rows": 0,
        "ready": False,
        "message": "Checking databases…",
    },
    "clickhouse": {
        "total_runs": 0,
        "current_query": None,
        "queries": [_fresh_query(i) for i in range(NUM_QUERIES)],
    },
    "postgres": {
        "total_runs": 0,
        "current_query": None,
        "queries": [_fresh_query(i) for i in range(NUM_QUERIES)],
    },
}

_bench_tasks: list[asyncio.Task] = []


# ── Helpers ──────────────────────────────────────────────────────────────────

def _percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    idx = (len(s) - 1) * p / 100
    lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
    frac = idx - lo
    return round(s[lo] + frac * (s[hi] - s[lo]), 2)


def _update_stats(q: dict, elapsed_ms: float) -> None:
    q["_times"].append(elapsed_ms)
    times = list(q["_times"])
    q["last_ms"] = round(elapsed_ms, 2)
    q["p50_ms"] = _percentile(times, 50)
    q["p90_ms"] = _percentile(times, 90)
    q["p99_ms"] = _percentile(times, 99)


def _serialisable(obj: dict) -> dict:
    """Strip non-serialisable internals (_times deque) for JSON response."""
    out = {}
    for k, v in obj.items():
        if k.startswith("_"):
            continue
        if isinstance(v, dict):
            out[k] = _serialisable(v)
        elif isinstance(v, list):
            out[k] = [_serialisable(i) if isinstance(i, dict) else i for i in v]
        else:
            out[k] = v
    return out


# ── ClickHouse: one full pass through all 43 queries ────────────────────────

async def _ch_pass(client: httpx.AsyncClient) -> None:
    params = {"user": CH_USER, "password": CH_PASSWORD, "default_format": "Null"}
    headers = {"Content-Type": "text/plain"}
    for i, sql in enumerate(CLICKHOUSE_QUERIES):
        if not state["running"]:
            return
        q = state["clickhouse"]["queries"][i]
        q["status"] = "running"
        state["clickhouse"]["current_query"] = i + 1
        t0 = time.perf_counter()
        try:
            resp = await asyncio.wait_for(
                client.post(CH_URL, params=params, content=sql, headers=headers),
                timeout=TIMEOUT_SEC + 1,
            )
            elapsed = (time.perf_counter() - t0) * 1000
            if elapsed > TIMEOUT_SEC * 1000:
                q["status"] = "timeout"
                q["timeout_count"] += 1
            elif resp.status_code != 200:
                q["status"] = "error"
                q["error_count"] += 1
            else:
                q["status"] = "ok"
                _update_stats(q, elapsed)
        except (asyncio.TimeoutError, httpx.TimeoutException):
            q["status"] = "timeout"
            q["timeout_count"] += 1
        except Exception:
            q["status"] = "error"
            q["error_count"] += 1
        finally:
            q["runs"] += 1
            state["clickhouse"]["total_runs"] += 1
        await asyncio.sleep(0)
    state["clickhouse"]["current_query"] = None


# ── PostgreSQL: one full pass through all 43 queries ────────────────────────

async def _pg_connect() -> asyncpg.Connection:
    conn = await asyncpg.connect(PG_DSN)
    await conn.execute(f"SET statement_timeout = '{int(TIMEOUT_SEC * 1000)}ms'")
    return conn


async def _pg_pass(conn: asyncpg.Connection) -> asyncpg.Connection:
    for i, sql in enumerate(POSTGRES_QUERIES):
        if not state["running"]:
            return conn
        q = state["postgres"]["queries"][i]
        q["status"] = "running"
        state["postgres"]["current_query"] = i + 1
        t0 = time.perf_counter()
        try:
            await asyncio.wait_for(conn.fetch(sql), timeout=TIMEOUT_SEC + 1)
            elapsed = (time.perf_counter() - t0) * 1000
            q["status"] = "ok"
            _update_stats(q, elapsed)
        except (asyncio.TimeoutError, asyncpg.QueryCanceledError):
            q["status"] = "timeout"
            q["timeout_count"] += 1
            try:
                await conn.close()
            except Exception:
                pass
            try:
                conn = await _pg_connect()
            except Exception:
                await asyncio.sleep(3)
        except Exception:
            q["status"] = "error"
            q["error_count"] += 1
            try:
                await conn.close()
            except Exception:
                pass
            try:
                conn = await _pg_connect()
            except Exception:
                await asyncio.sleep(3)
        finally:
            q["runs"] += 1
            state["postgres"]["total_runs"] += 1
        await asyncio.sleep(0)
    state["postgres"]["current_query"] = None
    return conn


# ── Main benchmark loop: CH pass → PG pass → repeat ─────────────────────────

async def _run_benchmark() -> None:
    pg_conn: Optional[asyncpg.Connection] = None
    try:
        pg_conn = await _pg_connect()
    except Exception:
        return

    async with httpx.AsyncClient(timeout=TIMEOUT_SEC + 2) as ch_client:
        while state["running"]:
            # ── ClickHouse turn ──────────────────────────────────────────────
            state["current_db"] = "clickhouse"
            await _ch_pass(ch_client)

            if not state["running"]:
                break

            # ── PostgreSQL turn ──────────────────────────────────────────────
            state["current_db"] = "postgres"
            pg_conn = await _pg_pass(pg_conn)

    state["current_db"] = None
    if pg_conn:
        try:
            await pg_conn.close()
        except Exception:
            pass


# ── Loader status watcher ────────────────────────────────────────────────────

async def _watch_loader() -> None:
    async with httpx.AsyncClient(timeout=5) as client:
        while True:
            # ClickHouse row count
            try:
                r = await client.post(
                    CH_URL,
                    params={"user": CH_USER, "password": CH_PASSWORD},
                    content="SELECT count() FROM hits",
                )
                if r.status_code == 200:
                    state["loader"]["clickhouse_rows"] = int(r.text.strip())
            except Exception:
                pass

            # PostgreSQL row count
            try:
                conn = await asyncpg.connect(PG_DSN)
                row = await conn.fetchrow("SELECT count(*) FROM hits")
                state["loader"]["postgres_rows"] = int(row[0])
                await conn.close()
            except Exception:
                pass

            ch_rows = state["loader"]["clickhouse_rows"]
            pg_rows = state["loader"]["postgres_rows"]

            if ch_rows > 0 and pg_rows > 0:
                state["loader"]["ready"] = True
                state["loader"]["message"] = (
                    f"Ready — ClickHouse {ch_rows:,} rows · PostgreSQL {pg_rows:,} rows"
                )
            elif ch_rows == 0 and pg_rows == 0:
                state["loader"]["ready"] = False
                state["loader"]["message"] = "Downloading and loading dataset… this may take a while"
            elif ch_rows > 0 and pg_rows == 0:
                state["loader"]["ready"] = False
                state["loader"]["message"] = (
                    f"ClickHouse ready ({ch_rows:,} rows) · PostgreSQL still loading…"
                )
            else:
                state["loader"]["ready"] = False
                state["loader"]["message"] = (
                    f"ClickHouse loading… · PostgreSQL {pg_rows:,} rows"
                )

            await asyncio.sleep(10)


# ── App lifecycle ────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_watch_loader())
    yield
    task.cancel()
    for t in _bench_tasks:
        t.cancel()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Routes ───────────────────────────────────────────────────────────────────

@app.post("/benchmark/start")
async def start_benchmark():
    if state["running"]:
        return {"status": "already_running"}
    if not state["loader"]["ready"]:
        return {"status": "not_ready", "message": state["loader"]["message"]}

    # Reset stats
    state["clickhouse"]["total_runs"] = 0
    state["clickhouse"]["current_query"] = None
    state["postgres"]["total_runs"] = 0
    state["postgres"]["current_query"] = None
    for db in ("clickhouse", "postgres"):
        for q in state[db]["queries"]:
            q["runs"] = 0
            q["timeout_count"] = 0
            q["error_count"] = 0
            q["_times"].clear()
            q["last_ms"] = None
            q["p50_ms"] = None
            q["p90_ms"] = None
            q["p99_ms"] = None
            q["status"] = "pending"

    state["running"] = True
    state["current_db"] = None
    _bench_tasks.clear()
    _bench_tasks.append(asyncio.create_task(_run_benchmark()))
    return {"status": "started"}


@app.post("/benchmark/stop")
async def stop_benchmark():
    state["running"] = False
    state["current_db"] = None
    for t in _bench_tasks:
        t.cancel()
    _bench_tasks.clear()
    for db in ("clickhouse", "postgres"):
        state[db]["current_query"] = None
        for q in state[db]["queries"]:
            if q["status"] == "running":
                q["status"] = "pending"
    return {"status": "stopped"}


@app.post("/loader/reload")
async def reload_loader(files: int = 5):
    files = max(1, min(10, files))

    # Stop any running benchmark first
    if state["running"]:
        state["running"] = False
        state["current_db"] = None
        for t in _bench_tasks:
            t.cancel()
        _bench_tasks.clear()
        for db in ("clickhouse", "postgres"):
            state[db]["current_query"] = None
            for q in state[db]["queries"]:
                if q["status"] == "running":
                    q["status"] = "pending"

    state["loader"]["ready"]   = False
    state["loader"]["message"] = f"Reloading {files} parquet file(s)…"

    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.post(f"{LOADER_URL}/reload?files={files}")
            return r.json()
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.get("/status")
async def get_status():
    return _serialisable(state)
