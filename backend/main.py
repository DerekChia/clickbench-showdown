"""
ClickBench Showdown — FastAPI backend.

Supports N databases via a plugin system. The user selects 2 databases (db_a, db_b)
from the dashboard, and the benchmark runs queries alternately on each.
"""

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from db_registry import scan_databases, get_runner, list_databases
from db_runner import DBRunner, fresh_query, TIMEOUT_SEC
from docker_manager import ensure_running, stop_db, is_running
from query_fetcher import fetch_queries, QUERY_LABELS, QUERY_LABELS_SHORT

# ── Config ──────────────────────────────────────────────────────────────────

LOADER_URL = "http://showdown-loader:5000"
RESULTS_DIR = os.environ.get("RESULTS_DIR", "/tmp/clickbench_results")
WATCH_INTERVAL_SEC = 10
LOAD_MAX_WAIT_SEC = 3 * 3600  # a 10-file load into SQLite can take a while


def _default_files() -> int:
    """File count the dashboard should preselect — mirrors `showdown.sh --files`."""
    try:
        return max(1, min(10, int(os.environ.get("PARQUET_FILES", "1"))))
    except ValueError:
        return 1

# ── Registry ────────────────────────────────────────────────────────────────

_registry: dict[str, dict] = {}

# ── State ────────────────────────────────────────────────────────────────────


def _fresh_db_state(db_id: str, display_name: str, labels: list[str]) -> dict:
    n = len(labels)
    return {
        "db_id": db_id,
        "display_name": display_name,
        "total_runs": 0,
        "current_query": None,
        "queries": [fresh_query(i, labels[i] if i < len(labels) else f"Q{i+1}") for i in range(n)],
    }


state: dict = {
    "running": False,
    "warmup_in_progress": False,
    "current_db": None,
    "selected": {"db_a": None, "db_b": None},
    "loader": {
        "db_a_rows": 0,
        "db_b_rows": 0,
        "ready": False,
        "busy": False,
        "error": None,
        "message": "Select two databases to begin.",
        "default_files": _default_files(),
    },
    "db_a": None,
    "db_b": None,
}

_bench_tasks: list[asyncio.Task] = []
_setup_task: Optional[asyncio.Task] = None
_runner_a: Optional[DBRunner] = None
_runner_b: Optional[DBRunner] = None
_queries_a: list[str] = []
_queries_b: list[str] = []
_warmup_requested: bool = False
# True only once both databases have been fully loaded for the current
# selection. Row counts alone are not evidence of that — see _refresh_loader().
_load_complete: bool = False

# Last known row count per db_id. While a benchmark runs these are served from
# here instead of re-queried: a `SELECT count(*)` from the status poller is a
# full scan that competes with the query being timed, which would corrupt the
# very measurements this tool exists to produce.
_row_count_cache: dict[str, int] = {}


# ─��� Helpers ──────────────────────────────────────────────────────────────────

def _serialisable(obj) -> dict | list | str | int | float | bool | None:
    """Strip non-serialisable internals (_times deque, _* keys) for JSON response."""
    if isinstance(obj, dict):
        return {k: _serialisable(v) for k, v in obj.items() if not k.startswith("_")}
    if isinstance(obj, list):
        return [_serialisable(i) for i in obj]
    return obj


async def _stop_benchmark_internal() -> None:
    """Stop any running benchmark and wait for its task to finish unwinding.

    Waiting matters. The benchmark task closes both DB connections in a finally
    block, so a start that raced ahead of that cleanup would have its brand-new
    connections closed out from under it by the task it just replaced.
    """
    state["running"] = False
    state["current_db"] = None
    tasks = list(_bench_tasks)
    _bench_tasks.clear()
    for t in tasks:
        t.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    for slot in ("db_a", "db_b"):
        if state[slot]:
            state[slot]["current_query"] = None
            for q in state[slot]["queries"]:
                if q["status"] == "running":
                    q["status"] = "pending"


def _save_pass_results(pass_number: int) -> None:
    """Append one JSON line to the results history file."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, "history.jsonl")

    def _extract_query_results(db_state: dict) -> list[dict]:
        results = []
        for q in db_state["queries"]:
            results.append({
                "id": q["id"],
                "label": q["label"],
                "last_ms": q["last_ms"],
                "p50_ms": q["p50_ms"],
                "p90_ms": q["p90_ms"],
                "p99_ms": q["p99_ms"],
                "runs": q["runs"],
                "timeout_count": q["timeout_count"],
                "error_count": q["error_count"],
            })
        return results

    entry = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "db_a": state["selected"]["db_a"],
        "db_b": state["selected"]["db_b"],
        "pass": pass_number,
        "results": {
            "db_a": _extract_query_results(state["db_a"]),
            "db_b": _extract_query_results(state["db_b"]),
        },
    }
    with open(path, "a") as f:
        f.write(json.dumps(entry) + "\n")


# ── Benchmark loop ──────────────────────────────────────────────────────────

async def _run_benchmark() -> None:
    global _runner_a, _runner_b

    try:
        await _runner_a.connect()
    except Exception as e:
        print(f"[bench] Failed to connect to db_a: {e}", flush=True)
        state["running"] = False
        return

    try:
        await _runner_b.connect()
    except Exception as e:
        print(f"[bench] Failed to connect to db_b: {e}", flush=True)
        await _runner_a.close()
        state["running"] = False
        return

    try:
        # Warm-up pass (if requested)
        if _warmup_requested and state["running"]:
            state["warmup_in_progress"] = True
            print("[bench] Running warm-up pass...", flush=True)

            # Create throwaway state dicts for the warm-up
            from db_runner import fresh_query as _fresh_q
            warmup_queries_a = [_fresh_q(i, f"Q{i+1}") for i in range(len(_queries_a))]
            warmup_queries_b = [_fresh_q(i, f"Q{i+1}") for i in range(len(_queries_b))]
            warmup_db_a = {"total_runs": 0, "current_query": None}
            warmup_db_b = {"total_runs": 0, "current_query": None}

            await _runner_a.run_pass(
                _queries_a, warmup_queries_a, warmup_db_a,
                lambda: state["running"],
            )
            if state["running"]:
                await _runner_b.run_pass(
                    _queries_b, warmup_queries_b, warmup_db_b,
                    lambda: state["running"],
                )

            state["warmup_in_progress"] = False
            print("[bench] Warm-up pass complete", flush=True)

        pass_number = 0
        while state["running"]:
            # db_a pass
            state["current_db"] = state["selected"]["db_a"]
            await _runner_a.run_pass(
                _queries_a, state["db_a"]["queries"], state["db_a"],
                lambda: state["running"],
            )

            if not state["running"]:
                break

            # db_b pass
            state["current_db"] = state["selected"]["db_b"]
            await _runner_b.run_pass(
                _queries_b, state["db_b"]["queries"], state["db_b"],
                lambda: state["running"],
            )

            if not state["running"]:
                break

            # Both passes complete — save results
            pass_number += 1
            try:
                _save_pass_results(pass_number)
            except Exception as e:
                print(f"[bench] Failed to save pass results: {e}", flush=True)
    finally:
        state["current_db"] = None
        state["warmup_in_progress"] = False
        await _runner_a.close()
        await _runner_b.close()


# ── Loader status watcher ───────────────────────────────────────���────────────

async def _loader_status() -> dict:
    """Fetch the loader's own view of what it's doing. {} if unreachable."""
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(f"{LOADER_URL}/status")
            if r.status_code == 200:
                return r.json()
    except Exception:
        pass
    return {}


async def _row_count(db_id: str, cfg: dict, probe: bool = True) -> int:
    """Row count for a database, cached.

    With probe=False no query is issued and the last known value is returned —
    used while a benchmark is running so status polling can't contend with the
    queries being measured.
    """
    if not probe:
        return _row_count_cache.get(db_id, 0)
    try:
        if not await is_running(db_id, cfg):
            _row_count_cache.pop(db_id, None)
            return 0
        n = await get_runner(db_id, _registry).get_row_count()
        _row_count_cache[db_id] = n
        return n
    except Exception:
        return _row_count_cache.get(db_id, 0)


async def _refresh_loader() -> None:
    """Recompute loader status, row counts, and readiness for the selection."""
    db_a_id = state["selected"]["db_a"]
    db_b_id = state["selected"]["db_b"]

    lstat = await _loader_status()
    state["loader"]["busy"] = bool(lstat.get("reloading"))
    if lstat.get("error"):
        state["loader"]["error"] = lstat["error"]

    if not (db_a_id and db_b_id and db_a_id in _registry and db_b_id in _registry):
        state["loader"]["ready"] = False
        state["loader"]["db_a_rows"] = 0
        state["loader"]["db_b_rows"] = 0
        if not db_a_id or not db_b_id:
            state["loader"]["message"] = "Select two databases to begin."
        return

    cfg_a = _registry[db_a_id]
    cfg_b = _registry[db_b_id]
    # Never query the databases mid-benchmark; serve the cached counts.
    probe = not state["running"]
    a_rows = await _row_count(db_a_id, cfg_a, probe)
    b_rows = await _row_count(db_b_id, cfg_b, probe)
    state["loader"]["db_a_rows"] = a_rows
    state["loader"]["db_b_rows"] = b_rows

    name_a = cfg_a["display_name"]
    name_b = cfg_b["display_name"]
    error = state["loader"]["error"]

    # Readiness means the loader has *finished*, not that some rows have shown
    # up. Row counts climb throughout a load, and the PostgreSQL loader ends
    # with SET LOGGED + VACUUM ANALYZE — benchmarking inside that window times
    # queries against a database that is still doing heavy write work.
    if error:
        state["loader"]["ready"] = False
        state["loader"]["message"] = f"Load failed: {error}"
    elif state["loader"]["busy"]:
        state["loader"]["ready"] = False
        state["loader"]["message"] = lstat.get("message") or "Loading dataset..."
    elif not _load_complete:
        # Setup is still running; it owns the status message.
        state["loader"]["ready"] = False
    elif a_rows > 0 and b_rows > 0:
        state["loader"]["ready"] = True
        state["loader"]["message"] = (
            f"Ready - {name_a} {a_rows:,} rows | {name_b} {b_rows:,} rows"
        )
    else:
        state["loader"]["ready"] = False
        state["loader"]["message"] = (
            f"No data - {name_a} {a_rows:,} rows | {name_b} {b_rows:,} rows"
        )


async def _watch_loader() -> None:
    """Periodically refresh loader status and row counts."""
    while True:
        try:
            await _refresh_loader()
        except Exception as e:
            print(f"[watch] {e}", flush=True)
        await asyncio.sleep(WATCH_INTERVAL_SEC)


# ── App lifecycle ────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _registry
    _registry = scan_databases()
    print(f"[init] Found {len(_registry)} database(s): {list(_registry.keys())}", flush=True)
    task = asyncio.create_task(_watch_loader())
    yield
    task.cancel()
    if _setup_task is not None:
        _setup_task.cancel()
    for t in _bench_tasks:
        t.cancel()


app = FastAPI(lifespan=lifespan)

# This API can start and stop containers and has the Docker socket mounted, so
# it should not accept cross-origin requests from arbitrary websites the user
# happens to have open. Restrict to local and private-network origins — enough
# for the dashboard on :3000, whether reached via localhost or a LAN address.
ALLOWED_ORIGIN_REGEX = os.environ.get(
    "ALLOWED_ORIGIN_REGEX",
    r"^https?://("
    r"localhost|127\.0\.0\.1|\[::1\]|0\.0\.0\.0"
    r"|[A-Za-z0-9-]+\.local"
    r"|10\.\d{1,3}\.\d{1,3}\.\d{1,3}"
    r"|192\.168\.\d{1,3}\.\d{1,3}"
    r"|172\.(1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}"
    r")(:\d+)?$",
)
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=ALLOWED_ORIGIN_REGEX,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Suppress noisy access logs for high-frequency polling endpoints
import logging

class _PollFilter(logging.Filter):
    _quiet_paths = ("/status", "/databases")
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return not any(f'"GET {p} ' in msg for p in self._quiet_paths)

logging.getLogger("uvicorn.access").addFilter(_PollFilter())


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/databases")
async def get_databases():
    """List all available databases."""
    db_list = list_databases(_registry)
    # The dashboard polls this endpoint, so it must not query the databases
    # while a benchmark is running — cached counts only.
    probe = not state["running"]
    for db in db_list:
        db_id = db["id"]
        cfg = _registry.get(db_id, {})
        try:
            db["running"] = await is_running(db_id, cfg)
        except Exception:
            db["running"] = False
        db["selected"] = db_id in (state["selected"]["db_a"], state["selected"]["db_b"])
        db["rows"] = await _row_count(db_id, cfg, probe) if db["running"] else 0
    return db_list


@app.post("/select")
async def select_databases(db_a: str, db_b: str, files: int = 1):
    """Select two databases for comparison. Starts containers and loads data."""
    global _runner_a, _runner_b, _queries_a, _queries_b, _setup_task, _load_complete
    files = max(1, min(10, files))

    if db_a == db_b:
        return {"status": "error", "message": "Please select two different databases."}

    if db_a not in _registry:
        return {"status": "error", "message": f"Unknown database: {db_a}"}
    if db_b not in _registry:
        return {"status": "error", "message": f"Unknown database: {db_b}"}

    # Stop running benchmark
    await _stop_benchmark_internal()

    # Cancel any setup still in flight. Without this, double-clicking Apply
    # leaves two setup tasks racing over the same runner/query globals.
    if _setup_task is not None and not _setup_task.done():
        _setup_task.cancel()
        await asyncio.gather(_setup_task, return_exceptions=True)
    _setup_task = None
    _load_complete = False

    # Stop previously running containers that are no longer needed
    prev_a = state["selected"]["db_a"]
    prev_b = state["selected"]["db_b"]
    to_stop = set()
    if prev_a and prev_a not in (db_a, db_b):
        to_stop.add(prev_a)
    if prev_b and prev_b not in (db_a, db_b):
        to_stop.add(prev_b)

    for db_id in to_stop:
        try:
            await stop_db(db_id, _registry[db_id])
        except Exception as e:
            print(f"[select] Failed to stop {db_id}: {e}", flush=True)

    # Update selection
    state["selected"]["db_a"] = db_a
    state["selected"]["db_b"] = db_b

    cfg_a = _registry[db_a]
    cfg_b = _registry[db_b]

    state["loader"]["ready"] = False
    state["loader"]["error"] = None
    state["loader"]["message"] = (
        f"Starting {cfg_a['display_name']} and {cfg_b['display_name']}..."
    )

    # Start containers in background
    _setup_task = asyncio.create_task(_setup_databases(db_a, db_b, files))

    return {"status": "ok", "db_a": db_a, "db_b": db_b, "files": files}


async def _create_schema_if_needed(db_id: str, config: dict) -> None:
    """Execute schema.sql against a database if the file exists."""
    import importlib.util

    schema_path = config.get("_schema_path")
    if not schema_path or not os.path.exists(schema_path):
        return

    db_dir = config.get("_dir", "")
    loader_path = os.path.join(db_dir, "loader.py")
    if not os.path.exists(loader_path):
        return

    try:
        # Ensure parent of databases dir is on sys.path for cross-db imports
        import sys
        databases_dir = os.environ.get("DATABASES_DIR", "/app/databases")
        parent = os.path.dirname(databases_dir.rstrip("/"))
        if parent not in sys.path:
            sys.path.insert(0, parent)

        spec = importlib.util.spec_from_file_location(f"loader_{db_id}", loader_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        if hasattr(mod, "create_schema"):
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, mod.create_schema, config, schema_path)
            print(f"[setup] Schema created for {db_id}", flush=True)
    except Exception as e:
        print(f"[setup] Schema creation for {db_id} (may already exist): {e}", flush=True)


async def _wait_for_loader_idle() -> None:
    """Block until the loader finishes its current operation.

    Raises if the loader reports an error, so a failed load surfaces in the UI
    instead of leaving it stuck on "loading..." forever.
    """
    deadline = time.monotonic() + LOAD_MAX_WAIT_SEC
    while time.monotonic() < deadline:
        st = await _loader_status()
        if st.get("error"):
            raise RuntimeError(st["error"])
        if not st.get("reloading", False):
            return
        msg = st.get("message")
        if msg:
            state["loader"]["message"] = msg
        await asyncio.sleep(2)
    raise TimeoutError(f"Loader did not finish within {LOAD_MAX_WAIT_SEC}s")


async def _trigger_load(db_id: str, cfg: dict, files: int) -> None:
    """Load data into a database and wait for the load to actually complete.

    Always reloads (truncate + insert) with the requested file count.
    Waits for the loader to be free if it's busy with another database.
    """
    state["loader"]["message"] = f"Loading data into {cfg['display_name']}..."
    async with httpx.AsyncClient(timeout=10) as client:
        for attempt in range(120):  # Wait up to ~10 minutes for a free loader
            r = await client.post(f"{LOADER_URL}/load?db={db_id}&files={files}")
            data = r.json()
            status = data.get("status")
            if status == "started":
                print(f"[setup] Load triggered for {db_id} ({files} files)", flush=True)
                break
            if status == "busy":
                if attempt == 0:
                    print(f"[setup] Loader busy — waiting to load {db_id}...", flush=True)
                await asyncio.sleep(5)
                continue
            raise RuntimeError(f"Loader rejected load for {db_id}: {data}")
        else:
            raise TimeoutError(f"Loader stayed busy; could not load {db_id}")

    await _wait_for_loader_idle()
    print(f"[setup] Load complete for {db_id}", flush=True)


async def _setup_databases(db_a: str, db_b: str, files: int = 1) -> None:
    """Start containers, create schemas, load data, and set up runners."""
    global _runner_a, _runner_b, _queries_a, _queries_b, _load_complete

    cfg_a = _registry[db_a]
    cfg_b = _registry[db_b]

    try:
        # Start containers
        state["loader"]["message"] = (
            f"Starting {cfg_a['display_name']} container..."
        )
        await ensure_running(db_a, cfg_a)

        state["loader"]["message"] = (
            f"Starting {cfg_b['display_name']} container..."
        )
        await ensure_running(db_b, cfg_b)

        # Create schemas (needed before row count polling works)
        state["loader"]["message"] = "Creating schemas..."
        await _create_schema_if_needed(db_a, cfg_a)
        await _create_schema_if_needed(db_b, cfg_b)

        # Fetch queries from ClickBench repo
        state["loader"]["message"] = "Fetching queries from ClickBench..."
        repo_a = cfg_a.get("clickbench_repo_path", db_a)
        repo_b = cfg_b.get("clickbench_repo_path", db_b)
        _queries_a = await fetch_queries(repo_a)
        _queries_b = await fetch_queries(repo_b)

        n_a = len(_queries_a)
        n_b = len(_queries_b)

        # ClickBench queries are compared strictly positionally: row i shows
        # A's query i beside B's query i under one label. If the two files
        # disagree on length (upstream commenting out a query is enough), every
        # row after that point silently compares different SQL — and the chart
        # and CSV export inherit the misalignment. Refuse rather than mislead.
        if n_a != n_b:
            raise RuntimeError(
                f"Query count mismatch: {repo_a} has {n_a}, {repo_b} has {n_b}. "
                f"Refusing to run — positional comparison would pair different queries."
            )

        # Use min query count and shared labels
        labels = QUERY_LABELS[:max(n_a, n_b)]
        while len(labels) < max(n_a, n_b):
            labels.append(f"Q{len(labels) + 1}")

        # Initialize state for both databases
        state["db_a"] = _fresh_db_state(db_a, cfg_a["display_name"], labels[:n_a])
        state["db_b"] = _fresh_db_state(db_b, cfg_b["display_name"], labels[:n_b])

        # Create runners
        _runner_a = get_runner(db_a, _registry)
        _runner_b = get_runner(db_b, _registry)

        # Load one database at a time. The loader serialises on a single lock
        # anyway, and going in order lets us wait for each load to actually
        # finish — which is what makes _load_complete meaningful.
        await _trigger_load(db_a, cfg_a, files)
        await _trigger_load(db_b, cfg_b, files)

        _load_complete = True
        state["loader"]["message"] = "Data loaded — ready to benchmark."
        await _refresh_loader()

    except asyncio.CancelledError:
        raise
    except Exception as e:
        _load_complete = False
        state["loader"]["ready"] = False
        state["loader"]["error"] = str(e)
        state["loader"]["message"] = f"Setup failed: {e}"
        print(f"[setup] Error: {e}", flush=True)


@app.post("/benchmark/start")
async def start_benchmark(warmup: bool = False):
    global _warmup_requested

    if state["running"]:
        return {"status": "already_running"}
    if not state["loader"]["ready"]:
        return {"status": "not_ready", "message": state["loader"]["message"]}
    if not state["db_a"] or not state["db_b"]:
        return {"status": "not_ready", "message": "No databases selected."}

    # Pre-benchmark data validation
    db_a_id = state["selected"]["db_a"]
    db_b_id = state["selected"]["db_b"]
    try:
        runner_a_check = get_runner(db_a_id, _registry)
        runner_b_check = get_runner(db_b_id, _registry)
        rows_a = await runner_a_check.get_row_count()
        rows_b = await runner_b_check.get_row_count()
    except Exception as e:
        return {"status": "error", "message": f"Failed to validate row counts: {e}"}

    if rows_a == 0 or rows_b == 0:
        return {
            "status": "not_ready",
            "message": f"Data not loaded. {db_a_id}: {rows_a:,} rows, {db_b_id}: {rows_b:,} rows.",
        }

    if rows_a > 0 and rows_b > 0:
        diff_pct = abs(rows_a - rows_b) / max(rows_a, rows_b) * 100
        if diff_pct > 1:
            return {
                "status": "error",
                "message": (
                    f"Row count mismatch exceeds 1%: {db_a_id}={rows_a:,}, "
                    f"{db_b_id}={rows_b:,} (diff={diff_pct:.1f}%)"
                ),
            }

    # Reset stats
    for slot in ("db_a", "db_b"):
        db_state = state[slot]
        db_state["total_runs"] = 0
        db_state["current_query"] = None
        for q in db_state["queries"]:
            q["runs"] = 0
            q["timeout_count"] = 0
            q["error_count"] = 0
            q["_times"].clear()
            q["last_ms"] = None
            q["p50_ms"] = None
            q["p90_ms"] = None
            q["p99_ms"] = None
            q["status"] = "pending"

    _warmup_requested = warmup
    state["running"] = True
    state["warmup_in_progress"] = False
    state["current_db"] = None
    _bench_tasks.clear()
    _bench_tasks.append(asyncio.create_task(_run_benchmark()))
    return {"status": "started", "warmup": warmup}


@app.post("/benchmark/stop")
async def stop_benchmark():
    await _stop_benchmark_internal()
    return {"status": "stopped"}


@app.post("/loader/reload")
async def reload_loader(files: int = 1):
    """Re-download the dataset and reload it into the selected databases.

    This used to only re-download parquet files while the dashboard button was
    labelled "Load Data" and reported "Done" on completion. Because the row
    counts were still non-zero and the previous load was still marked complete,
    the UI went straight back to "Ready" — so asking for 10 files left you
    benchmarking the 1 file you already had.
    """
    global _setup_task, _load_complete
    files = max(1, min(10, files))

    await _stop_benchmark_internal()
    if _setup_task is not None and not _setup_task.done():
        _setup_task.cancel()
        await asyncio.gather(_setup_task, return_exceptions=True)
    _setup_task = None
    _load_complete = False

    state["loader"]["ready"] = False
    state["loader"]["error"] = None

    db_a = state["selected"]["db_a"]
    db_b = state["selected"]["db_b"]
    if db_a and db_b and db_a in _registry and db_b in _registry:
        state["loader"]["message"] = f"Reloading {files} file(s) into both databases..."
        _setup_task = asyncio.create_task(_setup_databases(db_a, db_b, files))
        return {"status": "started", "files": files, "db_a": db_a, "db_b": db_b}

    # Nothing selected yet — just make sure the files are on disk.
    state["loader"]["message"] = f"Downloading {files} parquet file(s)..."
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.post(f"{LOADER_URL}/reload?files={files}")
            return r.json()
    except Exception as exc:
        state["loader"]["error"] = str(exc)
        return {"status": "error", "message": str(exc)}


@app.get("/queries")
async def get_queries():
    """Return the current query SQL and labels for both selected databases."""
    return {
        "db_a": {
            "db_id": state["selected"]["db_a"],
            "queries": _queries_a,
        },
        "db_b": {
            "db_id": state["selected"]["db_b"],
            "queries": _queries_b,
        },
        "labels": QUERY_LABELS,
        "labels_short": QUERY_LABELS_SHORT,
    }


@app.get("/status")
async def get_status():
    result = _serialisable(state)
    result["warmup_in_progress"] = state.get("warmup_in_progress", False)
    return result


@app.get("/results/history")
async def get_results_history():
    """Return the last 100 entries from the benchmark history."""
    path = os.path.join(RESULTS_DIR, "history.jsonl")
    if not os.path.exists(path):
        return {"entries": []}
    entries = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return {"entries": entries[-100:]}


@app.get("/results/latest")
async def get_results_latest():
    """Return the most recent benchmark pass result."""
    path = os.path.join(RESULTS_DIR, "history.jsonl")
    if not os.path.exists(path):
        return {"entry": None}
    last_line = None
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                last_line = line
    if last_line:
        return {"entry": json.loads(last_line)}
    return {"entry": None}


@app.get("/validate")
async def validate_data():
    """Validate data integrity for both selected databases (row counts + checksums)."""
    db_a_id = state["selected"]["db_a"]
    db_b_id = state["selected"]["db_b"]

    if not db_a_id or not db_b_id:
        return {"valid": False, "message": "No databases selected."}

    result = {"valid": True, "db_a": {}, "db_b": {}}

    # Get row counts
    try:
        runner_a = get_runner(db_a_id, _registry)
        rows_a = await runner_a.get_row_count()
        result["db_a"]["rows"] = rows_a
    except Exception as e:
        result["db_a"]["rows"] = 0
        result["db_a"]["error"] = str(e)
        result["valid"] = False

    try:
        runner_b = get_runner(db_b_id, _registry)
        rows_b = await runner_b.get_row_count()
        result["db_b"]["rows"] = rows_b
    except Exception as e:
        result["db_b"]["rows"] = 0
        result["db_b"]["error"] = str(e)
        result["valid"] = False

    # Check row count match
    rows_a = result["db_a"].get("rows", 0)
    rows_b = result["db_b"].get("rows", 0)
    if rows_a == 0 or rows_b == 0:
        result["valid"] = False
        result["message"] = "One or both databases have no data."
    elif abs(rows_a - rows_b) / max(rows_a, rows_b) * 100 > 1:
        result["valid"] = False
        result["message"] = (
            f"Row count mismatch: {db_a_id}={rows_a:,}, {db_b_id}={rows_b:,}"
        )

    # Run checksum query: SUM(RegionID) FROM hits
    checksum_sql = "SELECT SUM(RegionID) FROM hits"

    async def _get_checksum(db_id: str) -> Optional[int]:
        """Checksum for one database, or None if the query didn't succeed.

        Uses fetch_scalar rather than temporarily rewriting the runner's
        row_count_query: that config dict used to be the registry's own object,
        so the override leaked to every other runner for that database and a
        concurrent status poll would read SUM(RegionID) as the row count. It
        also lets a failure stay distinguishable from a genuine 0 — get_row_count
        reports both as 0, which made two broken databases look like a match.
        """
        try:
            return await get_runner(db_id, _registry).fetch_scalar(checksum_sql)
        except Exception:
            return None

    checksum_a = await _get_checksum(db_a_id)
    checksum_b = await _get_checksum(db_b_id)

    result["db_a"]["checksum"] = checksum_a
    result["db_b"]["checksum"] = checksum_b

    if checksum_a is not None and checksum_b is not None:
        if checksum_a != checksum_b:
            result["valid"] = False
            result["checksum_match"] = False
            result["message"] = (
                f"Checksum mismatch (possible data corruption): "
                f"{db_a_id}={checksum_a}, {db_b_id}={checksum_b}"
            )
        else:
            result["checksum_match"] = True
    else:
        # A checksum that couldn't be computed is not a passing checksum.
        result["checksum_match"] = None
        result["valid"] = False
        failed = [
            db for db, c in ((db_a_id, checksum_a), (db_b_id, checksum_b)) if c is None
        ]
        result["message"] = f"Could not verify checksums for: {', '.join(failed)}"

    return result
