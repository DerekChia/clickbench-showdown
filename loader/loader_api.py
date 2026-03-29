#!/usr/bin/env python3
"""
Loader HTTP API — keeps the loader container alive after the initial data load
and accepts on-demand reload requests from the backend.

Endpoints
---------
POST /reload?files=N   Truncate both databases and reload N parquet files (1-10).
GET  /status           Return {reloading: bool, message: str}.
"""

import json
import os
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

# ── Config (same env vars as load.sh) ────────────────────────────────────────
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "bench_pass")
CH_URL  = f"http://{CH_HOST}:{CH_PORT}/"

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_USER = os.getenv("POSTGRES_USER", "bench_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "bench_pass")
PG_DB   = os.getenv("POSTGRES_DB",   "hits")

BASE_URL    = "https://datasets.clickhouse.com/hits_compatible/athena_partitioned"
PARQUET_DIR = os.getenv("PARQUET_DIR", "/tmp/hits_parquet")

# ── State ─────────────────────────────────────────────────────────────────────
_lock   = threading.Lock()
_status = {"reloading": False, "message": "idle"}


def log(msg: str) -> None:
    print(f"[loader-api] {msg}", flush=True)


# ── Reload worker ─────────────────────────────────────────────────────────────

def _run(args: list[str], env: dict, capture: bool = False) -> None:
    kwargs: dict = {"check": True, "env": env}
    if capture:
        kwargs["capture_output"] = True
    subprocess.run(args, **kwargs)


def _reload(files: int) -> None:
    env = os.environ.copy()
    env["PARQUET_FILES"] = str(files)
    env["PARQUET_DIR"]   = PARQUET_DIR
    env["PGPASSWORD"]    = PG_PASS
    os.makedirs(PARQUET_DIR, exist_ok=True)

    try:
        # 1. Truncate both databases
        log("Truncating ClickHouse…")
        _run(["curl", "-sf",
              f"{CH_URL}?user={CH_USER}&password={CH_PASS}",
              "--data", "TRUNCATE TABLE hits"], env=env)

        log("Truncating PostgreSQL…")
        _run(["psql", "-h", PG_HOST, "-p", PG_PORT, "-U", PG_USER, "-d", PG_DB,
              "-c", "TRUNCATE TABLE hits"], env=env, capture=True)

        # 2. Download any missing parquet files in parallel
        log(f"Ensuring {files} parquet file(s) are cached…")
        procs = []
        for i in range(files):
            path = os.path.join(PARQUET_DIR, f"hits_{i}.parquet")
            if not os.path.exists(path):
                log(f"  Downloading hits_{i}.parquet…")
                procs.append(subprocess.Popen([
                    "curl", "-sf", "--retry", "5", "--retry-delay", "3",
                    "-o", path, f"{BASE_URL}/hits_{i}.parquet"
                ], env=env))
        for p in procs:
            p.wait()
            if p.returncode != 0:
                raise RuntimeError("One or more parquet downloads failed.")

        # 3. Load ClickHouse file by file
        log("Loading ClickHouse…")
        for i in range(files):
            path = os.path.join(PARQUET_DIR, f"hits_{i}.parquet")
            log(f"  Inserting hits_{i}.parquet into ClickHouse…")
            _run([
                "curl", "-sf",
                f"{CH_URL}?user={CH_USER}&password={CH_PASS}"
                f"&max_execution_time=3600"
                f"&input_format_parquet_case_insensitive_column_matching=1"
                f"&query=INSERT%20INTO%20hits%20FORMAT%20Parquet",
                "--data-binary", f"@{path}",
            ], env=env)

        # 4. Load PostgreSQL via load_pg.py
        log("Loading PostgreSQL…")
        _run(["python3", "/load_pg.py"], env=env)

        log("Reload complete.")
        _status["message"] = "idle"

    except Exception as exc:
        log(f"Reload failed: {exc}")
        _status["message"] = f"error: {exc}"
    finally:
        _status["reloading"] = False
        _lock.release()


# ── HTTP handler ──────────────────────────────────────────────────────────────

class _Handler(BaseHTTPRequestHandler):

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/reload":
            self._respond(404, {"error": "not found"})
            return

        params = parse_qs(parsed.query)
        try:
            files = max(1, min(10, int(params.get("files", ["5"])[0])))
        except (ValueError, IndexError):
            files = 5

        if not _lock.acquire(blocking=False):
            self._respond(409, {"status": "busy", "message": "reload already in progress"})
            return

        _status["reloading"] = True
        _status["message"]   = f"Reloading {files} file(s)…"
        threading.Thread(target=_reload, args=(files,), daemon=True).start()
        self._respond(202, {"status": "started", "files": files})

    def do_GET(self):
        if self.path == "/status":
            self._respond(200, _status)
        else:
            self._respond(404, {"error": "not found"})

    def _respond(self, code: int, data: dict) -> None:
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # suppress per-request access logs


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log("Loader API listening on :5000")
    HTTPServer(("0.0.0.0", 5000), _Handler).serve_forever()
