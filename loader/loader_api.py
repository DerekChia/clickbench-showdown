#!/usr/bin/env python3
"""
Loader HTTP API — downloads parquet files and loads data into databases on demand.

Endpoints
---------
POST /reload?files=N           Download N parquet files (1-10).
POST /load?db=<id>&files=N     Load data into a specific database.
GET  /status                   Return {reloading: bool, message: str}.
"""

import importlib.util
import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

import yaml

# ── Config ──────────────────────────────────────────────────────────────────
BASE_URL = "https://datasets.clickhouse.com/hits_compatible/athena_partitioned"
PARQUET_DIR = os.getenv("PARQUET_DIR", "/tmp/hits_parquet")
DATABASES_DIR = os.getenv("DATABASES_DIR", "/app/databases")

# ── State ────────────────────────────────────────────────────────────────────
# `error` is reported separately from `message` so the backend can tell a
# finished-with-failure load apart from one that's still running.
_lock = threading.Lock()
_status = {"reloading": False, "message": "idle", "error": None, "db": None}


def log(msg: str) -> None:
    print(f"[loader-api] {msg}", flush=True)


# ── Database config loading ──────────────────────────────────────────────────

def _load_db_config(db_id: str) -> dict | None:
    """Load config.yaml for a database plugin."""
    config_path = os.path.join(DATABASES_DIR, db_id, "config.yaml")
    if not os.path.exists(config_path):
        return None
    with open(config_path) as f:
        config = yaml.safe_load(f)
    config["_dir"] = os.path.join(DATABASES_DIR, db_id)
    config["_schema_path"] = os.path.join(DATABASES_DIR, db_id, "schema.sql")
    return config


def _load_db_loader(db_id: str):
    """Dynamically import the loader module for a database."""
    loader_path = os.path.join(DATABASES_DIR, db_id, "loader.py")
    if not os.path.exists(loader_path):
        return None
    # Ensure parent of DATABASES_DIR is on sys.path so cross-db imports
    # like "from databases.mysql.loader import ..." work
    parent = os.path.dirname(DATABASES_DIR.rstrip("/"))
    if parent not in sys.path:
        sys.path.insert(0, parent)
    spec = importlib.util.spec_from_file_location(f"loader_{db_id}", loader_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ── Download parquet files ───────────────────────────────────────────────────

def _download_parquets(files: int) -> None:
    """Ensure the requested number of parquet files are downloaded.

    Downloads land in a .part file and are renamed only on success. Writing
    straight to the final path meant an interrupted download left a truncated
    file behind, which every later run then skipped as "already on disk".
    """
    os.makedirs(PARQUET_DIR, exist_ok=True)
    procs = []
    for i in range(files):
        path = os.path.join(PARQUET_DIR, f"hits_{i}.parquet")
        if not os.path.exists(path):
            log(f"  Downloading hits_{i}.parquet...")
            part = path + ".part"
            procs.append((i, path, part, subprocess.Popen([
                "curl", "-sf", "--retry", "5", "--retry-delay", "3",
                "-o", part, f"{BASE_URL}/hits_{i}.parquet"
            ])))

    failed = []
    for i, path, part, proc in procs:
        if proc.wait() == 0:
            os.replace(part, path)
        else:
            failed.append(f"hits_{i}.parquet")
            try:
                os.remove(part)
            except OSError:
                pass

    if failed:
        raise RuntimeError(f"Parquet download failed: {', '.join(failed)}")


# ── Load data into a specific database ───────────────────────────────────────

def _load_db(db_id: str, files: int) -> None:
    """Load parquet data into a specific database."""
    config = _load_db_config(db_id)
    if config is None:
        raise ValueError(f"Unknown database: {db_id}")

    loader_mod = _load_db_loader(db_id)
    if loader_mod is None:
        raise ValueError(f"No loader found for database: {db_id}")

    # Create schema if needed
    schema_path = config.get("_schema_path")
    if schema_path and os.path.exists(schema_path) and hasattr(loader_mod, "create_schema"):
        log(f"Creating schema for {db_id}...")
        try:
            loader_mod.create_schema(config, schema_path)
        except Exception as e:
            log(f"Schema creation for {db_id} (may already exist): {e}")

    # Load data
    log(f"Loading {files} file(s) into {db_id}...")
    total = loader_mod.load(PARQUET_DIR, files, config)
    log(f"Loaded {total:,} rows into {db_id}.")


# ── Reload worker ────────────────────────────────────────────────────────────

def _reload(files: int) -> None:
    try:
        _download_parquets(files)
        _status["message"] = "idle"
    except Exception as exc:
        log(f"Download failed: {exc}")
        _status["message"] = f"error: {exc}"
        _status["error"] = str(exc)
    finally:
        _status["reloading"] = False
        _lock.release()


def _load_single_db(db_id: str, files: int) -> None:
    try:
        _download_parquets(files)
        _load_db(db_id, files)
        _status["message"] = "idle"
    except Exception as exc:
        log(f"Load failed for {db_id}: {exc}")
        _status["message"] = f"error: {exc}"
        _status["error"] = f"{db_id}: {exc}"
    finally:
        _status["reloading"] = False
        _lock.release()


# ── HTTP handler ─────────────────────────────────────────────────────────────

class _Handler(BaseHTTPRequestHandler):

    def do_POST(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if parsed.path == "/reload":
            self._handle_reload(params)
        elif parsed.path == "/load":
            self._handle_load(params)
        else:
            self._respond(404, {"error": "not found"})

    def _handle_reload(self, params):
        """Download parquet files only."""
        try:
            files = max(1, min(10, int(params.get("files", ["1"])[0])))
        except (ValueError, IndexError):
            files = 1

        if not _lock.acquire(blocking=False):
            self._respond(409, {"status": "busy", "message": "operation already in progress"})
            return

        _status["reloading"] = True
        _status["error"] = None
        _status["db"] = None
        _status["message"] = f"Downloading {files} file(s)..."
        threading.Thread(target=_reload, args=(files,), daemon=True).start()
        self._respond(202, {"status": "started", "files": files})

    def _handle_load(self, params):
        """Load data into a specific database."""
        db_id = params.get("db", [None])[0]
        if not db_id:
            self._respond(400, {"error": "missing 'db' parameter"})
            return

        try:
            files = max(1, min(10, int(params.get("files", ["1"])[0])))
        except (ValueError, IndexError):
            files = 1

        if not _lock.acquire(blocking=False):
            self._respond(409, {"status": "busy", "message": "operation already in progress"})
            return

        _status["reloading"] = True
        _status["error"] = None
        _status["db"] = db_id
        _status["message"] = f"Loading {files} file(s) into {db_id}..."
        threading.Thread(target=_load_single_db, args=(db_id, files), daemon=True).start()
        self._respond(202, {"status": "started", "db": db_id, "files": files})

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
        pass


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log("Loader API listening on :5000")
    HTTPServer(("0.0.0.0", 5000), _Handler).serve_forever()
