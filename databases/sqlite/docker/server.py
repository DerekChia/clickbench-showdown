"""Lightweight HTTP SQL server for SQLite.

Endpoints
---------
GET  /ping      Liveness probe (never touches the database).
POST /          Run one statement, return tab-separated rows.
POST /script    Run a multi-statement script as a single transaction.

Two properties matter for benchmark fairness:

  * The connection is opened once and reused, so SQLite's page cache stays warm
    between queries. Opening a fresh connection per request made every measured
    query pay cold-cache cost that server-based databases never pay.
  * Queries are interruptible. An `X-Query-Timeout` header (seconds) arms a
    watchdog that calls interrupt() and answers 504, so an abandoned client
    request doesn't leave a query burning CPU and blocking everything behind it.
"""

import os
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DB_PATH = os.environ.get("SQLITE_PATH", "/data/showdown.sqlite")
CACHE_KIB = int(os.environ.get("SQLITE_CACHE_KIB", "262144"))  # 256 MiB page cache

_conn = None
_conn_lock = threading.Lock()   # guards creation of the shared connection
_query_lock = threading.Lock()  # serialises use of the shared connection


def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(f"PRAGMA cache_size=-{CACHE_KIB}")
    return conn


def _shared_conn() -> sqlite3.Connection:
    global _conn
    with _conn_lock:
        if _conn is None:
            _conn = _new_conn()
        return _conn


class _Watchdog:
    """Interrupt a connection after `seconds`; no-op when seconds is falsy."""

    def __init__(self, conn: sqlite3.Connection, seconds: float):
        self.fired = False
        self._timer = None
        if seconds and seconds > 0:
            self._timer = threading.Timer(seconds, self._fire)
            self._conn = conn

    def _fire(self):
        self.fired = True
        try:
            self._conn.interrupt()
        except Exception:
            pass

    def __enter__(self):
        if self._timer:
            self._timer.start()
        return self

    def __exit__(self, *exc):
        if self._timer:
            self._timer.cancel()
        return False


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path == "/ping":
            self._respond(200, "ok")
            return
        self._respond(404, "not found")

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        sql = self.rfile.read(length).decode("utf-8").strip()
        if not sql:
            self._respond(400, "empty query")
            return

        try:
            timeout = float(self.headers.get("X-Query-Timeout", "0"))
        except ValueError:
            timeout = 0.0

        if self.path.rstrip("/") == "/script":
            self._run_script(sql)
        else:
            self._run_query(sql, timeout)

    def _run_script(self, sql: str) -> None:
        """Multi-statement script (schema creation, batched inserts)."""
        conn = _shared_conn()
        with _query_lock:
            try:
                conn.executescript("BEGIN;\n" + sql + "\nCOMMIT;")
                self._respond(200, "ok")
            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                self._respond(500, str(e))

    def _run_query(self, sql: str, timeout: float) -> None:
        # Reuse the warm shared connection when it's free. When a benchmark
        # query already holds it, fall back to a throwaway connection so health
        # checks and row-count polls aren't queued behind a slow query.
        acquired = _query_lock.acquire(blocking=False)
        conn = _shared_conn() if acquired else _new_conn()
        try:
            with _Watchdog(conn, timeout) as wd:
                try:
                    cur = conn.execute(sql)
                    rows = cur.fetchall()
                except Exception as e:
                    if wd.fired:
                        self._respond(504, f"query timed out after {timeout}s")
                    else:
                        self._respond(500, str(e))
                    return
            self._respond(200, "\n".join("\t".join(str(v) for v in row) for row in rows))
        finally:
            if acquired:
                _query_lock.release()
            else:
                conn.close()

    def _respond(self, code, body):
        data = body.encode("utf-8")
        self.send_response(code)
        # Declare the charset — without it clients default to Latin-1 per
        # RFC 2616 and mangle non-ASCII result text.
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    port = int(os.environ.get("PORT", "9998"))
    print(f"SQLite server listening on :{port}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
