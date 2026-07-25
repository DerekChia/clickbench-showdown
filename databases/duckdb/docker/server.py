"""Lightweight HTTP SQL server for DuckDB.

Endpoints
---------
GET  /ping   Liveness probe (never touches the database).
POST /       Run one statement, return tab-separated rows.

Two properties matter for benchmark fairness:

  * The database is opened once. Every request runs on a cursor of that shared
    connection, so DuckDB's buffer pool stays warm between queries — opening a
    fresh connection per request made every measured query pay cold-start cost
    that server-based databases never pay. Cursors are independent connections
    to the same instance, so concurrent requests don't block each other.
  * Queries are interruptible. An `X-Query-Timeout` header (seconds) arms a
    watchdog that calls interrupt() and answers 504, so an abandoned client
    request doesn't leave a query burning CPU.
"""

import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import duckdb

DB_PATH = os.environ.get("DUCKDB_PATH", "/data/showdown.duckdb")

_conn = None
_conn_lock = threading.Lock()


def _shared_conn():
    global _conn
    with _conn_lock:
        if _conn is None:
            _conn = duckdb.connect(DB_PATH)
        return _conn


class _Watchdog:
    """Interrupt a cursor after `seconds`; no-op when seconds is falsy."""

    def __init__(self, cur, seconds: float):
        self.fired = False
        self._timer = None
        if seconds and seconds > 0:
            self._timer = threading.Timer(seconds, self._fire)
            self._cur = cur

    def _fire(self):
        self.fired = True
        try:
            self._cur.interrupt()
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

        # A cursor is a separate connection sharing the same database instance:
        # warm buffer pool, no cross-request blocking.
        cur = _shared_conn().cursor()
        try:
            with _Watchdog(cur, timeout) as wd:
                try:
                    rows = cur.execute(sql).fetchall()
                except Exception as e:
                    if wd.fired:
                        self._respond(504, f"query timed out after {timeout}s")
                    else:
                        self._respond(500, str(e))
                    return
            self._respond(200, "\n".join("\t".join(str(v) for v in row) for row in rows))
        finally:
            try:
                cur.close()
            except Exception:
                pass

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
    port = int(os.environ.get("PORT", "9999"))
    print(f"DuckDB server listening on :{port}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
