"""
Abstract database runner and concrete implementations for each connection protocol.

Each runner wraps the details of connecting to and querying a specific database type,
exposing a uniform async interface for the benchmark loop.
"""

import asyncio
import time
from abc import ABC, abstractmethod
from collections import deque

import asyncpg
import httpx


TIMEOUT_SEC = 10.0
MAX_SAMPLES = 200


def _percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    idx = (len(s) - 1) * p / 100
    lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
    frac = idx - lo
    return round(s[lo] + frac * (s[hi] - s[lo]), 2)


def update_stats(q: dict, elapsed_ms: float) -> None:
    q["_times"].append(elapsed_ms)
    times = list(q["_times"])
    q["last_ms"] = round(elapsed_ms, 2)
    q["p50_ms"] = _percentile(times, 50)
    q["p90_ms"] = _percentile(times, 90)
    q["p99_ms"] = _percentile(times, 99)


def fresh_query(idx: int, label: str) -> dict:
    return {
        "id": idx + 1,
        "label": label,
        "runs": 0,
        "timeout_count": 0,
        "error_count": 0,
        "_times": deque(maxlen=MAX_SAMPLES),
        "last_ms": None,
        "p50_ms": None,
        "p90_ms": None,
        "p99_ms": None,
        "status": "pending",
    }


class DBRunner(ABC):
    """Abstract interface for running benchmark queries against any database."""

    MAX_CONSECUTIVE_ERRORS = 5

    def __init__(self, db_id: str, config: dict):
        self.db_id = db_id
        self.config = config
        self._consecutive_errors = 0

    @abstractmethod
    async def connect(self) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...

    @abstractmethod
    async def execute_query(self, sql: str, timeout_sec: float) -> float:
        """Execute a single query, return elapsed_ms."""
        ...

    @abstractmethod
    async def fetch_scalar(self, sql: str) -> int:
        """Run a one-value query on a short-lived connection. Raises on failure.

        Callers that need to tell "the query failed" apart from "the answer is
        genuinely 0" — /validate's checksum comparison, for one — must use this
        rather than get_row_count(), which reports both as 0.
        """
        ...

    async def get_row_count(self) -> int:
        """Row count, or 0 if it can't be determined."""
        try:
            return await self.fetch_scalar(
                self.config.get("row_count_query", "SELECT count(*) FROM hits")
            )
        except Exception:
            return 0

    async def run_pass(
        self,
        queries: list[str],
        state_queries: list[dict],
        state_db: dict,
        is_running: callable,
    ) -> None:
        """Run all queries sequentially. Shared logic, calls execute_query per query."""
        for i, sql in enumerate(queries):
            if not is_running():
                return
            q = state_queries[i]
            q["status"] = "running"
            state_db["current_query"] = i + 1
            try:
                elapsed_ms = await self.execute_query(sql, TIMEOUT_SEC)
                if elapsed_ms > TIMEOUT_SEC * 1000:
                    q["status"] = "timeout"
                    q["timeout_count"] += 1
                else:
                    q["status"] = "ok"
                    update_stats(q, elapsed_ms)
                    self._consecutive_errors = 0
            except asyncio.TimeoutError:
                q["status"] = "timeout"
                q["timeout_count"] += 1
                self._consecutive_errors += 1
            except Exception:
                q["status"] = "error"
                q["error_count"] += 1
                self._consecutive_errors += 1
            finally:
                q["runs"] += 1
                state_db["total_runs"] += 1

            # Attempt reconnect after too many consecutive errors
            if self._consecutive_errors >= self.MAX_CONSECUTIVE_ERRORS:
                print(
                    f"[bench] {self.db_id}: attempting reconnect after "
                    f"{self._consecutive_errors} consecutive errors",
                    flush=True,
                )
                try:
                    await self.close()
                    await self.connect()
                    self._consecutive_errors = 0
                except Exception:
                    pass  # continue the loop even if reconnect fails

            await asyncio.sleep(0)
        state_db["current_query"] = None


# ── ClickHouse: HTTP protocol ────────────────────────────────────────────────


class HTTPRunner(DBRunner):
    """Runner for databases with HTTP SQL interface (ClickHouse, DuckDB, SQLite)."""

    def __init__(self, db_id: str, config: dict):
        super().__init__(db_id, config)
        conn = config["connection"]
        self.url = f"http://{conn['host']}:{conn['port']}/"
        self.user = conn.get("user", "")
        self.password = conn.get("password", "")
        self._client: httpx.AsyncClient | None = None

    def _params(self) -> dict:
        """Build query params — only include auth if credentials are set."""
        p = {}
        if self.user:
            p["user"] = self.user
        if self.password:
            p["password"] = self.password
        return p

    async def connect(self) -> None:
        self._client = httpx.AsyncClient(timeout=TIMEOUT_SEC + 2)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def execute_query(self, sql: str, timeout_sec: float) -> float:
        params = self._params()
        headers = {
            "Content-Type": "text/plain",
            # Honoured by the DuckDB/SQLite HTTP servers, which interrupt the
            # query and answer 504 rather than letting an abandoned request keep
            # burning CPU. Ignored by ClickHouse.
            "X-Query-Timeout": str(timeout_sec),
        }
        t0 = time.perf_counter()
        resp = await asyncio.wait_for(
            self._client.post(self.url, params=params, content=sql, headers=headers),
            timeout=timeout_sec + 1,
        )
        elapsed = (time.perf_counter() - t0) * 1000
        if resp.status_code == 504:
            # Server-side timeout — record it as a timeout, not an error.
            raise asyncio.TimeoutError()
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP error {resp.status_code}: {resp.text[:200]}")
        return elapsed

    async def fetch_scalar(self, sql: str) -> int:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(
                self.url,
                params=self._params(),
                content=sql,
                headers={"X-Query-Timeout": "10"},
            )
            if r.status_code != 200:
                raise RuntimeError(f"HTTP error {r.status_code}: {r.text[:200]}")
            return int(r.text.strip())


# ── PostgreSQL: asyncpg wire protocol ────────────────────────────────────────


class AsyncpgRunner(DBRunner):
    """Runner for PostgreSQL (and compatible: TimescaleDB, CrateDB, QuestDB) via asyncpg."""

    def __init__(self, db_id: str, config: dict):
        super().__init__(db_id, config)
        conn = config["connection"]
        self.dsn = (
            f"postgresql://{conn['user']}:{conn['password']}"
            f"@{conn['host']}:{conn['port']}/{conn['database']}"
        )
        self._conn: asyncpg.Connection | None = None

    async def connect(self) -> None:
        self._conn = await asyncpg.connect(self.dsn)
        await self._conn.execute(f"SET statement_timeout = '{int(TIMEOUT_SEC * 1000)}ms'")

    async def close(self) -> None:
        if self._conn:
            try:
                await self._conn.close()
            except Exception:
                pass
            self._conn = None

    async def _reconnect(self) -> None:
        await self.close()
        try:
            await self.connect()
        except Exception:
            await asyncio.sleep(3)

    async def execute_query(self, sql: str, timeout_sec: float) -> float:
        t0 = time.perf_counter()
        try:
            await asyncio.wait_for(self._conn.fetch(sql), timeout=timeout_sec + 1)
            return (time.perf_counter() - t0) * 1000
        except (asyncio.TimeoutError, asyncpg.QueryCanceledError):
            await self._reconnect()
            raise asyncio.TimeoutError()
        except Exception:
            await self._reconnect()
            raise

    async def fetch_scalar(self, sql: str) -> int:
        temp_conn = await asyncpg.connect(self.dsn)
        try:
            row = await temp_conn.fetchrow(sql)
            return int(row[0])
        finally:
            await temp_conn.close()


# ── MySQL/MariaDB: aiomysql wire protocol ────────────────────────────────────


class AioMySQLRunner(DBRunner):
    """Runner for MySQL/MariaDB (and compatible: Doris, StarRocks) via aiomysql."""

    def __init__(self, db_id: str, config: dict):
        super().__init__(db_id, config)
        conn = config["connection"]
        self._conn_params = {
            "host": conn["host"],
            "port": int(conn["port"]),
            "user": conn["user"],
            "password": conn["password"],
            "db": conn["database"],
        }
        self._conn = None

    async def connect(self) -> None:
        import aiomysql
        self._conn = await aiomysql.connect(**self._conn_params)
        await self._apply_statement_timeout()

    async def _apply_statement_timeout(self) -> None:
        """Set the per-statement timeout once per connection.

        This used to run inside execute_query's stopwatch, so every measurement
        carried an extra round-trip — two on MariaDB, where the MySQL spelling
        raises first. It's a session variable, so once at connect is enough.
          MySQL:   max_execution_time (ms)
          MariaDB: max_statement_time (seconds)
        """
        timeout_ms = int(TIMEOUT_SEC * 1000)
        async with self._conn.cursor() as cur:
            try:
                await cur.execute(f"SET SESSION max_execution_time = {timeout_ms}")
            except Exception:
                try:
                    await cur.execute(f"SET SESSION max_statement_time = {TIMEOUT_SEC:.3f}")
                except Exception:
                    pass  # neither dialect accepted it; client-side timeout still applies

    async def close(self) -> None:
        if self._conn:
            try:
                await self._conn.ensure_closed()
            except Exception:
                pass
            self._conn = None

    async def _reconnect(self) -> None:
        await self.close()
        try:
            await self.connect()
        except Exception:
            await asyncio.sleep(3)

    async def execute_query(self, sql: str, timeout_sec: float) -> float:
        if self._conn is None or self._conn.closed:
            await self._reconnect()
        try:
            async with self._conn.cursor() as cur:
                # Stopwatch starts here — cursor setup and the session timeout
                # are deliberately outside it, so this runner measures the same
                # thing the HTTP and asyncpg runners do.
                t0 = time.perf_counter()
                await cur.execute(sql)
                await cur.fetchall()
                return (time.perf_counter() - t0) * 1000
        except asyncio.TimeoutError:
            await self._reconnect()
            raise
        except Exception as e:
            # Check if it's a query timeout from MySQL (error 3024)
            err_str = str(e)
            if "3024" in err_str or "max_execution_time" in err_str.lower():
                await self._reconnect()
                raise asyncio.TimeoutError()
            await self._reconnect()
            raise

    async def fetch_scalar(self, sql: str) -> int:
        import aiomysql
        conn = await aiomysql.connect(**self._conn_params)
        try:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                row = await cur.fetchone()
            return int(row[0])
        finally:
            try:
                await conn.ensure_closed()
            except Exception:
                pass


# ── MonetDB: pymonetdb in thread executor ────────────────────────────────────


class MonetDBRunner(DBRunner):
    """Runner for MonetDB via pymonetdb."""

    def __init__(self, db_id: str, config: dict):
        super().__init__(db_id, config)
        self._conn = None

    def _new_conn(self):
        import pymonetdb
        conn = self.config["connection"]
        return pymonetdb.connect(
            hostname=conn["host"],
            port=int(conn["port"]),
            username=conn["user"],
            password=conn["password"],
            database=conn["database"],
        )

    async def connect(self) -> None:
        loop = asyncio.get_event_loop()
        self._conn = await loop.run_in_executor(None, self._new_conn)
        await loop.run_in_executor(None, self._set_query_timeout)

    def _set_query_timeout(self) -> None:
        """Ask the server to give up on long queries.

        Best-effort only. Measured against MonetDB 11, a 25s query with
        setquerytimeout(3) still ran ~29s before reporting "Query aborted due to
        timeout" — the limit is enforced lazily, so it does not promptly free
        the connection. Dropping and rebuilding the connection in
        execute_query's error path is what actually keeps an abandoned query
        from interleaving with the next one; this just stops the server from
        working on it forever.
        """
        try:
            cur = self._conn.cursor()
            cur.execute(f"call sys.setquerytimeout({int(TIMEOUT_SEC)})")
            cur.close()
            self._conn.commit()
        except Exception as e:
            print(f"[bench] {self.db_id}: could not set server query timeout: {e}", flush=True)

    async def close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    async def _reconnect(self) -> None:
        await self.close()
        try:
            await self.connect()
        except Exception:
            await asyncio.sleep(3)

    async def execute_query(self, sql: str, timeout_sec: float) -> float:
        if self._conn is None:
            await self._reconnect()
            if self._conn is None:
                raise RuntimeError(f"{self.db_id}: not connected")

        loop = asyncio.get_event_loop()
        # Bind the connection now rather than reading self._conn inside the
        # thread: after a timeout we swap in a new connection, and a thread that
        # had not yet reached .cursor() would otherwise grab the replacement and
        # corrupt the very stream the reconnect was meant to protect.
        conn = self._conn
        t0 = time.perf_counter()

        def _run():
            cur = conn.cursor()
            try:
                cur.execute(sql)
                cur.fetchall()
            finally:
                cur.close()

        try:
            await asyncio.wait_for(
                loop.run_in_executor(None, _run),
                timeout=timeout_sec + 1,
            )
            return (time.perf_counter() - t0) * 1000
        except Exception:
            # wait_for abandons the future but cannot stop the thread, which may
            # still be reading from this connection — and pymonetdb connections
            # are not thread-safe. Retire it and build a fresh one.
            await self._reconnect()
            raise

    async def fetch_scalar(self, sql: str) -> int:
        loop = asyncio.get_event_loop()

        def _run():
            conn = self._new_conn()
            try:
                cur = conn.cursor()
                cur.execute(sql)
                row = cur.fetchone()
                cur.close()
                return int(row[0])
            finally:
                conn.close()

        return await loop.run_in_executor(None, _run)


# ── Runner factory ───────────────────────────────────────────────────────────
#
# DuckDB and SQLite both speak `http` — they run behind the small HTTP SQL
# servers in databases/<id>/docker/, so HTTPRunner covers them.

PROTOCOL_RUNNERS = {
    "http": HTTPRunner,
    "asyncpg": AsyncpgRunner,
    "aiomysql": AioMySQLRunner,
    "monetdb": MonetDBRunner,
}


def create_runner(db_id: str, config: dict) -> DBRunner:
    """Create a runner instance based on the connection protocol in config."""
    protocol = config["connection"]["protocol"]
    runner_cls = PROTOCOL_RUNNERS.get(protocol)
    if runner_cls is None:
        raise ValueError(f"Unknown protocol '{protocol}' for database '{db_id}'")
    return runner_cls(db_id, config)
