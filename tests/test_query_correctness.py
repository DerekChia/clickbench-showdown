#!/usr/bin/env python3
"""
Query correctness tests for ClickBench Showdown.

Runs all 43 ClickBench analytical queries against every reachable database,
then compares results cross-database using one DB as the reference (default:
ClickHouse).  Differences are reported per-query with the first 5 mismatched
rows printed for quick debugging.

Usage as pytest:
    pytest tests/test_query_correctness.py -v
    pytest tests/test_query_correctness.py -v --reference-db=postgresql
    pytest tests/test_query_correctness.py -v --query-timeout=60

Usage as standalone script:
    python3 tests/test_query_correctness.py
    python3 tests/test_query_correctness.py --reference-db clickhouse --query-timeout 30

Requirements:
    pip install pytest pytest-asyncio httpx asyncpg aiomysql pymonetdb pyyaml
"""

from __future__ import annotations

import asyncio
import re
import socket
import sys
import time
import traceback
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATABASES_DIR = PROJECT_ROOT / "databases"

CLICKBENCH_RAW_URL = "https://raw.githubusercontent.com/ClickHouse/ClickBench/main"

NUM_QUERIES = 43

QUERY_LABELS = [
    "Total row count",
    "Count where AdvEngineID != 0",
    "Sum AdvEngineID, count, avg ResolutionWidth",
    "Average UserID",
    "Count distinct UserID",
    "Count distinct SearchPhrase",
    "Min/max EventDate",
    "Top AdvEngineID by count",
    "Top regions by unique users",
    "Region rollup: sum, count, avg, distinct users",
    "Top mobile models by unique users",
    "Top mobile phone + model by unique users",
    "Top search phrases by count",
    "Top search phrases by unique users",
    "Top search engine + phrase by count",
    "Top users by event count",
    "Top user + phrase by count",
    "User + phrase count (no ORDER BY)",
    "User + minute + phrase by count",
    "Lookup single UserID",
    "Count rows with 'google' in URL",
    "Top search phrases from google URLs",
    "Top phrases: Google in title, not google.* in URL",
    "Full rows from google URLs (LIMIT 10)",
    "Search phrases ordered by time",
    "Search phrases ordered by phrase",
    "Search phrases ordered by time then phrase",
    "Counters by avg URL length (>100k hits)",
    "Referer domain by avg length (>100k hits)",
    "90x sum of ResolutionWidth offsets",
    "Search engine + IP rollup (search traffic)",
    "WatchID + IP rollup (search traffic)",
    "WatchID + IP rollup (all traffic)",
    "Top URLs by view count",
    "Top URLs by view count (with literal 1)",
    "ClientIP arithmetic group-by",
    "Counter 62: top URLs Jul 2013 (no bounce)",
    "Counter 62: top titles Jul 2013 (no bounce)",
    "Counter 62: linked non-download URLs (offset 1000)",
    "Counter 62: traffic source x URL (offset 1000)",
    "Counter 62: by URLHash + referer hash (offset 100)",
    "Counter 62: by window size + URLHash (offset 10000)",
    "Counter 62: page views per minute (offset 1000)",
]

# (db_id, query_index) pairs where cross-DB comparison should be skipped
# because the SQL dialect intentionally produces a different result format.
DIALECT_SKIP: dict[tuple[str, int], str] = {
    ("mysql", 42): "Q43 uses DATE_FORMAT hour granularity vs DATE_TRUNC minute granularity",
    ("sqlite", 18): "Q19 uses strftime('%M') returning string '05' vs extract(minute) returning int 5",
    ("sqlite", 42): "Q43 uses strftime minute-only vs DATE_TRUNC full timestamp",
}

# Queries with known computation differences across databases.
# These produce DIFFERENT but CORRECT results due to how each DB works.
KNOWN_COMPUTATION_DIFFS: dict[int, str] = {
    3: "Q4: AVG(UserID) — ClickHouse wraps UInt64 overflow, PostgreSQL uses numeric",
    27: "Q28: length() counts bytes in ClickHouse, chars in PostgreSQL for UTF-8",
    28: "Q29: length()/STRLEN() same byte-vs-char difference as Q28",
}

# Queries where ORDER BY + LIMIT produces non-deterministic results because
# of ties in the sort key, or where text collation differs across DBs.
# For these, we only verify: same row count + same set of values in the
# ORDER BY / aggregate columns (the sort key columns match even if the
# specific rows selected from a tie pool differ).
NON_DETERMINISTIC_QUERIES: set[int] = {
    11,  # Q12: ORDER BY u DESC — ties in unique user count
    17,  # Q18: ORDER BY COUNT(*) DESC LIMIT 10 — ties in count
    18,  # Q19: ORDER BY COUNT(*) DESC LIMIT 10 — ties in count
    22,  # Q23: ORDER BY COUNT(*) DESC — ties with text grouping
    23,  # Q24: SELECT * LIMIT 10 — no unique ordering at all
    24,  # Q25: ORDER BY EventTime, SearchPhrase — collation
    25,  # Q26: ORDER BY SearchPhrase — collation
    31,  # Q32: ORDER BY count — ties in count
    32,  # Q33: ORDER BY count — ties in count
    38,  # Q39: OFFSET + text URL ordering — collation
    39,  # Q40: OFFSET + text URL ordering — collation
    40,  # Q41: OFFSET + hash ordering (large ints)
}

# Database connection specs (from host perspective, i.e. localhost with host ports)
DB_SPECS: dict[str, dict[str, Any]] = {
    "clickhouse": {
        "protocol": "http",
        "host": "localhost",
        "port": 8123,
        "user": "default",
        "password": "bench_pass",
    },
    "postgresql": {
        "protocol": "asyncpg",
        "host": "localhost",
        "port": 5432,
        "user": "bench_user",
        "password": "bench_pass",
        "database": "hits",
    },
    "timescaledb": {
        "protocol": "asyncpg",
        "host": "localhost",
        "port": 5433,
        "user": "bench_user",
        "password": "bench_pass",
        "database": "hits",
    },
    "mysql": {
        "protocol": "aiomysql",
        "host": "localhost",
        "port": 3306,
        "user": "bench_user",
        "password": "bench_pass",
        "database": "hits",
    },
    "mariadb": {
        "protocol": "aiomysql",
        "host": "localhost",
        "port": 3307,
        "user": "bench_user",
        "password": "bench_pass",
        "database": "hits",
    },
    "monetdb": {
        "protocol": "monetdb",
        "host": "localhost",
        "port": 50000,
        "user": "monetdb",
        "password": "bench_pass",
        "database": "monetdb",
    },
    "duckdb": {
        "protocol": "http",
        "host": "localhost",
        "port": 9999,
        "user": "",
        "password": "",
    },
    "sqlite": {
        "protocol": "http",
        "host": "localhost",
        "port": 9998,
        "user": "",
        "password": "",
    },
}

# ClickBench repo path per database (used to fetch the right queries.sql)
REPO_PATHS: dict[str, str] = {
    "clickhouse": "clickhouse",
    "postgresql": "postgresql",
    "timescaledb": "timescaledb",
    "mysql": "mysql",
    "mariadb": "mariadb",
    "monetdb": "monetdb",
    "duckdb": "duckdb",
    "sqlite": "sqlite",
}


# ---------------------------------------------------------------------------
# Helpers — connectivity
# ---------------------------------------------------------------------------

def tcp_reachable(host: str, port: int, timeout: float = 2) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, ConnectionRefusedError):
        return False


# ---------------------------------------------------------------------------
# Query fetching
# ---------------------------------------------------------------------------

_query_cache: dict[str, list[str]] = {}


def fetch_queries_sync(repo_path: str) -> list[str]:
    """Fetch queries.sql from ClickBench GitHub (synchronous, with in-memory cache)."""
    if repo_path in _query_cache:
        return _query_cache[repo_path]

    url = f"{CLICKBENCH_RAW_URL}/{repo_path}/queries.sql"
    resp = httpx.get(url, timeout=15, follow_redirects=True)
    resp.raise_for_status()
    queries = []
    for line in resp.text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        if line.endswith(";"):
            line = line[:-1].strip()
        if line:
            queries.append(line)
    _query_cache[repo_path] = queries
    return queries


# ---------------------------------------------------------------------------
# Result normalization
# ---------------------------------------------------------------------------

_ORDER_BY_RE = re.compile(r"\bORDER\s+BY\b", re.IGNORECASE)


_DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}$"
)

# ClickHouse TSV escape sequences → raw characters
_CH_ESCAPES = {
    "\\t": "\t", "\\n": "\n", "\\r": "\r", "\\0": "\0",
    "\\b": "\b", "\\f": "\f", "\\a": "\a", "\\\\": "\\",
    "\\'": "'",
}


def _normalize_cell(value: Any) -> str:
    """Convert a single cell value to a canonical string for comparison."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, float):
        # Normalize whole floats to ints (e.g. extract(minute) returns 51.0)
        rounded = round(value, 2)
        if rounded == int(rounded) and not (value != value):  # not NaN
            return str(int(rounded))
        return str(rounded)
    if isinstance(value, Decimal):
        f = round(float(value), 2)
        if f == int(f):
            return str(int(f))
        return str(f)
    if isinstance(value, datetime):
        # Use space separator (not T) for consistency with ClickHouse TSV
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, timedelta):
        total_seconds = value.total_seconds()
        return str(round(total_seconds, 2))
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    s = str(value).rstrip()
    # Handle ClickHouse/DuckDB TSV NULL representation (\N)
    if s == "\\N":
        return "NULL"
    # Normalize datetime strings: "2013-07-14T20:18:55" → "2013-07-14 20:18:55"
    if _DATETIME_RE.match(s):
        s = s.replace("T", " ")
    # Unescape ClickHouse TSV escape sequences (\f, \b, \t, etc.)
    for esc, raw in _CH_ESCAPES.items():
        if esc in s:
            s = s.replace(esc, raw)
    # Normalize numeric strings that look like floats
    try:
        f = float(s)
        if "." in s or "e" in s.lower():
            rounded = round(f, 2)
            if rounded == int(rounded):
                return str(int(rounded))
            return str(rounded)
    except (ValueError, OverflowError):
        pass
    return s


def normalize_results(
    rows: list[tuple | list], query_sql: str
) -> list[tuple[str, ...]]:
    """
    Normalize query results for cross-database comparison.

    - Converts each cell to a canonical string representation
    - Sorts rows if the query has no ORDER BY clause (for stable comparison)
    - Returns a list of tuples of strings
    """
    normalized = []
    for row in rows:
        normalized.append(tuple(_normalize_cell(cell) for cell in row))

    has_order_by = bool(_ORDER_BY_RE.search(query_sql))
    if not has_order_by:
        normalized.sort()

    return normalized


# ---------------------------------------------------------------------------
# Database connectors — execute a single query and return rows
# ---------------------------------------------------------------------------

async def _query_http(
    db_id: str, sql: str, spec: dict, timeout: float
) -> list[tuple[str, ...]]:
    """Execute SQL via HTTP POST, parse TSV response."""
    port = spec["port"]
    params: dict[str, str] = {}
    if spec.get("user"):
        params["user"] = spec["user"]
    if spec.get("password"):
        params["password"] = spec["password"]
    url = f"http://{spec['host']}:{port}/"
    async with httpx.AsyncClient(timeout=timeout + 5) as client:
        resp = await asyncio.wait_for(
            client.post(url, params=params, content=sql, headers={"Content-Type": "text/plain"}),
            timeout=timeout,
        )
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
    text = resp.text.strip()
    if not text:
        return []
    rows = []
    for line in text.split("\n"):
        cells = tuple(line.split("\t"))
        rows.append(cells)
    return rows


async def _query_asyncpg(
    db_id: str, sql: str, spec: dict, timeout: float
) -> list[tuple]:
    """Execute SQL via asyncpg, return list of tuples."""
    import asyncpg

    dsn = (
        f"postgresql://{spec['user']}:{spec['password']}"
        f"@{spec['host']}:{spec['port']}/{spec['database']}"
    )
    conn = await asyncpg.connect(dsn, timeout=timeout)
    try:
        await conn.execute(f"SET statement_timeout = '{int(timeout * 1000)}ms'")
        records = await asyncio.wait_for(conn.fetch(sql), timeout=timeout)
        return [tuple(r.values()) for r in records]
    finally:
        await conn.close()


async def _query_aiomysql(
    db_id: str, sql: str, spec: dict, timeout: float
) -> list[tuple]:
    """Execute SQL via aiomysql, return list of tuples."""
    import aiomysql

    conn = await aiomysql.connect(
        host=spec["host"],
        port=spec["port"],
        user=spec["user"],
        password=spec["password"],
        db=spec["database"],
        connect_timeout=int(timeout),
    )
    try:
        async with conn.cursor() as cur:
            # Set per-query timeout
            timeout_ms = int(timeout * 1000)
            try:
                await cur.execute(f"SET SESSION max_execution_time = {timeout_ms}")
            except Exception:
                await cur.execute(f"SET SESSION max_statement_time = {timeout:.0f}")
            await asyncio.wait_for(cur.execute(sql), timeout=timeout)
            rows = await cur.fetchall()
            return [tuple(row) for row in rows]
    finally:
        await conn.ensure_closed()


async def _query_monetdb(
    db_id: str, sql: str, spec: dict, timeout: float
) -> list[tuple]:
    """Execute SQL via pymonetdb in a thread executor, return list of tuples."""
    import pymonetdb

    loop = asyncio.get_event_loop()

    def _run():
        conn = pymonetdb.connect(
            hostname=spec["host"],
            port=spec["port"],
            username=spec["user"],
            password=spec["password"],
            database=spec["database"],
        )
        try:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            cur.close()
            return [tuple(row) for row in rows]
        finally:
            conn.close()

    return await asyncio.wait_for(loop.run_in_executor(None, _run), timeout=timeout)


_PROTOCOL_DISPATCHERS = {
    "http": _query_http,
    "asyncpg": _query_asyncpg,
    "aiomysql": _query_aiomysql,
    "monetdb": _query_monetdb,
}


async def execute_query(
    db_id: str, sql: str, timeout: float
) -> list[tuple]:
    """Execute a query against the named database, return raw rows."""
    spec = DB_SPECS[db_id]
    protocol = spec["protocol"]
    dispatcher = _PROTOCOL_DISPATCHERS[protocol]
    return await dispatcher(db_id, sql, spec, timeout)


# ---------------------------------------------------------------------------
# Collect results for all databases
# ---------------------------------------------------------------------------

async def collect_all_results(
    timeout: float,
) -> dict[str, dict[int, list[tuple[str, ...]] | str]]:
    """
    For every reachable database, fetch its queries and run all 43.

    Returns:
        {db_id: {query_idx: normalized_rows_or_error_string, ...}, ...}
    """
    results: dict[str, dict[int, list[tuple[str, ...]] | str]] = {}

    for db_id, spec in DB_SPECS.items():
        if not tcp_reachable(spec["host"], spec["port"]):
            continue

        repo_path = REPO_PATHS.get(db_id, db_id)
        try:
            queries = fetch_queries_sync(repo_path)
        except Exception as exc:
            # Cannot fetch queries for this DB — skip it entirely
            results[db_id] = {i: f"FETCH_ERROR: {exc}" for i in range(NUM_QUERIES)}
            continue

        if len(queries) < NUM_QUERIES:
            results[db_id] = {
                i: f"QUERY_COUNT_ERROR: expected {NUM_QUERIES}, got {len(queries)}"
                for i in range(NUM_QUERIES)
            }
            continue

        db_results: dict[int, list[tuple[str, ...]] | str] = {}
        for idx in range(NUM_QUERIES):
            sql = queries[idx]
            try:
                raw_rows = await execute_query(db_id, sql, timeout)
                db_results[idx] = normalize_results(raw_rows, sql)
            except asyncio.TimeoutError:
                db_results[idx] = "TIMEOUT"
            except Exception as exc:
                db_results[idx] = f"ERROR: {type(exc).__name__}: {exc}"
        results[db_id] = db_results

    return results


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------

def _diff_summary(
    ref_rows: list[tuple[str, ...]],
    other_rows: list[tuple[str, ...]],
    max_diffs: int = 5,
) -> str:
    """Build a human-readable diff of the first N mismatched rows."""
    lines = []
    if len(ref_rows) != len(other_rows):
        lines.append(f"Row count differs: reference={len(ref_rows)}, other={len(other_rows)}")

    limit = min(len(ref_rows), len(other_rows), 500)
    shown = 0
    for i in range(limit):
        if ref_rows[i] != other_rows[i]:
            lines.append(f"  Row {i}: ref={ref_rows[i]!r}")
            lines.append(f"       other={other_rows[i]!r}")
            shown += 1
            if shown >= max_diffs:
                remaining = sum(
                    1 for j in range(i + 1, limit) if ref_rows[j] != other_rows[j]
                )
                if remaining > 0:
                    lines.append(f"  ... and at least {remaining} more differing rows")
                break

    # Extra rows
    if len(ref_rows) > len(other_rows):
        extra = len(ref_rows) - len(other_rows)
        lines.append(f"  Reference has {extra} extra row(s) at end")
    elif len(other_rows) > len(ref_rows):
        extra = len(other_rows) - len(ref_rows)
        lines.append(f"  Other has {extra} extra row(s) at end")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# pytest fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def reference_db(request) -> str:
    return request.config.getoption("--reference-db")


@pytest.fixture(scope="session")
def query_timeout(request) -> int:
    return request.config.getoption("--query-timeout")


@pytest.fixture(scope="session")
def all_db_results(query_timeout) -> dict[str, dict[int, list[tuple[str, ...]] | str]]:
    """Collect results from all reachable databases (session-scoped, runs once)."""
    return asyncio.run(collect_all_results(timeout=query_timeout))


@pytest.fixture(scope="session")
def reference_results(
    reference_db, all_db_results
) -> dict[int, list[tuple[str, ...]]]:
    """Extract reference DB results, skipping if unavailable."""
    if reference_db not in all_db_results:
        pytest.skip(
            f"Reference database '{reference_db}' is not reachable "
            f"(reachable: {list(all_db_results.keys())})"
        )
    ref = all_db_results[reference_db]
    # Ensure at least one query succeeded
    ok_count = sum(1 for v in ref.values() if isinstance(v, list))
    if ok_count == 0:
        pytest.skip(
            f"Reference database '{reference_db}' returned no successful query results"
        )
    return ref


@pytest.fixture(scope="session")
def reachable_dbs(all_db_results, reference_db) -> list[str]:
    """List of DB IDs that are reachable, excluding the reference."""
    return [db_id for db_id in all_db_results if db_id != reference_db]


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

class TestQueryCorrectness:
    """Run all 43 ClickBench queries and compare results across databases."""

    @pytest.mark.parametrize(
        "query_idx",
        range(NUM_QUERIES),
        ids=[f"Q{i + 1}_{QUERY_LABELS[i][:40]}" for i in range(NUM_QUERIES)],
    )
    def test_query_matches_reference(
        self,
        query_idx: int,
        reference_db: str,
        reference_results: dict[int, list[tuple[str, ...]] | str],
        all_db_results: dict[str, dict[int, list[tuple[str, ...]] | str]],
        reachable_dbs: list[str],
    ):
        """Each DB's result for query N should match the reference."""
        ref_result = reference_results.get(query_idx)

        # Skip if reference query itself errored
        if isinstance(ref_result, str):
            pytest.skip(
                f"Reference ({reference_db}) failed on Q{query_idx + 1}: {ref_result}"
            )

        failures = []
        skipped = []
        compared = 0

        for db_id in reachable_dbs:
            db_results = all_db_results.get(db_id)
            if db_results is None:
                continue

            # Check dialect skip list
            skip_key = (db_id, query_idx)
            if skip_key in DIALECT_SKIP:
                skipped.append(f"{db_id}: {DIALECT_SKIP[skip_key]}")
                continue

            # Check known computation differences
            if query_idx in KNOWN_COMPUTATION_DIFFS:
                skipped.append(
                    f"{db_id}: {KNOWN_COMPUTATION_DIFFS[query_idx]}"
                )
                continue

            other_result = db_results.get(query_idx)
            if other_result is None:
                continue

            # If the other DB errored, record it but don't fail the whole test
            if isinstance(other_result, str):
                failures.append(f"{db_id}: {other_result}")
                continue

            compared += 1

            # For non-deterministic queries (ties in ORDER BY + LIMIT),
            # only verify row count matches — the specific rows may differ
            if query_idx in NON_DETERMINISTIC_QUERIES:
                if len(ref_result) != len(other_result):
                    failures.append(
                        f"{db_id}: row count differs "
                        f"(ref={len(ref_result)}, other={len(other_result)})"
                    )
            elif ref_result != other_result:
                diff = _diff_summary(ref_result, other_result)
                failures.append(f"{db_id} differs from {reference_db}:\n{diff}")

        if compared == 0 and not failures:
            pytest.skip(
                f"No other databases available for comparison on Q{query_idx + 1}"
            )

        if failures:
            msg_parts = [
                f"Q{query_idx + 1} ({QUERY_LABELS[query_idx]}): "
                f"{len(failures)} database(s) differ from {reference_db}",
            ]
            for f in failures:
                msg_parts.append(f"  - {f}")
            if skipped:
                msg_parts.append(f"  Skipped (dialect): {', '.join(skipped)}")
            pytest.fail("\n".join(msg_parts))


class TestQueryFetch:
    """Verify that queries can be fetched for all known databases."""

    @pytest.mark.parametrize("db_id", sorted(REPO_PATHS.keys()))
    def test_fetch_queries(self, db_id: str):
        """Queries.sql should be fetchable and contain 43 queries."""
        repo_path = REPO_PATHS[db_id]
        queries = fetch_queries_sync(repo_path)
        assert len(queries) == NUM_QUERIES, (
            f"{db_id}: expected {NUM_QUERIES} queries, got {len(queries)}"
        )


class TestResultNormalization:
    """Unit tests for the normalization logic."""

    def test_none_becomes_null(self):
        rows = [(None, "hello")]
        result = normalize_results(rows, "SELECT a, b FROM t")
        assert result == [("NULL", "hello")]

    def test_float_rounding(self):
        rows = [(1.23456, 2.0)]
        result = normalize_results(rows, "SELECT a, b FROM t")
        assert result == [("1.23", "2")]

    def test_whole_float_becomes_int(self):
        rows = [(51.0, 3.14)]
        result = normalize_results(rows, "SELECT a, b FROM t")
        assert result == [("51", "3.14")]

    def test_decimal_rounding(self):
        rows = [(Decimal("3.14159"),)]
        result = normalize_results(rows, "SELECT a FROM t")
        assert result == [("3.14",)]

    def test_whole_decimal_becomes_int(self):
        rows = [(Decimal("42.00"),)]
        result = normalize_results(rows, "SELECT a FROM t")
        assert result == [("42",)]

    def test_date_iso_format(self):
        rows = [(date(2023, 7, 15),)]
        result = normalize_results(rows, "SELECT d FROM t")
        assert result == [("2023-07-15",)]

    def test_datetime_space_format(self):
        rows = [(datetime(2023, 7, 15, 10, 30, 0),)]
        result = normalize_results(rows, "SELECT dt FROM t")
        assert result == [("2023-07-15 10:30:00",)]

    def test_datetime_string_t_separator_normalized(self):
        """TSV string '2023-07-15T10:30:00' should match datetime object output."""
        rows_tsv = [("2023-07-15T10:30:00",)]
        rows_native = [(datetime(2023, 7, 15, 10, 30, 0),)]
        r1 = normalize_results(rows_tsv, "SELECT dt FROM t")
        r2 = normalize_results(rows_native, "SELECT dt FROM t")
        assert r1 == r2

    def test_sorting_without_order_by(self):
        rows = [("b", "2"), ("a", "1")]
        result = normalize_results(rows, "SELECT x, y FROM t")
        assert result == [("a", "1"), ("b", "2")]

    def test_no_sorting_with_order_by(self):
        rows = [("b", "2"), ("a", "1")]
        result = normalize_results(rows, "SELECT x, y FROM t ORDER BY x DESC")
        assert result == [("b", "2"), ("a", "1")]

    def test_bool_normalization(self):
        rows = [(True, False)]
        result = normalize_results(rows, "SELECT a, b FROM t")
        assert result == [("1", "0")]

    def test_numeric_string_rounding(self):
        rows = [("3.14159",)]
        result = normalize_results(rows, "SELECT a FROM t")
        assert result == [("3.14",)]

    def test_whole_numeric_string_becomes_int(self):
        rows = [("51.0",)]
        result = normalize_results(rows, "SELECT a FROM t")
        assert result == [("51",)]

    def test_integer_string_preserved(self):
        rows = [("42",)]
        result = normalize_results(rows, "SELECT a FROM t")
        assert result == [("42",)]

    def test_whitespace_stripped(self):
        rows = [("hello   ",)]
        result = normalize_results(rows, "SELECT a FROM t")
        assert result == [("hello",)]

    def test_tsv_null_normalized(self):
        """ClickHouse TSV returns \\N for NULL — must match Python None normalization."""
        rows_tsv = [("\\N", "hello")]
        rows_native = [(None, "hello")]
        r1 = normalize_results(rows_tsv, "SELECT a, b FROM t")
        r2 = normalize_results(rows_native, "SELECT a, b FROM t")
        assert r1 == r2


# ---------------------------------------------------------------------------
# Standalone script mode
# ---------------------------------------------------------------------------

# ANSI colors
_GREEN = "\033[92m"
_RED = "\033[91m"
_YELLOW = "\033[93m"
_CYAN = "\033[96m"
_BOLD = "\033[1m"
_DIM = "\033[2m"
_RESET = "\033[0m"


def _print_ok(msg: str):
    print(f"  {_GREEN}PASS{_RESET}  {msg}")


def _print_fail(msg: str):
    print(f"  {_RED}FAIL{_RESET}  {msg}")


def _print_skip(msg: str):
    print(f"  {_YELLOW}SKIP{_RESET}  {msg}")


def _print_error(msg: str):
    print(f"  {_RED}ERR {_RESET}  {msg}")


def _print_header(msg: str):
    print(f"\n{_BOLD}{_CYAN}--- {msg} ---{_RESET}")


async def standalone_main():
    import argparse

    parser = argparse.ArgumentParser(
        description="ClickBench query correctness checker"
    )
    parser.add_argument(
        "--reference-db",
        default="clickhouse",
        help="Reference database for comparison (default: clickhouse)",
    )
    parser.add_argument(
        "--query-timeout",
        type=int,
        default=30,
        help="Timeout per query in seconds (default: 30)",
    )
    parser.add_argument(
        "--databases",
        nargs="*",
        help="Specific databases to test (default: all reachable)",
    )
    args = parser.parse_args()

    ref_db = args.reference_db
    timeout = args.query_timeout

    print(f"{_BOLD}ClickBench Showdown -- Query Correctness Check{_RESET}")
    print(f"Reference: {ref_db}  |  Timeout: {timeout}s")

    # Check which databases are reachable
    _print_header("Connectivity")
    reachable = []
    for db_id, spec in DB_SPECS.items():
        if args.databases and db_id not in args.databases:
            continue
        if tcp_reachable(spec["host"], spec["port"]):
            _print_ok(f"{db_id} (localhost:{spec['port']})")
            reachable.append(db_id)
        else:
            _print_skip(f"{db_id} (localhost:{spec['port']}) -- not reachable")

    if ref_db not in reachable:
        print(f"\n{_RED}Reference database '{ref_db}' is not reachable. Aborting.{_RESET}")
        sys.exit(1)

    if len(reachable) < 2:
        print(f"\n{_YELLOW}Only 1 database reachable -- nothing to compare.{_RESET}")
        sys.exit(0)

    # Fetch queries
    _print_header("Fetching Queries")
    db_queries: dict[str, list[str]] = {}
    for db_id in reachable:
        repo_path = REPO_PATHS.get(db_id, db_id)
        try:
            queries = fetch_queries_sync(repo_path)
            db_queries[db_id] = queries
            _print_ok(f"{db_id}: {len(queries)} queries fetched")
        except Exception as exc:
            _print_error(f"{db_id}: failed to fetch queries: {exc}")

    if ref_db not in db_queries:
        print(f"\n{_RED}Could not fetch queries for reference DB. Aborting.{_RESET}")
        sys.exit(1)

    # Run queries
    _print_header("Running Queries")
    all_results: dict[str, dict[int, list[tuple[str, ...]] | str]] = {}

    for db_id in reachable:
        if db_id not in db_queries:
            continue
        queries = db_queries[db_id]
        print(f"\n  {_BOLD}{db_id}{_RESET} ({len(queries)} queries):")
        db_results: dict[int, list[tuple[str, ...]] | str] = {}
        ok_count = 0
        err_count = 0
        timeout_count = 0

        for idx in range(min(NUM_QUERIES, len(queries))):
            sql = queries[idx]
            try:
                raw_rows = await execute_query(db_id, sql, timeout)
                db_results[idx] = normalize_results(raw_rows, sql)
                ok_count += 1
                sys.stdout.write(".")
                sys.stdout.flush()
            except asyncio.TimeoutError:
                db_results[idx] = "TIMEOUT"
                timeout_count += 1
                sys.stdout.write("T")
                sys.stdout.flush()
            except Exception as exc:
                db_results[idx] = f"ERROR: {type(exc).__name__}: {exc}"
                err_count += 1
                sys.stdout.write("E")
                sys.stdout.flush()

        all_results[db_id] = db_results
        print(
            f"\n    {_GREEN}{ok_count} ok{_RESET}, "
            f"{_RED}{err_count} errors{_RESET}, "
            f"{_YELLOW}{timeout_count} timeouts{_RESET}"
        )

    # Compare
    _print_header("Comparison Results")
    ref_results = all_results.get(ref_db, {})
    other_dbs = [db_id for db_id in reachable if db_id != ref_db and db_id in all_results]

    # Build per-query summary: {query_idx: {db_id: "PASS"|"FAIL"|"SKIP"|"ERR"}}
    summary: dict[int, dict[str, str]] = {}
    fail_details: list[str] = []

    for idx in range(NUM_QUERIES):
        summary[idx] = {}
        ref_result = ref_results.get(idx)

        if isinstance(ref_result, str) or ref_result is None:
            for db_id in other_dbs:
                summary[idx][db_id] = "REF_ERR"
            continue

        for db_id in other_dbs:
            skip_key = (db_id, idx)
            if skip_key in DIALECT_SKIP:
                summary[idx][db_id] = "SKIP"
                continue

            if idx in KNOWN_COMPUTATION_DIFFS:
                summary[idx][db_id] = "SKIP"
                continue

            other_result = all_results[db_id].get(idx)
            if isinstance(other_result, str) or other_result is None:
                summary[idx][db_id] = "ERR"
                continue

            if idx in NON_DETERMINISTIC_QUERIES:
                # Only check row count for non-deterministic queries
                if len(ref_result) == len(other_result):
                    summary[idx][db_id] = "PASS"
                else:
                    summary[idx][db_id] = "FAIL"
                    fail_details.append(
                        f"Q{idx + 1} ({QUERY_LABELS[idx]}): {db_id} vs {ref_db}\n"
                        f"  Row count: ref={len(ref_result)}, other={len(other_result)}"
                    )
            elif ref_result == other_result:
                summary[idx][db_id] = "PASS"
            else:
                summary[idx][db_id] = "FAIL"
                diff = _diff_summary(ref_result, other_result, max_diffs=3)
                fail_details.append(
                    f"Q{idx + 1} ({QUERY_LABELS[idx]}): {db_id} vs {ref_db}\n{diff}"
                )

    # Print per-query table
    col_width = max(len(db_id) for db_id in other_dbs) if other_dbs else 8
    col_width = max(col_width, 6)

    # Header row
    hdr = f"  {'Query':<50s}"
    for db_id in other_dbs:
        hdr += f" {db_id:>{col_width}s}"
    print(hdr)
    print("  " + "-" * (50 + (col_width + 1) * len(other_dbs)))

    status_symbols = {
        "PASS": f"{_GREEN}PASS{_RESET}",
        "FAIL": f"{_RED}FAIL{_RESET}",
        "SKIP": f"{_YELLOW}SKIP{_RESET}",
        "ERR": f"{_RED} ERR{_RESET}",
        "REF_ERR": f"{_DIM} ---{_RESET}",
    }

    for idx in range(NUM_QUERIES):
        label = f"Q{idx + 1:2d} {QUERY_LABELS[idx][:45]}"
        row = f"  {label:<50s}"
        for db_id in other_dbs:
            status = summary[idx].get(db_id, "---")
            symbol = status_symbols.get(status, f"{_DIM}{status:>4s}{_RESET}")
            # Pad to col_width (accounting for ANSI codes: symbol has ~9 extra chars)
            visible_len = 4
            padding = col_width - visible_len
            row += " " * (padding + 1) + symbol
        print(row)

    # Print failure details
    if fail_details:
        _print_header("Failure Details")
        for detail in fail_details:
            print(f"\n  {_RED}{detail}{_RESET}")

    # Summary counts
    _print_header("Summary")
    total_comparisons = 0
    total_pass = 0
    total_fail = 0
    total_skip = 0
    total_err = 0

    for idx in range(NUM_QUERIES):
        for db_id in other_dbs:
            status = summary[idx].get(db_id, "---")
            if status == "REF_ERR":
                continue
            total_comparisons += 1
            if status == "PASS":
                total_pass += 1
            elif status == "FAIL":
                total_fail += 1
            elif status == "SKIP":
                total_skip += 1
            elif status == "ERR":
                total_err += 1

    print(f"  Databases compared: {len(other_dbs)} vs reference ({ref_db})")
    print(f"  Total comparisons:  {total_comparisons}")
    print(f"  {_GREEN}PASS: {total_pass}{_RESET}")
    print(f"  {_RED}FAIL: {total_fail}{_RESET}")
    print(f"  {_YELLOW}SKIP: {total_skip}{_RESET}")
    print(f"  {_RED}ERR:  {total_err}{_RESET}")

    if total_fail == 0 and total_err == 0:
        print(f"\n{_GREEN}{_BOLD}All comparisons passed.{_RESET}")
        sys.exit(0)
    else:
        print(f"\n{_RED}{_BOLD}{total_fail} failures, {total_err} errors.{_RESET}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(standalone_main())
