"""
Timeout and error handling tests for the benchmark runner.
Uses mocking — no Docker or running services required.
"""
from __future__ import annotations

import asyncio
import os
import sys
import types
from unittest.mock import AsyncMock, MagicMock

import pytest

# Add backend to path so we can import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

_backend_dir = os.path.join(os.path.dirname(__file__), "..", "backend")

def _import_with_future_annotations(module_name: str):
    """Import a backend module after compiling it with annotations future flag."""
    path = os.path.join(_backend_dir, module_name + ".py")
    with open(path) as f:
        source = "from __future__ import annotations\n" + f.read()
    code = compile(source, path, "exec")
    mod = types.ModuleType(module_name)
    mod.__file__ = path
    mod.__package__ = None
    sys.modules[module_name] = mod
    exec(code, mod.__dict__)
    return mod

_dr = _import_with_future_annotations("db_runner")
DBRunner = _dr.DBRunner
fresh_query = _dr.fresh_query
TIMEOUT_SEC = _dr.TIMEOUT_SEC


# ── Helper: concrete subclass with mocked execute_query ────────────────────


class MockRunner(DBRunner):
    """Concrete DBRunner subclass that delegates execute_query to a mock."""

    def __init__(self, execute_side_effects=None):
        # Minimal config — not used by run_pass
        super().__init__("mock_db", {"connection": {"protocol": "http"}})
        self._mock_execute = AsyncMock(side_effect=execute_side_effects)

    async def connect(self):
        pass

    async def close(self):
        pass

    async def execute_query(self, sql: str, timeout_sec: float) -> float:
        return await self._mock_execute(sql, timeout_sec)

    async def fetch_scalar(self, sql: str) -> int:
        # get_row_count() is now shared in the base class and delegates here.
        return 0


def _make_state_db():
    """Create a minimal state_db dict matching what run_pass expects."""
    return {"total_runs": 0, "current_query": None}


# ── Timeout handling ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_timeout_handling():
    runner = MockRunner(execute_side_effects=[asyncio.TimeoutError()])
    queries_sql = ["SELECT 1"]
    q = fresh_query(0, "Q1")
    state_queries = [q]
    state_db = _make_state_db()

    await runner.run_pass(queries_sql, state_queries, state_db, lambda: True)

    assert q["status"] == "timeout"
    assert q["timeout_count"] == 1
    assert q["runs"] == 1


# ── Error handling ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_error_handling():
    runner = MockRunner(execute_side_effects=[RuntimeError("connection lost")])
    queries_sql = ["SELECT 1"]
    q = fresh_query(0, "Q1")
    state_queries = [q]
    state_db = _make_state_db()

    await runner.run_pass(queries_sql, state_queries, state_db, lambda: True)

    assert q["status"] == "error"
    assert q["error_count"] == 1
    assert q["runs"] == 1


# ── Successful query ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_successful_query():
    runner = MockRunner(execute_side_effects=[42.5])
    queries_sql = ["SELECT 1"]
    q = fresh_query(0, "Q1")
    state_queries = [q]
    state_db = _make_state_db()

    await runner.run_pass(queries_sql, state_queries, state_db, lambda: True)

    assert q["status"] == "ok"
    assert q["last_ms"] == 42.5
    assert q["runs"] == 1


# ── Mixed results ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_mixed_results():
    runner = MockRunner(
        execute_side_effects=[
            100.0,                          # Q1 succeeds
            asyncio.TimeoutError(),         # Q2 times out
            RuntimeError("connection lost"),  # Q3 errors
        ]
    )
    queries_sql = ["SELECT 1", "SELECT 2", "SELECT 3"]
    q1 = fresh_query(0, "Q1")
    q2 = fresh_query(1, "Q2")
    q3 = fresh_query(2, "Q3")
    state_queries = [q1, q2, q3]
    state_db = _make_state_db()

    await runner.run_pass(queries_sql, state_queries, state_db, lambda: True)

    # Q1: success
    assert q1["status"] == "ok"
    assert q1["last_ms"] == 100.0
    assert q1["runs"] == 1

    # Q2: timeout
    assert q2["status"] == "timeout"
    assert q2["timeout_count"] == 1
    assert q2["runs"] == 1

    # Q3: error
    assert q3["status"] == "error"
    assert q3["error_count"] == 1
    assert q3["runs"] == 1

    # Total runs across all queries
    assert state_db["total_runs"] == 3
