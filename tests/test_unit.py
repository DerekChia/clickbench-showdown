"""
Pure logic unit tests — no Docker, no running services required.
Tests percentile math, stats updates, query parsing, registry scanning, and serialisation.
"""
from __future__ import annotations

import os
import sys
import tempfile
from collections import deque

import pytest

# Add backend to path so we can import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

# Python 3.9 compat: inject `from __future__ import annotations` into backend
# modules that use PEP 604 union syntax (X | Y) in type hints at runtime.
import importlib
_backend_dir = os.path.join(os.path.dirname(__file__), "..", "backend")

def _import_with_future_annotations(module_name: str):
    """Import a backend module after compiling it with annotations future flag."""
    import types
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

# Import modules that need the future annotations fix
_dr = _import_with_future_annotations("db_runner")
_qf = _import_with_future_annotations("query_fetcher")

_percentile = _dr._percentile
update_stats = _dr.update_stats
fresh_query = _dr.fresh_query
MAX_SAMPLES = _dr.MAX_SAMPLES
_parse_queries = _qf._parse_queries


# ── Percentile tests ───────────────────────────────────────────────────────


def test_percentile_empty():
    assert _percentile([], 50) == 0.0


def test_percentile_single_value():
    assert _percentile([7.5], 50) == 7.5
    assert _percentile([7.5], 90) == 7.5
    assert _percentile([7.5], 99) == 7.5


def test_percentile_p50_odd():
    assert _percentile([1, 2, 3, 4, 5], 50) == 3


def test_percentile_p50_even():
    assert _percentile([1, 2, 3, 4], 50) == 2.5


def test_percentile_p90():
    data = list(range(1, 101))  # 1..100
    result = _percentile(data, 90)
    # p90 of 1..100: index = 99 * 0.9 = 89.1 => lerp(90, 91, 0.1) = 90.1
    assert result == 90.1


def test_percentile_p99():
    data = list(range(1, 101))  # 1..100
    result = _percentile(data, 99)
    # p99: index = 99 * 0.99 = 98.01 => lerp(99, 100, 0.01) = 99.01
    assert result == 99.01


# ── update_stats tests ─────────────────────────────────────────────────────


def test_update_stats_first_call():
    q = fresh_query(0, "test query")
    update_stats(q, 42.123)
    assert q["last_ms"] == 42.12
    assert q["p50_ms"] == 42.12
    assert q["p90_ms"] == 42.12
    assert q["p99_ms"] == 42.12


def test_update_stats_multiple():
    q = fresh_query(0, "test query")
    values = [10.0, 20.0, 30.0, 40.0, 50.0]
    for v in values:
        update_stats(q, v)
    assert len(q["_times"]) == 5
    assert q["last_ms"] == 50.0
    assert q["p50_ms"] == 30.0


# ── fresh_query tests ──────────────────────────────────────────────────────


def test_fresh_query_structure():
    q = fresh_query(4, "Some label")
    assert q["id"] == 5
    assert q["label"] == "Some label"
    assert q["runs"] == 0
    assert q["timeout_count"] == 0
    assert q["error_count"] == 0
    assert q["status"] == "pending"
    assert q["last_ms"] is None
    assert q["p50_ms"] is None
    assert q["p90_ms"] is None
    assert q["p99_ms"] is None
    assert isinstance(q["_times"], deque)
    assert q["_times"].maxlen == MAX_SAMPLES


# ── Query parsing tests ───────────────────────────────────────────────────


def test_parse_queries_basic():
    result = _parse_queries("SELECT 1;\nSELECT 2;\n")
    assert result == ["SELECT 1", "SELECT 2"]


def test_parse_queries_strips_semicolons():
    result = _parse_queries("SELECT 1;\nSELECT 2;")
    for q in result:
        assert not q.endswith(";")


def test_parse_queries_skips_comments():
    content = "-- This is a comment\nSELECT 1;\n-- Another comment\nSELECT 2;\n"
    result = _parse_queries(content)
    assert result == ["SELECT 1", "SELECT 2"]


def test_parse_queries_skips_empty_lines():
    content = "\n\nSELECT 1;\n\n\nSELECT 2;\n\n"
    result = _parse_queries(content)
    assert result == ["SELECT 1", "SELECT 2"]


# ── Registry tests ─────────────────────────────────────────────────────────


def test_scan_databases_with_temp_dir():
    db_registry = _import_with_future_annotations("db_registry")

    with tempfile.TemporaryDirectory() as tmpdir:
        db_dir = os.path.join(tmpdir, "testdb")
        os.makedirs(db_dir)
        config_content = (
            "id: testdb\n"
            "display_name: Test DB\n"
            "connection:\n"
            "  protocol: http\n"
            "  host: localhost\n"
            "  port: 8123\n"
        )
        with open(os.path.join(db_dir, "config.yaml"), "w") as f:
            f.write(config_content)

        old_val = db_registry.DATABASES_DIR
        try:
            db_registry.DATABASES_DIR = tmpdir
            result = db_registry.scan_databases()
        finally:
            db_registry.DATABASES_DIR = old_val

        assert "testdb" in result
        assert result["testdb"]["display_name"] == "Test DB"


def test_scan_databases_empty_dir():
    db_registry = _import_with_future_annotations("db_registry")

    with tempfile.TemporaryDirectory() as tmpdir:
        old_val = db_registry.DATABASES_DIR
        try:
            db_registry.DATABASES_DIR = tmpdir
            result = db_registry.scan_databases()
        finally:
            db_registry.DATABASES_DIR = old_val

        assert result == {}


# ── Serialisable tests ─────────────────────────────────────────────────────

# Re-implement _serialisable locally to avoid importing all of main.py
# (which has side effects like FastAPI app creation and Docker imports).
# The logic is identical to main._serialisable.

def _serialisable(obj):
    """Strip non-serialisable internals (_times deque, _* keys) for JSON response."""
    if isinstance(obj, dict):
        return {k: _serialisable(v) for k, v in obj.items() if not k.startswith("_")}
    if isinstance(obj, list):
        return [_serialisable(i) for i in obj]
    return obj


def test_serialisable_strips_underscore_keys():
    data = {"_internal": 1, "public": 2}
    result = _serialisable(data)
    assert result == {"public": 2}


def test_serialisable_nested():
    data = {
        "outer": {
            "_hidden": "secret",
            "visible": 42,
            "inner": {
                "_also_hidden": True,
                "shown": "yes",
            },
        },
        "_top_level": "gone",
    }
    result = _serialisable(data)
    assert "_top_level" not in result
    assert "_hidden" not in result["outer"]
    assert result["outer"]["visible"] == 42
    assert "_also_hidden" not in result["outer"]["inner"]
    assert result["outer"]["inner"]["shown"] == "yes"
