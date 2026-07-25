"""Loader data pipeline round-trip tests.

Verifies that data loaded into databases matches what's in the parquet files.
Catches encoding bugs (like bytes-to-string issues) at the data loading level.
"""

import importlib
import re
import sys
from pathlib import Path

import pytest

pyarrow = pytest.importorskip("pyarrow", reason="pyarrow required for loader tests")
pd = pytest.importorskip("pandas", reason="pandas required for loader tests")

# ---------------------------------------------------------------------------
# Setup: make database loader modules importable
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PARQUET_DIR = PROJECT_ROOT / "tmp"
PARQUET_FILE = PARQUET_DIR / "hits_0.parquet"

TEXT_COLUMNS = ["searchphrase", "url", "title", "referer",
                "mobilephone_model", "params", "pagecharset"]
TIMESTAMP_COLUMNS = ["eventtime", "clienteventtime", "localeventtime"]
DATE_COLUMNS = ["eventdate"]

TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
BYTES_REPR_RE = re.compile(r"^b'")


def _load_pg_transform():
    """Import PostgreSQL loader's _transform_chunk function."""
    pg_loader_path = PROJECT_ROOT / "databases" / "postgresql"
    if str(pg_loader_path) not in sys.path:
        sys.path.insert(0, str(pg_loader_path))
    spec = importlib.util.spec_from_file_location(
        "pg_loader", pg_loader_path / "loader.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod._transform_chunk


def _load_mysql_parquet_to_tsv_transform():
    """Import MySQL loader's transform logic (reuse the inner chunk logic).

    The MySQL loader imports pymysql at module level, so we mock it if missing.
    """
    mysql_loader_path = PROJECT_ROOT / "databases" / "mysql"
    if str(mysql_loader_path) not in sys.path:
        sys.path.insert(0, str(mysql_loader_path))

    # pymysql may not be installed locally -- stub it so the module loads
    if "pymysql" not in sys.modules:
        try:
            import pymysql  # noqa: F401
        except ImportError:
            from unittest import mock
            sys.modules["pymysql"] = mock.MagicMock()

    spec = importlib.util.spec_from_file_location(
        "mysql_loader", mysql_loader_path / "loader.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _read_parquet_sample(n_rows=100):
    """Read the first n_rows from the parquet file as a pyarrow batch."""
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(str(PARQUET_FILE))
    for batch in pf.iter_batches(batch_size=n_rows):
        return batch
    return None


def _pg_transform_sample(n_rows=100):
    """Read parquet and apply PostgreSQL transform_chunk."""
    batch = _read_parquet_sample(n_rows)
    df = batch.to_pandas()
    transform = _load_pg_transform()
    return transform(df)


def _mysql_transform_sample(n_rows=100):
    """Read parquet and apply the same transform MySQL uses internally."""
    import csv
    import pandas as pd

    mysql_mod = _load_mysql_parquet_to_tsv_transform()

    batch = _read_parquet_sample(n_rows)
    df = batch.to_pandas()
    df.columns = [c.lower() for c in df.columns]

    for col in mysql_mod.TIMESTAMP_COLS:
        if col in df.columns:
            df[col] = (
                pd.to_datetime(df[col], unit="s", utc=True)
                .dt.tz_localize(None)
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )

    for col in mysql_mod.DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit="D").dt.strftime("%Y-%m-%d")

    for col in mysql_mod.CHAR1_COLS:
        if col in df.columns:
            def _to_char(x):
                if pd.isna(x):
                    return ""
                if isinstance(x, (bytes, bytearray)):
                    return x.decode("latin-1")
                return chr(int(x))
            df[col] = df[col].apply(_to_char)

    for col in df.columns:
        if col in mysql_mod.CHAR1_COLS:
            continue
        if df[col].dtype == object and len(df) > 0:
            sample = df[col].iloc[0]
            if isinstance(sample, (bytes, bytearray)):
                df[col] = df[col].apply(
                    lambda x: x.decode("utf-8", errors="replace")
                    if isinstance(x, (bytes, bytearray)) else x
                )

    return df


# ===========================================================================
# Test Class 1: Parquet-to-DataFrame conversion
# ===========================================================================


class TestParquetConversion:
    """Test that parquet -> dataframe conversion produces correct types."""

    @pytest.fixture(autouse=True)
    def _require_parquet(self):
        if not PARQUET_FILE.exists():
            pytest.skip(
                f"Parquet file not found at {PARQUET_FILE}; "
                "run the loader to download data first"
            )

    def test_no_bytes_objects_in_any_column(self):
        """No column should contain raw Python bytes after transformation."""
        df = _pg_transform_sample()
        for col in df.columns:
            if df[col].dtype == object:
                bytes_mask = df[col].apply(
                    lambda x: isinstance(x, (bytes, bytearray))
                )
                assert not bytes_mask.any(), (
                    f"Column '{col}' contains bytes objects after transform"
                )

    def test_text_columns_are_strings(self):
        """Key text columns must be proper Python str instances."""
        df = _pg_transform_sample()
        for col in TEXT_COLUMNS:
            col_lower = col.lower()
            if col_lower not in df.columns:
                continue
            non_null = df[col_lower].dropna()
            if len(non_null) == 0:
                continue
            bad = non_null.apply(lambda x: not isinstance(x, str))
            assert not bad.any(), (
                f"Column '{col_lower}' has non-string values: "
                f"{non_null[bad].head(3).tolist()}"
            )

    def test_timestamp_format(self):
        """Timestamp columns must match YYYY-MM-DD HH:MM:SS format."""
        df = _pg_transform_sample()
        for col in TIMESTAMP_COLUMNS:
            if col not in df.columns:
                continue
            non_null = df[col].dropna()
            if len(non_null) == 0:
                continue
            bad = non_null.apply(lambda x: not TIMESTAMP_RE.match(str(x)))
            assert not bad.any(), (
                f"Column '{col}' has bad timestamp format: "
                f"{non_null[bad].head(3).tolist()}"
            )

    def test_date_format(self):
        """Date columns must match YYYY-MM-DD format."""
        df = _pg_transform_sample()
        for col in DATE_COLUMNS:
            if col not in df.columns:
                continue
            non_null = df[col].dropna()
            if len(non_null) == 0:
                continue
            bad = non_null.apply(lambda x: not DATE_RE.match(str(x)))
            assert not bad.any(), (
                f"Column '{col}' has bad date format: "
                f"{non_null[bad].head(3).tolist()}"
            )

    def test_no_bytes_repr_strings(self):
        """No column value should start with b' (the str(bytes) repr pattern)."""
        df = _pg_transform_sample()
        for col in df.columns:
            if df[col].dtype != object:
                continue
            non_null = df[col].dropna()
            if len(non_null) == 0:
                continue
            bad = non_null.apply(
                lambda x: bool(BYTES_REPR_RE.match(str(x)))
            )
            assert not bad.any(), (
                f"Column '{col}' has str(bytes) repr values: "
                f"{non_null[bad].head(3).tolist()}"
            )


# ===========================================================================
# Test Class 2: Cross-loader consistency
# ===========================================================================


class TestCrossLoaderConsistency:
    """Test that all loaders produce the same text values."""

    @pytest.fixture(autouse=True)
    def _require_parquet(self):
        if not PARQUET_FILE.exists():
            pytest.skip(
                f"Parquet file not found at {PARQUET_FILE}; "
                "run the loader to download data first"
            )

    def test_pg_and_mysql_text_columns_match(self):
        """PostgreSQL and MySQL loaders must produce identical text values."""
        pg_df = _pg_transform_sample(50)
        mysql_df = _mysql_transform_sample(50)

        for col in TEXT_COLUMNS:
            col_lower = col.lower()
            if col_lower not in pg_df.columns or col_lower not in mysql_df.columns:
                continue

            pg_vals = pg_df[col_lower].fillna("").astype(str).tolist()
            mysql_vals = mysql_df[col_lower].fillna("").astype(str).tolist()

            assert pg_vals == mysql_vals, (
                f"Text column '{col_lower}' differs between PG and MySQL loaders. "
                f"First mismatch at index "
                f"{next(i for i, (a, b) in enumerate(zip(pg_vals, mysql_vals)) if a != b)}"
            )

    def test_pg_and_mysql_timestamps_match(self):
        """PostgreSQL and MySQL loaders must produce identical timestamp values."""
        pg_df = _pg_transform_sample(50)
        mysql_df = _mysql_transform_sample(50)

        for col in TIMESTAMP_COLUMNS:
            if col not in pg_df.columns or col not in mysql_df.columns:
                continue

            pg_vals = pg_df[col].fillna("").astype(str).tolist()
            mysql_vals = mysql_df[col].fillna("").astype(str).tolist()

            assert pg_vals == mysql_vals, (
                f"Timestamp column '{col}' differs between PG and MySQL loaders"
            )

    def test_pg_and_mysql_dates_match(self):
        """PostgreSQL and MySQL loaders must produce identical date values."""
        pg_df = _pg_transform_sample(50)
        mysql_df = _mysql_transform_sample(50)

        for col in DATE_COLUMNS:
            if col not in pg_df.columns or col not in mysql_df.columns:
                continue

            pg_vals = pg_df[col].fillna("").astype(str).tolist()
            mysql_vals = mysql_df[col].fillna("").astype(str).tolist()

            assert pg_vals == mysql_vals, (
                f"Date column '{col}' differs between PG and MySQL loaders"
            )


# ===========================================================================
# Test Class 3: Database round-trip (requires running DBs)
# ===========================================================================


class TestDatabaseRoundTrip:
    """Test that queried data matches parquet source (requires running DBs)."""

    QUERY = (
        "SELECT SearchPhrase, URL, Title "
        "FROM hits "
        "WHERE SearchPhrase != '' "
        "ORDER BY EventTime "
        "LIMIT 10"
    )

    @staticmethod
    def _clickhouse_rows():
        """Fetch rows from ClickHouse via HTTP."""
        import requests

        url = "http://localhost:8123/"
        r = requests.get(
            url,
            params={
                "user": "default",
                "password": "",
                "query": TestDatabaseRoundTrip.QUERY + " FORMAT TabSeparated",
            },
            timeout=5,
        )
        r.raise_for_status()
        rows = []
        for line in r.text.strip().split("\n"):
            if line:
                rows.append(line.split("\t"))
        return rows

    @staticmethod
    def _postgresql_rows():
        """Fetch rows from PostgreSQL via psycopg2."""
        import psycopg2

        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="default",
            user="default",
            password="default",
        )
        with conn.cursor() as cur:
            cur.execute(TestDatabaseRoundTrip.QUERY)
            rows = [[str(v) for v in row] for row in cur.fetchall()]
        conn.close()
        return rows

    def test_clickhouse_no_bytes_repr(self):
        """ClickHouse query results should not contain str(bytes) repr."""
        try:
            rows = self._clickhouse_rows()
        except Exception as exc:
            pytest.skip(f"ClickHouse not reachable at localhost:8123: {exc}")

        if not rows:
            pytest.skip("No rows with non-empty SearchPhrase in ClickHouse")

        for i, row in enumerate(rows):
            for j, val in enumerate(row):
                assert not BYTES_REPR_RE.match(val), (
                    f"Row {i} col {j} has bytes repr: {val!r}"
                )

    def test_postgresql_no_bytes_repr(self):
        """PostgreSQL query results should not contain str(bytes) repr."""
        try:
            rows = self._postgresql_rows()
        except Exception as exc:
            pytest.skip(f"PostgreSQL not reachable at localhost:5432: {exc}")

        if not rows:
            pytest.skip("No rows with non-empty SearchPhrase in PostgreSQL")

        for i, row in enumerate(rows):
            for j, val in enumerate(row):
                assert not BYTES_REPR_RE.match(val), (
                    f"Row {i} col {j} has bytes repr: {val!r}"
                )

    def test_clickhouse_pg_text_match(self):
        """ClickHouse and PostgreSQL should return the same text values."""
        try:
            ch_rows = self._clickhouse_rows()
        except Exception as exc:
            pytest.skip(f"ClickHouse not reachable: {exc}")

        try:
            pg_rows = self._postgresql_rows()
        except Exception as exc:
            pytest.skip(f"PostgreSQL not reachable: {exc}")

        if not ch_rows or not pg_rows:
            pytest.skip("Not enough data in one or both databases")

        n = min(len(ch_rows), len(pg_rows))
        for i in range(n):
            assert ch_rows[i] == pg_rows[i], (
                f"Row {i} mismatch:\n"
                f"  ClickHouse: {ch_rows[i]}\n"
                f"  PostgreSQL: {pg_rows[i]}"
            )
