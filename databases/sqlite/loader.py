"""SQLite data loader — streams parquet into the SQLite HTTP server container."""

import os
import time

TIMESTAMP_COLS = ["eventtime", "clienteventtime", "localeventtime"]
DATE_COLS = ["eventdate"]
CHAR1_COLS = ["hitcolor"]

# SQLite treats a multi-row VALUES clause as a compound SELECT, capped at
# SQLITE_MAX_COMPOUND_SELECT (500) rows per statement.
ROWS_PER_STATEMENT = 500
# Statements bundled into one POST, executed as a single transaction server-side.
STATEMENTS_PER_REQUEST = 20
# Rows pulled out of the parquet file at a time — bounds loader memory.
PARQUET_BATCH = 50_000


def _post_sql(config: dict, sql: str, path: str = "/") -> str:
    """Send SQL to the SQLite HTTP server."""
    import requests
    conn = config["connection"]
    url = f"http://{conn['host']}:{conn['port']}{path}"
    r = requests.post(url, data=sql.encode("utf-8"), timeout=600)
    if r.status_code != 200:
        raise RuntimeError(f"SQLite error {r.status_code}: {r.text[:500]}")
    return r.text


def _transform(df):
    """Apply the shared ClickBench column transformations to a chunk."""
    import pandas as pd

    df.columns = [c.lower() for c in df.columns]

    for col in TIMESTAMP_COLS:
        if col in df.columns:
            df[col] = (
                pd.to_datetime(df[col], unit="s", utc=True)
                .dt.tz_localize(None)
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )

    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit="D").dt.strftime("%Y-%m-%d")

    for col in CHAR1_COLS:
        if col in df.columns:
            def _to_char(x):
                if pd.isna(x):
                    return ""
                if isinstance(x, (bytes, bytearray)):
                    return x.decode("latin-1")
                return chr(int(x))
            df[col] = df[col].apply(_to_char)

    # Decode any remaining bytes columns (parquet stores strings as binary)
    for col in df.columns:
        if col in CHAR1_COLS:
            continue
        if df[col].dtype == object and len(df) > 0:
            sample = df[col].iloc[0]
            if isinstance(sample, (bytes, bytearray)):
                df[col] = df[col].apply(
                    lambda x: x.decode("utf-8", errors="replace")
                    if isinstance(x, (bytes, bytearray)) else x
                )

    return df


def _literal(v) -> str:
    if v is None or (isinstance(v, float) and v != v):  # NaN check
        return "NULL"
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", errors="replace")
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


def load(parquet_dir: str, num_files: int, config: dict) -> int:
    """Load parquet files into SQLite. Returns total rows inserted.

    Data is streamed straight from parquet on every load. An earlier version
    cached a CSV of the converted rows, but re-reading that cache turned every
    value back into a string and every NULL into an empty string — so a cached
    reload produced a different table than the first load.
    """
    # A failed truncate must be fatal — otherwise the load appends to existing
    # data and silently doubles the row count.
    _post_sql(config, "DELETE FROM hits")

    total = 0
    for i in range(num_files):
        path = os.path.join(parquet_dir, f"hits_{i}.parquet")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing parquet file: {path}")

        t0 = time.time()
        print(f"  [sqlite] Streaming hits_{i}.parquet...", flush=True)
        n_rows = _load_file(config, path)
        total += n_rows
        print(f"  [sqlite] hits_{i}: {n_rows:,} rows in {time.time() - t0:.1f}s", flush=True)

    return total


def _load_file(config: dict, path: str) -> int:
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(path)
    n_rows = 0
    statements: list = []

    for batch in pf.iter_batches(batch_size=PARQUET_BATCH):
        df = _transform(batch.to_pandas())
        col_names = ",".join(df.columns)
        rows = df.values.tolist()
        del df

        for start in range(0, len(rows), ROWS_PER_STATEMENT):
            chunk = rows[start:start + ROWS_PER_STATEMENT]
            values = ",".join(
                "(" + ",".join(_literal(v) for v in row) + ")" for row in chunk
            )
            statements.append(f"INSERT INTO hits ({col_names}) VALUES {values};")
            n_rows += len(chunk)

            if len(statements) >= STATEMENTS_PER_REQUEST:
                _post_sql(config, "\n".join(statements), path="/script")
                statements = []

        del rows

    if statements:
        _post_sql(config, "\n".join(statements), path="/script")

    return n_rows


def truncate(config: dict) -> None:
    _post_sql(config, "DELETE FROM hits")


def create_schema(config: dict, schema_path: str) -> None:
    with open(schema_path) as f:
        sql = f.read()
    _post_sql(config, sql, path="/script")
