"""PostgreSQL data loader — bulk insert parquet files via pyarrow + psycopg2 COPY."""

import csv
import io
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

TIMESTAMP_COLS = ["eventtime", "clienteventtime", "localeventtime"]
DATE_COLS = ["eventdate"]
CHAR1_COLS = ["hitcolor"]

BATCH_SIZE = 100_000

_print_lock = threading.Lock()


def _log(msg: str) -> None:
    with _print_lock:
        print(f"  [postgresql] {msg}", flush=True)


def _connect(config: dict):
    import psycopg2
    conn_cfg = config["connection"]
    conn = psycopg2.connect(
        host=conn_cfg["host"],
        port=int(conn_cfg["port"]),
        dbname=conn_cfg["database"],
        user=conn_cfg["user"],
        password=conn_cfg["password"],
    )
    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit = off")
    conn.commit()
    return conn


def _available_memory_gib() -> float:
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / (1024 ** 2)
    except OSError:
        pass
    return 0.0


def _transform_chunk(df):
    """Apply column transformations to a chunk DataFrame in-place and return it."""
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


def _dataframe_to_copy_buffer(df) -> io.StringIO:
    buf = io.StringIO()
    df.to_csv(buf, sep="\t", header=False, index=False, na_rep="\\N", quoting=csv.QUOTE_MINIMAL)
    buf.seek(0)
    return buf


def _insert_part(part_num: int, path: str, config: dict, parquet_dir: str = "") -> int:
    import pyarrow.parquet as pq

    t0 = time.time()
    _log(f"Loading hits_{part_num}.parquet in chunks of {BATCH_SIZE:,} rows...")

    pf = pq.ParquetFile(path)
    col_names = [c.lower() for c in pf.schema_arrow.names]
    cols_sql = ", ".join(col_names)

    conn = _connect(config)
    total_rows = 0
    try:
        for batch in pf.iter_batches(batch_size=BATCH_SIZE):
            df = batch.to_pandas()
            df = _transform_chunk(df)
            n = len(df)
            buf = _dataframe_to_copy_buffer(df)
            del df

            with conn.cursor() as cur:
                cur.copy_expert(
                    f"COPY hits ({cols_sql}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')",
                    buf,
                )
            conn.commit()
            total_rows += n

        _log(f"Inserted {total_rows:,} rows from hits_{part_num}.parquet in {time.time() - t0:.1f}s")
    finally:
        conn.close()

    return total_rows


def load(parquet_dir: str, num_files: int, config: dict) -> int:
    """Load parquet files into PostgreSQL. Returns total rows inserted."""
    env_workers = os.getenv("INSERT_WORKERS", "").strip()
    mem_gib = _available_memory_gib()
    insert_workers = int(env_workers) if env_workers else (2 if mem_gib >= 8.0 else 1)

    conn = _connect(config)
    with conn.cursor() as cur:
        # Also valid on TimescaleDB hypertables — it propagates to the chunks.
        cur.execute("ALTER TABLE hits SET UNLOGGED")
        cur.execute("TRUNCATE TABLE hits")
    conn.commit()
    conn.close()

    parts = list(range(num_files))
    files = {p: os.path.join(parquet_dir, f"hits_{p}.parquet") for p in parts}

    _log(f"Inserting {num_files} files with {insert_workers} workers...")
    t0 = time.time()
    total = 0
    with ThreadPoolExecutor(max_workers=insert_workers) as pool:
        futures = {pool.submit(_insert_part, p, files[p], config, parquet_dir): p for p in parts}
        for fut in as_completed(futures):
            total += fut.result()

    final_conn = _connect(config)
    with final_conn.cursor() as cur:
        cur.execute("ALTER TABLE hits SET LOGGED")
    final_conn.commit()
    final_conn.autocommit = True
    with final_conn.cursor() as cur:
        cur.execute("VACUUM ANALYZE hits")
    final_conn.close()

    _log(f"Done - {total:,} rows in {time.time() - t0:.1f}s")
    return total


def truncate(config: dict) -> None:
    """Truncate the hits table."""
    import psycopg2

    conn_cfg = config["connection"]
    conn = psycopg2.connect(
        host=conn_cfg["host"],
        port=int(conn_cfg["port"]),
        dbname=conn_cfg["database"],
        user=conn_cfg["user"],
        password=conn_cfg["password"],
    )
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE hits")
    conn.commit()
    conn.close()


def create_schema(config: dict, schema_path: str) -> None:
    """Execute the schema SQL file. Uses asyncpg (available in backend) with
    a psycopg2 fallback (available in loader)."""
    conn_cfg = config["connection"]
    with open(schema_path) as f:
        sql = f.read()

    try:
        import asyncpg
        import asyncio

        async def _run():
            conn = await asyncpg.connect(
                host=conn_cfg["host"],
                port=int(conn_cfg["port"]),
                database=conn_cfg["database"],
                user=conn_cfg["user"],
                password=conn_cfg["password"],
            )
            try:
                await conn.execute(sql)
            finally:
                await conn.close()

        # Handle being called from sync context (run_in_executor)
        try:
            loop = asyncio.get_running_loop()
            # Already in an async context — can't use asyncio.run
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                pool.submit(asyncio.run, _run()).result()
        except RuntimeError:
            asyncio.run(_run())
    except ImportError:
        import psycopg2
        conn = psycopg2.connect(
            host=conn_cfg["host"],
            port=int(conn_cfg["port"]),
            dbname=conn_cfg["database"],
            user=conn_cfg["user"],
            password=conn_cfg["password"],
        )
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        conn.close()
