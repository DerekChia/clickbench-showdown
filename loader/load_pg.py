#!/usr/bin/env python3
r"""
Load ClickBench athena parquet files (hits_0..hits_9) into PostgreSQL.

Two-phase approach:
  Phase 1 — download all 10 parquet files in parallel (10 threads).
  Phase 2 — convert + INSERT each file in parallel (4 workers, own connection each).

Type conversion is done with pandas vectorized operations (C-level) instead of
row-by-row Python loops, which is orders of magnitude faster for 1 M rows x 105 cols.
"""

import csv
import io
import os
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import pyarrow.parquet as pq
import psycopg2
import requests

# ── Config ─────────────────────────────────────────────────────────────────────
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_USER = os.getenv("POSTGRES_USER", "bench_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "bench_pass")
PG_DB   = os.getenv("POSTGRES_DB",   "hits")

BASE_URL         = "https://datasets.clickhouse.com/hits_compatible/athena_partitioned"
PARTS            = list(range(int(os.getenv("PARQUET_FILES", "5"))))
PARQUET_DIR      = os.getenv("PARQUET_DIR", "")   # set by load.sh when files are pre-downloaded
DOWNLOAD_WORKERS = 10
DOWNLOAD_RETRIES = 5
DOWNLOAD_BACKOFF = 3   # seconds; doubles each retry


def _available_memory_gib() -> float:
    """Return available memory in GiB by reading /proc/meminfo (Linux)."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / (1024 ** 2)
    except OSError:
        pass
    return 0.0


_mem_gib = _available_memory_gib()
_env_workers = os.getenv("INSERT_WORKERS", "").strip()
if _env_workers:
    INSERT_WORKERS = int(_env_workers)
else:
    INSERT_WORKERS = 2 if _mem_gib >= 8.0 else 1

# Columns needing vectorized type conversion before COPY
TIMESTAMP_COLS = ["eventtime", "clienteventtime", "localeventtime"]
DATE_COLS      = ["eventdate"]
CHAR1_COLS     = ["hitcolor"]

_print_lock = threading.Lock()


def log(msg: str) -> None:
    with _print_lock:
        print(msg, flush=True)


# ── Parquet → pandas with vectorized type fixes ────────────────────────────────

def parquet_to_dataframe(path: str) -> pd.DataFrame:
    """Read a parquet file and apply all type conversions vectorized."""
    table = pq.read_table(path)
    df = table.to_pandas()
    # Parquet columns are lowercase (Athena convention) — matches PG schema.
    df.columns = [c.lower() for c in df.columns]

    # Unix epoch seconds → timestamp string  (vectorized, C-level)
    for col in TIMESTAMP_COLS:
        if col in df.columns:
            df[col] = (
                pd.to_datetime(df[col], unit="s", utc=True)
                .dt.tz_localize(None)
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )

    # Days since epoch → date string  (vectorized)
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit="D").dt.strftime("%Y-%m-%d")

    # Single-character column stored as either a bytes object or an integer code point
    for col in CHAR1_COLS:
        if col in df.columns:
            def _to_char(x):
                if pd.isna(x):
                    return ""
                if isinstance(x, (bytes, bytearray)):
                    return x.decode("latin-1")
                return chr(int(x))
            df[col] = df[col].apply(_to_char)

    return df


def dataframe_to_copy_buffer(df: pd.DataFrame) -> io.StringIO:
    """Serialise DataFrame to a tab-separated CSV buffer for PostgreSQL COPY."""
    buf = io.StringIO()
    df.to_csv(
        buf,
        sep="\t",
        header=False,
        index=False,
        na_rep="",           # NaN → empty string (matched by NULL '' in COPY)
        quoting=csv.QUOTE_MINIMAL,
    )
    buf.seek(0)
    return buf


# ── Phase 1: parallel download ─────────────────────────────────────────────────

def download_part(part_num: int) -> tuple[int, str]:
    url = f"{BASE_URL}/hits_{part_num}.parquet"
    t0 = time.time()
    delay = DOWNLOAD_BACKOFF
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        log(f"  [hits_{part_num}] Downloading {url} … (attempt {attempt}/{DOWNLOAD_RETRIES})")
        fd, path = tempfile.mkstemp(suffix=f"_hits_{part_num}.parquet")
        try:
            resp = requests.get(url, stream=True, timeout=600)
            resp.raise_for_status()
            total = 0
            with os.fdopen(fd, "wb") as f:
                for chunk in resp.iter_content(chunk_size=4 * 1024 * 1024):
                    f.write(chunk)
                    total += len(chunk)
            log(f"  [hits_{part_num}] Downloaded {total / 1e6:.1f} MB in {time.time() - t0:.1f}s")
            return part_num, path
        except Exception as exc:
            os.unlink(path)
            if attempt == DOWNLOAD_RETRIES:
                raise
            log(f"  [hits_{part_num}] Attempt {attempt} failed ({exc}), retrying in {delay}s…")
            time.sleep(delay)
            delay *= 2


def download_all() -> dict[int, str]:
    log(f"[load_pg] Phase 1 — downloading {len(PARTS)} files in parallel…")
    t0 = time.time()
    results: dict[int, str] = {}
    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as pool:
        futures = {pool.submit(download_part, p): p for p in PARTS}
        for fut in as_completed(futures):
            part_num, path = fut.result()
            results[part_num] = path
    log(f"[load_pg] Phase 1 done in {time.time() - t0:.1f}s")
    return results


# ── Phase 2: parallel insert ───────────────────────────────────────────────────

def pg_connect() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=PG_HOST, port=int(PG_PORT),
        dbname=PG_DB, user=PG_USER, password=PG_PASS,
    )
    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit = off")
    conn.commit()
    return conn


def insert_part(part_num: int, path: str) -> int:
    log(f"  [hits_{part_num}] Converting parquet → DataFrame…")
    t0 = time.time()
    df = parquet_to_dataframe(path)
    n_rows = len(df)
    col_names = list(df.columns)
    buf = dataframe_to_copy_buffer(df)
    del df
    log(f"  [hits_{part_num}] Converted {n_rows:,} rows in {time.time() - t0:.1f}s — inserting…")

    conn = pg_connect()
    t1 = time.time()
    try:
        with conn.cursor() as cur:
            cols = ", ".join(col_names)
            cur.copy_expert(
                f"COPY hits ({cols}) FROM STDIN"
                " WITH (FORMAT csv, DELIMITER E'\\t', NULL '')",
                buf,
            )
        conn.commit()
    finally:
        conn.close()

    log(f"  [hits_{part_num}] Inserted {n_rows:,} rows in {time.time() - t1:.1f}s")
    return n_rows


def insert_all(tmp_files: dict[int, str], cleanup: bool = True) -> int:
    log(f"[load_pg] Inserting {len(tmp_files)} files with {INSERT_WORKERS} workers…")
    t0 = time.time()
    total = 0
    try:
        with ThreadPoolExecutor(max_workers=INSERT_WORKERS) as pool:
            futures = {pool.submit(insert_part, p, tmp_files[p]): p for p in PARTS}
            for fut in as_completed(futures):
                total += fut.result()
    finally:
        if cleanup:
            for path in tmp_files.values():
                try:
                    os.unlink(path)
                except OSError:
                    pass
    log(f"[load_pg] Done — {total:,} rows inserted in {time.time() - t0:.1f}s")
    return total


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    source = "flag" if _env_workers else f"auto-detected, {_mem_gib:.1f} GiB available"
    log(f"[load_pg] Insert workers: {INSERT_WORKERS} ({source}).")
    log("[load_pg] Connecting to PostgreSQL…")
    setup_conn = psycopg2.connect(
        host=PG_HOST, port=int(PG_PORT),
        dbname=PG_DB, user=PG_USER, password=PG_PASS,
    )
    with setup_conn.cursor() as cur:
        cur.execute("ALTER TABLE hits SET UNLOGGED")
        cur.execute("TRUNCATE TABLE hits")
    setup_conn.commit()
    setup_conn.close()

    if PARQUET_DIR:
        log(f"[load_pg] Using pre-downloaded files from {PARQUET_DIR}")
        tmp_files = {p: os.path.join(PARQUET_DIR, f"hits_{p}.parquet") for p in PARTS}
        total = insert_all(tmp_files, cleanup=False)
    else:
        tmp_files = download_all()
        total     = insert_all(tmp_files, cleanup=True)

    log("[load_pg] Restoring durability and running VACUUM ANALYZE…")
    final_conn = psycopg2.connect(
        host=PG_HOST, port=int(PG_PORT),
        dbname=PG_DB, user=PG_USER, password=PG_PASS,
    )
    with final_conn.cursor() as cur:
        cur.execute("ALTER TABLE hits SET LOGGED")
    final_conn.commit()
    final_conn.autocommit = True
    with final_conn.cursor() as cur:
        cur.execute("VACUUM ANALYZE hits")
    final_conn.close()

    log(f"[load_pg] Done — {total:,} total rows loaded into PostgreSQL.")


if __name__ == "__main__":
    main()
