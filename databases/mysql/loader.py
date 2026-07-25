"""MySQL data loader — convert parquet to TSV, bulk load via LOAD DATA LOCAL INFILE."""

import csv
import io
import os
import time

import pymysql

TIMESTAMP_COLS = ["eventtime", "clienteventtime", "localeventtime"]
DATE_COLS = ["eventdate"]
CHAR1_COLS = ["hitcolor"]

BATCH_SIZE = 100_000


def _connect(config: dict):
    conn_cfg = config["connection"]
    return pymysql.connect(
        host=conn_cfg["host"],
        port=int(conn_cfg["port"]),
        user=conn_cfg["user"],
        password=conn_cfg["password"],
        database=conn_cfg["database"],
        local_infile=True,
    )


def _parquet_to_tsv(path: str, out_path: str) -> int:
    """Convert parquet to TSV file for MySQL LOAD DATA, streaming in chunks.
    Returns row count."""
    import pandas as pd
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(path)
    total_rows = 0

    for batch_idx, batch in enumerate(pf.iter_batches(batch_size=BATCH_SIZE)):
        df = batch.to_pandas()
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

        total_rows += len(df)
        # First batch writes (truncates), subsequent batches append
        mode = "w" if batch_idx == 0 else "a"
        df.to_csv(out_path, sep="\t", header=False, index=False, na_rep="\\N",
                   quoting=csv.QUOTE_NONE, escapechar="\\", mode=mode)
        del df

    return total_rows


def load(parquet_dir: str, num_files: int, config: dict) -> int:
    """Load parquet files into MySQL. Returns total rows inserted."""
    conn = _connect(config)
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE hits")
    conn.commit()

    total = 0
    for i in range(num_files):
        path = os.path.join(parquet_dir, f"hits_{i}.parquet")
        tsv_path = os.path.join(parquet_dir, f"hits_{i}.tsv")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing parquet file: {path}")

        t0 = time.time()
        if os.path.exists(tsv_path):
            print(f"  [mysql] hits_{i}.tsv already cached — skipping conversion.", flush=True)
            # Count lines to get row count
            with open(tsv_path) as f:
                n_rows = sum(1 for _ in f)
        else:
            print(f"  [mysql] Converting hits_{i}.parquet to TSV (chunked)...", flush=True)
            # Convert into a .part file and rename on success, so an interrupted
            # run can never leave a truncated TSV that later runs treat as cached.
            part_path = tsv_path + ".part"
            n_rows = _parquet_to_tsv(path, part_path)
            os.replace(part_path, tsv_path)

        print(f"  [mysql] Loading {n_rows:,} rows via LOAD DATA LOCAL INFILE...", flush=True)
        cursor.execute(
            f"LOAD DATA LOCAL INFILE '{tsv_path}' INTO TABLE hits "
            f"FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'"
        )
        conn.commit()
        total += n_rows

        print(f"  [mysql] hits_{i} done in {time.time() - t0:.1f}s", flush=True)

    cursor.close()
    conn.close()
    return total


def truncate(config: dict) -> None:
    conn = _connect(config)
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE hits")
    conn.commit()
    cursor.close()
    conn.close()


def create_schema(config: dict, schema_path: str) -> None:
    conn = _connect(config)
    cursor = conn.cursor()
    with open(schema_path) as f:
        sql = f.read()
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()
