"""MonetDB data loader — convert parquet to CSV, bulk load via pymonetdb COPY INTO."""

import csv
import os
import time

import pymonetdb

TIMESTAMP_COLS = ["eventtime", "clienteventtime", "localeventtime"]
DATE_COLS = ["eventdate"]
CHAR1_COLS = ["hitcolor"]


def _connect(config: dict):
    conn_cfg = config["connection"]
    return pymonetdb.connect(
        hostname=conn_cfg["host"],
        port=int(conn_cfg["port"]),
        username=conn_cfg["user"],
        password=conn_cfg["password"],
        database=conn_cfg["database"],
    )


def _parquet_to_csv(path: str, out_path: str) -> int:
    """Convert parquet to pipe-delimited CSV for MonetDB COPY INTO."""
    import pandas as pd
    import pyarrow.parquet as pq
    table = pq.read_table(path)
    df = table.to_pandas()
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

    n_rows = len(df)
    df.to_csv(out_path, sep="|", header=False, index=False, na_rep="", quoting=csv.QUOTE_MINIMAL)
    return n_rows


def _copy_to_container(container_name: str, local_path: str, container_dir: str) -> None:
    """Copy a file into a Docker container."""
    import docker
    import tarfile
    import io

    client = docker.from_env()
    container = client.containers.get(container_name)
    tar_stream = io.BytesIO()
    with tarfile.open(fileobj=tar_stream, mode='w') as tar:
        tar.add(local_path, arcname=os.path.basename(local_path))
    tar_stream.seek(0)
    container.put_archive(container_dir, tar_stream)


def load(parquet_dir: str, num_files: int, config: dict) -> int:
    """Load parquet files into MonetDB. Returns total rows inserted."""
    conn = _connect(config)
    cursor = conn.cursor()
    container_name = config.get("docker", {}).get("container_name", "showdown-monetdb")

    # Truncate existing data. A failure here must be fatal — otherwise the load
    # appends to what's already there and silently doubles the row count.
    try:
        cursor.execute("DELETE FROM hits")
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    total = 0
    for i in range(num_files):
        path = os.path.join(parquet_dir, f"hits_{i}.parquet")
        csv_path = os.path.join(parquet_dir, f"hits_{i}_monetdb.csv")
        csv_filename = f"hits_{i}_monetdb.csv"
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing parquet file: {path}")

        t0 = time.time()
        if os.path.exists(csv_path):
            print(f"  [monetdb] hits_{i}_monetdb.csv already cached — skipping conversion.", flush=True)
            with open(csv_path) as f:
                n_rows = sum(1 for _ in f)
        else:
            print(f"  [monetdb] Converting hits_{i}.parquet...", flush=True)
            # Convert into a .part file and rename on success, so an interrupted
            # run can never leave a truncated CSV that later runs treat as cached.
            part_path = csv_path + ".part"
            n_rows = _parquet_to_csv(path, part_path)
            os.replace(part_path, csv_path)

        # Copy CSV into MonetDB container (COPY INTO reads from server filesystem)
        print(f"  [monetdb] Copying CSV to container...", flush=True)
        _copy_to_container(container_name, csv_path, "/tmp")

        print(f"  [monetdb] Loading {n_rows:,} rows...", flush=True)
        cursor.execute(
            f"COPY INTO hits FROM '/tmp/{csv_filename}' "
            f"USING DELIMITERS '|', '\\n', '\"' NULL AS ''"
        )
        conn.commit()
        total += n_rows
        print(f"  [monetdb] hits_{i} done in {time.time() - t0:.1f}s", flush=True)

    cursor.close()
    conn.close()
    return total


def truncate(config: dict) -> None:
    conn = _connect(config)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM hits")
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
