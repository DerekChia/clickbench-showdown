"""DuckDB data loader — sends parquet data via the DuckDB HTTP server container."""

import os
import time


def _post_sql(config: dict, sql: str) -> str:
    """Send SQL to the DuckDB HTTP server."""
    import requests
    conn = config["connection"]
    url = f"http://{conn['host']}:{conn['port']}/"
    r = requests.post(url, data=sql, timeout=300)
    if r.status_code != 200:
        raise RuntimeError(f"DuckDB error {r.status_code}: {r.text[:500]}")
    return r.text


def load(parquet_dir: str, num_files: int, config: dict) -> int:
    """Load parquet files into DuckDB. Returns total rows inserted."""
    # Truncate existing data. A failure here must be fatal — otherwise the load
    # appends to what's already there and silently doubles the row count.
    _post_sql(config, "DELETE FROM hits")

    # DuckDB container has /data mounted; parquet files are in the loader's /tmp/hits_parquet.
    # We need to read parquet files and insert via SQL.
    # Since the DuckDB container can't see the loader's filesystem, we convert to CSV
    # and send via INSERT statements, or use the parquet_scan with a shared volume.
    # Simplest: use the DuckDB container's read_parquet on files accessible to it.
    # We'll copy parquet files to DuckDB's data volume via docker.
    import docker
    client = docker.from_env()
    container = client.containers.get(config.get("docker", {}).get("container_name", "showdown-duckdb"))

    total = 0
    for i in range(num_files):
        path = os.path.join(parquet_dir, f"hits_{i}.parquet")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing parquet file: {path}")

        print(f"  [duckdb] Copying hits_{i}.parquet to container...", flush=True)
        t0 = time.time()

        # Copy file into container at /data/
        import tarfile
        import io
        tar_stream = io.BytesIO()
        with tarfile.open(fileobj=tar_stream, mode='w') as tar:
            tar.add(path, arcname=f"hits_{i}.parquet")
        tar_stream.seek(0)
        container.put_archive("/data", tar_stream)

        print(f"  [duckdb] Inserting hits_{i}.parquet...", flush=True)
        _post_sql(config, f"""
            INSERT INTO hits
            SELECT
                * REPLACE (
                    epoch_ms(EventTime * 1000) AS EventTime,
                    CAST(DATE '1970-01-01' + INTERVAL (EventDate) DAY AS DATE) AS EventDate,
                    epoch_ms(ClientEventTime * 1000) AS ClientEventTime,
                    epoch_ms(LocalEventTime * 1000) AS LocalEventTime
                )
            FROM read_parquet('/data/hits_{i}.parquet')
        """)
        total_text = _post_sql(config, "SELECT count(*) FROM hits")
        current = int(total_text.strip())
        print(f"  [duckdb] hits_{i} done in {time.time() - t0:.1f}s ({current:,} rows total)", flush=True)

    result = _post_sql(config, "SELECT count(*) FROM hits")
    total = int(result.strip())
    return total


def truncate(config: dict) -> None:
    _post_sql(config, "DELETE FROM hits")


def create_schema(config: dict, schema_path: str) -> None:
    with open(schema_path) as f:
        sql = f.read()
    _post_sql(config, sql)
