"""ClickHouse data loader — bulk insert parquet files over the HTTP interface."""

import os


def _endpoint(config: dict) -> tuple[str, dict]:
    """Return (url, base query params) — auth only when credentials are set."""
    conn = config["connection"]
    url = f"http://{conn['host']}:{conn['port']}/"
    params = {}
    if conn.get("user"):
        params["user"] = conn["user"]
    if conn.get("password"):
        params["password"] = conn["password"]
    return url, params


def _post(url: str, params: dict, data=None, timeout: float = 3600) -> str:
    """POST to ClickHouse and raise on any non-200 response.

    Credentials go in the query string of an in-process HTTP call rather than
    a curl argv, so they never show up in the container's process list.
    Prefers httpx (backend image), falls back to requests (loader image).
    """
    try:
        import httpx
    except ImportError:
        import requests
        resp = requests.post(url, params=params, data=data, timeout=timeout)
        if resp.status_code != 200:
            raise RuntimeError(f"ClickHouse HTTP {resp.status_code}: {resp.text[:500]}")
        return resp.text

    resp = httpx.post(url, params=params, content=data, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"ClickHouse HTTP {resp.status_code}: {resp.text[:500]}")
    return resp.text


def load(parquet_dir: str, num_files: int, config: dict) -> int:
    """Load parquet files into ClickHouse. Returns total rows inserted."""
    url, base = _endpoint(config)

    # Truncate before loading. A failure here must be fatal: silently carrying
    # on would append to the existing table and double the row count.
    _post(url, base, data="TRUNCATE TABLE IF EXISTS hits", timeout=300)

    insert_params = {
        **base,
        "query": "INSERT INTO hits FORMAT Parquet",
        "max_execution_time": "3600",
        "input_format_parquet_case_insensitive_column_matching": "1",
    }

    for i in range(num_files):
        path = os.path.join(parquet_dir, f"hits_{i}.parquet")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing parquet file: {path}")
        print(f"  [clickhouse] Inserting hits_{i}.parquet...", flush=True)
        with open(path, "rb") as fh:
            _post(url, insert_params, data=fh)

    return int(_post(url, {**base, "query": "SELECT count() FROM hits"}, timeout=300).strip())


def truncate(config: dict) -> None:
    """Truncate the hits table."""
    url, base = _endpoint(config)
    _post(url, base, data="TRUNCATE TABLE IF EXISTS hits", timeout=300)


def create_schema(config: dict, schema_path: str) -> None:
    """Execute the schema SQL file via HTTP."""
    url, base = _endpoint(config)
    with open(schema_path) as f:
        sql = f.read()
    _post(url, base, data=sql, timeout=60)
