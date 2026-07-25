"""
Integration tests for ClickBench Showdown containers and services.

Tests verify:
  1. Core services (backend, dashboard, loader) are up and responding
  2. Database containers are running and healthy via Docker API
  3. Database connections work (SELECT 1) from the host
  4. Backend API endpoints return valid data

Run:
    pip install pytest pytest-asyncio httpx asyncpg aiomysql pymonetdb docker pyyaml
    pytest tests/test_containers.py -v
"""

from __future__ import annotations

import asyncio
import os
import socket
import sys
from pathlib import Path

import docker
import httpx
import pytest
import yaml

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATABASES_DIR = PROJECT_ROOT / "databases"
BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8000")
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "http://localhost:3000")


def _load_db_configs() -> dict[str, dict]:
    """Load all database configs from databases/ directory."""
    configs = {}
    for entry in sorted(DATABASES_DIR.iterdir()):
        config_path = entry / "config.yaml"
        if entry.is_dir() and config_path.exists():
            with open(config_path) as f:
                cfg = yaml.safe_load(f)
            configs[cfg.get("id", entry.name)] = cfg
    return configs


DB_CONFIGS = _load_db_configs()


def _get_docker_client():
    try:
        return docker.from_env()
    except Exception:
        pytest.skip("Docker not available")


# ---------------------------------------------------------------------------
# 1. Core services
# ---------------------------------------------------------------------------


class TestCoreServices:
    """Verify the 3 always-on services are reachable."""

    def test_backend_health(self):
        r = httpx.get(f"{BACKEND_URL}/databases", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)

    def test_backend_status(self):
        r = httpx.get(f"{BACKEND_URL}/status", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert "running" in data
        assert "selected" in data
        assert "loader" in data

    def test_dashboard_serves_html(self):
        r = httpx.get(DASHBOARD_URL, timeout=5)
        assert r.status_code == 200
        assert "text/html" in r.headers.get("content-type", "")


# ---------------------------------------------------------------------------
# 2. Docker container health
# ---------------------------------------------------------------------------


class TestDockerContainers:
    """Check every DB container exists and is healthy."""

    @pytest.fixture(autouse=True)
    def _client(self):
        self.client = _get_docker_client()

    @pytest.mark.parametrize("db_id,container_name", [
        pytest.param(db_id, cfg.get("docker", {}).get("container_name", f"showdown-{db_id}"), id=db_id)
        for db_id, cfg in DB_CONFIGS.items()
    ])
    def test_container_exists_and_running(self, db_id, container_name):
        try:
            container = self.client.containers.get(container_name)
        except docker.errors.NotFound:
            pytest.fail(f"Container '{container_name}' for {db_id} not found. Start it via the backend /select endpoint.")
        assert container.status == "running", (
            f"Container '{container_name}' status is '{container.status}', expected 'running'"
        )

    @pytest.mark.parametrize("db_id,container_name", [
        pytest.param(db_id, cfg.get("docker", {}).get("container_name", f"showdown-{db_id}"), id=db_id)
        for db_id, cfg in DB_CONFIGS.items()
        if cfg.get("docker", {}).get("healthcheck")
    ])
    def test_container_healthy(self, db_id, container_name):
        try:
            container = self.client.containers.get(container_name)
        except docker.errors.NotFound:
            pytest.skip(f"Container '{container_name}' not running")
        container.reload()
        health = container.attrs.get("State", {}).get("Health", {}).get("Status")
        assert health == "healthy", (
            f"Container '{container_name}' health is '{health}', expected 'healthy'"
        )


# ---------------------------------------------------------------------------
# 3. Database connectivity — direct from host
# ---------------------------------------------------------------------------


def _host_port(cfg: dict, container_port: int) -> int | None:
    """Derive the host port for a given container port from config.

    Config format: {container_port_str: host_port} (Docker SDK convention).
    Also tries the reverse in case config uses docker-compose style.
    """
    ports = cfg.get("docker", {}).get("ports", {})
    # Try container_port as key (Docker SDK style)
    for k, v in ports.items():
        if int(k) == container_port:
            return int(v)
    # Try container_port as value (docker-compose style)
    for k, v in ports.items():
        if int(v) == container_port:
            return int(k)
    return None


def _tcp_reachable(host: str, port: int, timeout: float = 3) -> bool:
    """Quick TCP connect check."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, ConnectionRefusedError):
        return False


class TestClickHouseConnection:
    """ClickHouse — HTTP ping and SELECT 1."""

    def _port(self):
        cfg = DB_CONFIGS.get("clickhouse")
        if not cfg:
            pytest.skip("clickhouse config not found")
        return _host_port(cfg, 8123) or 8123

    def test_tcp_reachable(self):
        port = self._port()
        if not _tcp_reachable("localhost", port):
            pytest.skip(f"ClickHouse not reachable on localhost:{port}")
        assert True

    def test_http_ping(self):
        port = self._port()
        r = httpx.get(f"http://localhost:{port}/ping", timeout=5)
        assert r.status_code == 200

    def test_select_1(self):
        port = self._port()
        cfg = DB_CONFIGS["clickhouse"]["connection"]
        r = httpx.get(
            f"http://localhost:{port}/",
            params={"query": "SELECT 1", "user": cfg["user"], "password": cfg["password"]},
            timeout=5,
        )
        assert r.status_code == 200
        assert r.text.strip() == "1"


@pytest.mark.asyncio
class TestPostgreSQLConnection:
    """PostgreSQL — asyncpg SELECT 1."""

    def _params(self):
        cfg = DB_CONFIGS.get("postgresql")
        if not cfg:
            pytest.skip("postgresql config not found")
        conn = cfg["connection"]
        port = _host_port(cfg, int(conn["port"])) or int(conn["port"])
        return conn["user"], conn["password"], conn["database"], port

    async def test_select_1(self):
        import asyncpg
        user, password, database, port = self._params()
        if not _tcp_reachable("localhost", port):
            pytest.skip(f"PostgreSQL not reachable on localhost:{port}")
        conn = await asyncpg.connect(
            host="localhost", port=port, user=user, password=password, database=database,
        )
        try:
            val = await conn.fetchval("SELECT 1")
            assert val == 1
        finally:
            await conn.close()


@pytest.mark.asyncio
class TestTimescaleDBConnection:
    """TimescaleDB — asyncpg SELECT 1."""

    def _params(self):
        cfg = DB_CONFIGS.get("timescaledb")
        if not cfg:
            pytest.skip("timescaledb config not found")
        conn = cfg["connection"]
        port = _host_port(cfg, int(conn["port"])) or int(conn["port"])
        return conn["user"], conn["password"], conn["database"], port

    async def test_select_1(self):
        import asyncpg
        user, password, database, port = self._params()
        if not _tcp_reachable("localhost", port):
            pytest.skip(f"TimescaleDB not reachable on localhost:{port}")
        conn = await asyncpg.connect(
            host="localhost", port=port, user=user, password=password, database=database,
        )
        try:
            val = await conn.fetchval("SELECT 1")
            assert val == 1
        finally:
            await conn.close()


@pytest.mark.asyncio
class TestMySQLConnection:
    """MySQL — aiomysql SELECT 1."""

    def _params(self):
        cfg = DB_CONFIGS.get("mysql")
        if not cfg:
            pytest.skip("mysql config not found")
        conn = cfg["connection"]
        port = _host_port(cfg, int(conn["port"])) or int(conn["port"])
        return conn["user"], conn["password"], conn["database"], port

    async def test_select_1(self):
        import aiomysql
        user, password, database, port = self._params()
        if not _tcp_reachable("localhost", port):
            pytest.skip(f"MySQL not reachable on localhost:{port}")
        conn = await aiomysql.connect(
            host="localhost", port=port, user=user, password=password, db=database,
        )
        try:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                row = await cur.fetchone()
                assert row[0] == 1
        finally:
            await conn.ensure_closed()


@pytest.mark.asyncio
class TestMariaDBConnection:
    """MariaDB — aiomysql SELECT 1."""

    def _params(self):
        cfg = DB_CONFIGS.get("mariadb")
        if not cfg:
            pytest.skip("mariadb config not found")
        conn = cfg["connection"]
        port = _host_port(cfg, int(conn["port"])) or int(conn["port"])
        return conn["user"], conn["password"], conn["database"], port

    async def test_select_1(self):
        import aiomysql
        user, password, database, port = self._params()
        if not _tcp_reachable("localhost", port):
            pytest.skip(f"MariaDB not reachable on localhost:{port}")
        conn = await aiomysql.connect(
            host="localhost", port=port, user=user, password=password, db=database,
        )
        try:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                row = await cur.fetchone()
                assert row[0] == 1
        finally:
            await conn.ensure_closed()


class TestMonetDBConnection:
    """MonetDB — pymonetdb SELECT 1."""

    def _params(self):
        cfg = DB_CONFIGS.get("monetdb")
        if not cfg:
            pytest.skip("monetdb config not found")
        conn = cfg["connection"]
        port = _host_port(cfg, int(conn["port"])) or int(conn["port"])
        return conn["user"], conn["password"], conn["database"], port

    def test_select_1(self):
        import pymonetdb
        user, password, database, port = self._params()
        if not _tcp_reachable("localhost", port):
            pytest.skip(f"MonetDB not reachable on localhost:{port}")
        conn = pymonetdb.connect(
            hostname="localhost", port=port, username=user, password=password, database=database,
        )
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            row = cur.fetchone()
            assert row[0] == 1
            cur.close()
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# 4. Backend loader compatibility
# ---------------------------------------------------------------------------


class TestBackendLoaderCompat:
    """Verify that every loader.py can be imported inside the backend container.

    The backend calls create_schema() from each database's loader.py.
    If a loader has top-level imports (e.g. pandas, curl subprocess) that
    aren't available in the backend image, schema creation silently fails.
    """

    BACKEND_CONTAINER = "showdown-backend"

    @pytest.fixture(autouse=True)
    def _client(self):
        self.client = _get_docker_client()
        try:
            c = self.client.containers.get(self.BACKEND_CONTAINER)
            if c.status != "running":
                pytest.skip("Backend container not running")
        except docker.errors.NotFound:
            pytest.skip("Backend container not found")

    def _exec_in_backend(self, python_code: str) -> tuple[int, str]:
        """Run python code inside the backend container. Returns (exit_code, output)."""
        c = self.client.containers.get(self.BACKEND_CONTAINER)
        result = c.exec_run(["python3", "-c", python_code], demux=True)
        stdout = (result.output[0] or b"").decode()
        stderr = (result.output[1] or b"").decode()
        return result.exit_code, stdout + stderr

    @pytest.mark.parametrize("db_id", sorted(DB_CONFIGS.keys()))
    def test_loader_imports_in_backend(self, db_id):
        """Each loader.py must import without error inside the backend container."""
        code = (
            "import importlib.util, sys; "
            f"spec = importlib.util.spec_from_file_location('loader_{db_id}', '/app/databases/{db_id}/loader.py'); "
            "mod = importlib.util.module_from_spec(spec); "
            "spec.loader.exec_module(mod); "
            "print('ok')"
        )
        exit_code, output = self._exec_in_backend(code)
        assert exit_code == 0, (
            f"loader.py for {db_id} failed to import in backend container:\n{output}"
        )

    @pytest.mark.parametrize("db_id", sorted(DB_CONFIGS.keys()))
    def test_create_schema_callable_in_backend(self, db_id):
        """Each loader.py must expose a create_schema() that is callable without
        importing unavailable dependencies (pandas, curl, etc.)."""
        code = (
            "import importlib.util; "
            f"spec = importlib.util.spec_from_file_location('loader_{db_id}', '/app/databases/{db_id}/loader.py'); "
            "mod = importlib.util.module_from_spec(spec); "
            "spec.loader.exec_module(mod); "
            "fn = getattr(mod, 'create_schema', None); "
            "assert fn is not None, 'create_schema not found'; "
            "assert callable(fn), 'create_schema not callable'; "
            "print('ok')"
        )
        exit_code, output = self._exec_in_backend(code)
        assert exit_code == 0, (
            f"create_schema for {db_id} not usable in backend container:\n{output}"
        )

    def test_backend_has_no_curl_dependency(self):
        """Backend image should not rely on curl being installed."""
        exit_code, output = self._exec_in_backend(
            "import subprocess; "
            "r = subprocess.run(['which', 'curl'], capture_output=True); "
            "print('curl found' if r.returncode == 0 else 'no curl'); "
        )
        # If curl IS present, that's fine — but loaders must not depend on it.
        # This test just documents the state. The loader import tests above
        # are the ones that actually catch the bug.
        if "curl found" in output:
            import warnings
            warnings.warn("curl is present in backend image — loaders should still not depend on it")

    @pytest.mark.parametrize("db_id", sorted(DB_CONFIGS.keys()))
    def test_schema_sql_exists(self, db_id):
        """Each database plugin must have a schema.sql file."""
        schema_path = DATABASES_DIR / db_id / "schema.sql"
        if not schema_path.exists():
            pytest.fail(f"Missing schema.sql for {db_id}")


class TestLoaderLoaderCompat:
    """Verify that every loader.py can be fully imported inside the loader container,
    including heavy dependencies like pandas/pyarrow/pymysql/pymonetdb/duckdb."""

    LOADER_CONTAINER = "showdown-loader"

    @pytest.fixture(autouse=True)
    def _client(self):
        self.client = _get_docker_client()
        try:
            c = self.client.containers.get(self.LOADER_CONTAINER)
            if c.status != "running":
                pytest.skip("Loader container not running")
        except docker.errors.NotFound:
            pytest.skip("Loader container not found")

    def _exec_in_loader(self, python_code: str) -> tuple[int, str]:
        c = self.client.containers.get(self.LOADER_CONTAINER)
        result = c.exec_run(["python3", "-c", python_code], demux=True)
        stdout = (result.output[0] or b"").decode()
        stderr = (result.output[1] or b"").decode()
        return result.exit_code, stdout + stderr

    @pytest.mark.parametrize("db_id", sorted(DB_CONFIGS.keys()))
    def test_loader_full_import(self, db_id):
        """Each loader.py must import fully (including pandas/pyarrow) in the loader container."""
        code = (
            "import importlib.util, sys, os; "
            "sys.path.insert(0, '/app'); "
            f"spec = importlib.util.spec_from_file_location('loader_{db_id}', '/app/databases/{db_id}/loader.py'); "
            "mod = importlib.util.module_from_spec(spec); "
            "spec.loader.exec_module(mod); "
            "assert callable(getattr(mod, 'load', None)), 'load() missing'; "
            "assert callable(getattr(mod, 'create_schema', None)), 'create_schema() missing'; "
            "print('ok')"
        )
        exit_code, output = self._exec_in_loader(code)
        assert exit_code == 0, (
            f"loader.py for {db_id} failed to import in loader container:\n{output}"
        )


# ---------------------------------------------------------------------------
# 5. Config validation
# ---------------------------------------------------------------------------


class TestConfigValidation:
    """Catch config.yaml issues (wrong ports, missing fields, credential mismatches)."""

    @pytest.mark.parametrize("db_id,cfg", list(DB_CONFIGS.items()), ids=list(DB_CONFIGS.keys()))
    def test_required_fields(self, db_id, cfg):
        """Every config must have id, display_name, and connection."""
        assert "id" in cfg, f"{db_id}: missing 'id'"
        assert "display_name" in cfg, f"{db_id}: missing 'display_name'"
        assert "connection" in cfg, f"{db_id}: missing 'connection'"
        conn = cfg["connection"]
        assert "protocol" in conn, f"{db_id}: missing connection.protocol"

    @pytest.mark.parametrize("db_id,cfg", [
        (db_id, cfg) for db_id, cfg in DB_CONFIGS.items()
    ], ids=list(DB_CONFIGS.keys()))
    def test_docker_config_present(self, db_id, cfg):
        """All databases must have docker config with image or build."""
        assert "docker" in cfg, f"{db_id}: missing 'docker' section"
        docker_cfg = cfg["docker"]
        assert "image" in docker_cfg or "build" in docker_cfg, f"{db_id}: missing docker.image or docker.build"

    @pytest.mark.parametrize("db_id,cfg", [
        (db_id, cfg) for db_id, cfg in DB_CONFIGS.items()
        if cfg.get("docker", {}).get("ports")
    ], ids=[db_id for db_id, cfg in DB_CONFIGS.items()
            if cfg.get("docker", {}).get("ports")])
    def test_port_mapping_format(self, db_id, cfg):
        """Port mappings must use Docker SDK format: {container_port: host_port}.
        The container port (key) must match what the DB actually listens on."""
        conn = cfg["connection"]
        container_port = int(conn["port"])
        ports = cfg["docker"]["ports"]
        port_keys = [int(k) for k in ports.keys()]
        assert container_port in port_keys, (
            f"{db_id}: connection.port={container_port} not found as a key in docker.ports={ports}. "
            f"Docker SDK format is {{container_port: host_port}}. "
            f"Keys are {port_keys}, expected {container_port} to be one of them."
        )

    @pytest.mark.parametrize("db_id,cfg", [
        (db_id, cfg) for db_id, cfg in DB_CONFIGS.items()
        if cfg.get("docker", {}).get("ports")
    ], ids=[db_id for db_id, cfg in DB_CONFIGS.items()
            if cfg.get("docker", {}).get("ports")])
    def test_no_host_port_conflicts(self, db_id, cfg):
        """No two databases should map to the same host port."""
        ports = cfg["docker"]["ports"]
        host_ports = [int(v) for v in ports.values()]
        for hp in host_ports:
            # Check all other configs for the same host port
            for other_id, other_cfg in DB_CONFIGS.items():
                if other_id == db_id:
                    continue
                other_ports = other_cfg.get("docker", {}).get("ports", {})
                other_host_ports = [int(v) for v in other_ports.values()]
                assert hp not in other_host_ports, (
                    f"{db_id} and {other_id} both map to host port {hp}"
                )

    @pytest.mark.parametrize("db_id,cfg", [
        (db_id, cfg) for db_id, cfg in DB_CONFIGS.items()
        if cfg.get("docker", {}).get("environment")
    ], ids=[db_id for db_id, cfg in DB_CONFIGS.items()
            if cfg.get("docker", {}).get("environment")])
    def test_credentials_match_environment(self, db_id, cfg):
        """Connection credentials should match the Docker environment variables."""
        conn = cfg["connection"]
        env = cfg["docker"]["environment"]
        password = conn.get("password", "")

        # Check common password env vars
        pw_env_keys = [k for k in env if "PASS" in k.upper()]
        if pw_env_keys and password:
            env_passwords = [str(env[k]) for k in pw_env_keys]
            assert password in env_passwords, (
                f"{db_id}: connection.password='{password}' doesn't match any "
                f"password env vars: {dict((k, env[k]) for k in pw_env_keys)}"
            )


# ---------------------------------------------------------------------------
# 6. Backend API integration
# ---------------------------------------------------------------------------


class TestBackendAPI:
    """Verify the backend API returns well-formed data."""

    def test_databases_list_matches_configs(self):
        """Backend /databases should list all databases from the registry."""
        r = httpx.get(f"{BACKEND_URL}/databases", timeout=5)
        assert r.status_code == 200
        data = r.json()
        api_ids = {db["id"] for db in data}
        config_ids = set(DB_CONFIGS.keys())
        assert config_ids == api_ids, (
            f"Config has {config_ids - api_ids} not in API; API has {api_ids - config_ids} not in config"
        )

    def test_databases_have_required_fields(self):
        r = httpx.get(f"{BACKEND_URL}/databases", timeout=5)
        for db in r.json():
            assert "id" in db
            assert "display_name" in db
            assert "running" in db

    def test_status_structure(self):
        r = httpx.get(f"{BACKEND_URL}/status", timeout=5)
        data = r.json()
        assert "running" in data
        assert "selected" in data
        assert "db_a" in data["selected"]
        assert "db_b" in data["selected"]
        assert "loader" in data
        assert "ready" in data["loader"]
        assert "message" in data["loader"]

    def test_select_same_db_rejected(self):
        """Selecting the same DB for both slots should return an error."""
        first_db = next(iter(DB_CONFIGS))
        r = httpx.post(
            f"{BACKEND_URL}/select",
            params={"db_a": first_db, "db_b": first_db},
            timeout=10,
        )
        assert r.status_code == 200
        assert r.json().get("status") == "error"

    def test_select_unknown_db_rejected(self):
        first_db = next(iter(DB_CONFIGS))
        r = httpx.post(
            f"{BACKEND_URL}/select",
            params={"db_a": first_db, "db_b": "nonexistent_db_xyz"},
            timeout=10,
        )
        assert r.status_code == 200
        assert r.json().get("status") == "error"
