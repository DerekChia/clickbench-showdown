#!/usr/bin/env python3
"""
Smoke test — spins up every database container, verifies health and connectivity.

Usage:
    python3 tests/smoke_test.py              # test all databases
    python3 tests/smoke_test.py clickhouse mariadb   # test specific ones
    python3 tests/smoke_test.py --teardown   # stop containers after testing

Requires:
    pip3 install docker pyyaml httpx asyncpg aiomysql pymonetdb
"""

from __future__ import annotations

import argparse
import asyncio
import socket
import sys
import time
from pathlib import Path

import docker
import docker.types
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATABASES_DIR = PROJECT_ROOT / "databases"
DOCKER_NETWORK = "clickbench-showdown_default"
BACKEND_URL = "http://localhost:8000"
DASHBOARD_URL = "http://localhost:3000"

# ANSI colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


def ok(msg):
    print(f"  {GREEN}PASS{RESET}  {msg}")


def fail(msg):
    print(f"  {RED}FAIL{RESET}  {msg}")


def skip(msg):
    print(f"  {YELLOW}SKIP{RESET}  {msg}")


def header(msg):
    print(f"\n{BOLD}{CYAN}--- {msg} ---{RESET}")


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_all_configs() -> dict[str, dict]:
    configs = {}
    for entry in sorted(DATABASES_DIR.iterdir()):
        cfg_path = entry / "config.yaml"
        if entry.is_dir() and cfg_path.exists():
            with open(cfg_path) as f:
                cfg = yaml.safe_load(f)
            cfg["_dir"] = str(entry)
            cfg["_schema_path"] = str(entry / "schema.sql")
            configs[cfg.get("id", entry.name)] = cfg
    return configs


def get_host_port(cfg: dict) -> int | None:
    """Get the host-side port for the DB's primary connection port."""
    conn = cfg.get("connection", {})
    container_port = int(conn.get("port", 0))
    ports = cfg.get("docker", {}).get("ports", {})
    # Docker SDK: {container_port: host_port}
    for k, v in ports.items():
        if int(k) == container_port:
            return int(v)
    return container_port or None


# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------

def get_docker_client():
    return docker.from_env()


def start_container(client, db_id: str, cfg: dict) -> str:
    """Start a container, return its name. Raises on failure."""
    docker_cfg = cfg.get("docker", {})
    name = docker_cfg.get("container_name", f"showdown-{db_id}")
    image = docker_cfg.get("image")
    build_path = docker_cfg.get("build")

    # Already running?
    try:
        c = client.containers.get(name)
        if c.status == "running":
            return name
        c.start()
        return name
    except docker.errors.NotFound:
        pass

    if build_path and not image:
        # Build image from Dockerfile
        abs_build_path = str(PROJECT_ROOT / build_path)
        image = f"showdown-{db_id}:latest"
        print(f"       Building {image}...")
        client.images.build(path=abs_build_path, tag=image, rm=True)
    elif image:
        # Pull if needed
        try:
            client.images.get(image)
        except docker.errors.ImageNotFound:
            print(f"       Pulling {image}...")
            client.images.pull(image)
    else:
        raise ValueError(f"No image or build for {db_id}")

    kwargs = {
        "name": name,
        "image": image,
        "detach": True,
        "network": DOCKER_NETWORK,
    }

    env = docker_cfg.get("environment", {})
    if env:
        kwargs["environment"] = env

    ports = docker_cfg.get("ports", {})
    if ports:
        kwargs["ports"] = {k: v for k, v in ports.items()}

    volumes = docker_cfg.get("volumes", {})
    if volumes:
        vol_binds = {}
        for vol_name, mount_path in volumes.items():
            try:
                client.volumes.get(vol_name)
            except docker.errors.NotFound:
                client.volumes.create(vol_name)
            vol_binds[vol_name] = {"bind": mount_path, "mode": "rw"}
        kwargs["volumes"] = vol_binds

    hc = docker_cfg.get("healthcheck")
    if hc:
        kwargs["healthcheck"] = {
            "test": hc["test"],
            "interval": int(hc.get("interval", 5)) * 10**9,
            "timeout": int(hc.get("timeout", 5)) * 10**9,
            "retries": int(hc.get("retries", 20)),
        }

    ulimits = docker_cfg.get("ulimits", {})
    if ulimits:
        kwargs["ulimits"] = [
            docker.types.Ulimit(name=k, soft=v, hard=v)
            for k, v in ulimits.items()
        ]

    command = docker_cfg.get("command")
    if command:
        kwargs["command"] = command

    container = client.containers.create(**kwargs)
    container.start()
    return name


def wait_healthy(client, name: str, docker_cfg: dict, max_wait: int = 120) -> bool:
    """Wait for container health. Returns True if healthy."""
    hc = docker_cfg.get("healthcheck")
    if not hc:
        time.sleep(3)
        return True

    interval = int(hc.get("interval", 5))
    start = time.time()
    while time.time() - start < max_wait:
        try:
            c = client.containers.get(name)
            c.reload()
            health = c.attrs.get("State", {}).get("Health", {}).get("Status")
            if health == "healthy":
                return True
            if c.status != "running":
                return False
        except docker.errors.NotFound:
            return False
        time.sleep(interval)
    return False


def stop_container(client, name: str):
    try:
        c = client.containers.get(name)
        c.stop(timeout=10)
        c.remove()
    except docker.errors.NotFound:
        pass


# ---------------------------------------------------------------------------
# Connection tests
# ---------------------------------------------------------------------------

def tcp_reachable(host: str, port: int, timeout: float = 5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, ConnectionRefusedError):
        return False


async def test_http(port: int, cfg: dict) -> bool:
    import httpx
    conn = cfg["connection"]
    # POST with SQL as body — works for ClickHouse, DuckDB, SQLite HTTP servers
    params = {}
    if conn.get("user"):
        params["user"] = conn["user"]
    if conn.get("password"):
        params["password"] = conn["password"]
    r = httpx.post(
        f"http://localhost:{port}/",
        params=params,
        content="SELECT 1",
        timeout=10,
    )
    assert r.status_code == 200 and r.text.strip() == "1"
    return True


async def test_asyncpg(port: int, cfg: dict) -> bool:
    import asyncpg
    conn = cfg["connection"]
    c = await asyncpg.connect(
        host="localhost", port=port,
        user=conn["user"], password=conn["password"], database=conn["database"],
    )
    try:
        val = await c.fetchval("SELECT 1")
        assert val == 1
    finally:
        await c.close()
    return True


async def test_aiomysql(port: int, cfg: dict) -> bool:
    import aiomysql
    conn = cfg["connection"]
    c = await aiomysql.connect(
        host="localhost", port=port,
        user=conn["user"], password=conn["password"], db=conn["database"],
    )
    try:
        async with c.cursor() as cur:
            await cur.execute("SELECT 1")
            row = await cur.fetchone()
            assert row[0] == 1
    finally:
        await c.ensure_closed()
    return True


async def test_monetdb(port: int, cfg: dict) -> bool:
    import pymonetdb
    conn = cfg["connection"]
    c = pymonetdb.connect(
        hostname="localhost", port=port,
        username=conn["user"], password=conn["password"], database=conn["database"],
    )
    try:
        cur = c.cursor()
        cur.execute("SELECT 1")
        row = cur.fetchone()
        assert row[0] == 1
        cur.close()
    finally:
        c.close()
    return True


PROTOCOL_TESTS = {
    "http": test_http,
    "asyncpg": test_asyncpg,
    "aiomysql": test_aiomysql,
    "monetdb": test_monetdb,
}


# ---------------------------------------------------------------------------
# Core services check
# ---------------------------------------------------------------------------

def check_core_services() -> bool:
    import httpx

    header("Core Services")
    all_ok = True

    # Backend
    try:
        r = httpx.get(f"{BACKEND_URL}/status", timeout=5)
        if r.status_code == 200:
            ok("Backend API (localhost:8000)")
        else:
            fail(f"Backend API returned {r.status_code}")
            all_ok = False
    except Exception as e:
        fail(f"Backend API: {e}")
        all_ok = False

    # Dashboard
    try:
        r = httpx.get(DASHBOARD_URL, timeout=5)
        if r.status_code == 200:
            ok("Dashboard (localhost:3000)")
        else:
            fail(f"Dashboard returned {r.status_code}")
            all_ok = False
    except Exception as e:
        fail(f"Dashboard: {e}")
        all_ok = False

    return all_ok


# ---------------------------------------------------------------------------
# Backend loader compatibility
# ---------------------------------------------------------------------------

def check_backend_loaders(client, configs: dict) -> bool:
    """Verify that loader.py files import cleanly inside the backend container."""
    header("Backend Loader Compatibility")
    all_ok = True

    try:
        backend = client.containers.get("showdown-backend")
        if backend.status != "running":
            skip("Backend container not running — skipping loader checks")
            return True
    except docker.errors.NotFound:
        skip("Backend container not found — skipping loader checks")
        return True

    for db_id in sorted(configs.keys()):
        # Test 1: loader.py imports without error
        code = (
            "import importlib.util; "
            f"spec = importlib.util.spec_from_file_location('loader_{db_id}', '/app/databases/{db_id}/loader.py'); "
            "mod = importlib.util.module_from_spec(spec); "
            "spec.loader.exec_module(mod); "
            "fn = getattr(mod, 'create_schema', None); "
            "assert fn and callable(fn), 'create_schema missing or not callable'; "
            "print('ok')"
        )
        result = backend.exec_run(["python3", "-c", code], demux=True)
        stdout = (result.output[0] or b"").decode().strip()
        stderr = (result.output[1] or b"").decode().strip()

        if result.exit_code == 0 and stdout == "ok":
            ok(f"{db_id}: loader.py imports + create_schema callable")
        else:
            error_msg = stderr or stdout
            # Extract the key error line
            for line in error_msg.splitlines():
                if "ModuleNotFoundError" in line or "No such file" in line or "Error" in line:
                    error_msg = line.strip()
                    break
            fail(f"{db_id}: {error_msg}")
            all_ok = False

    return all_ok


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

def check_configs(configs: dict) -> bool:
    """Validate config.yaml files for common mistakes."""
    header("Config Validation")
    all_ok = True

    # Collect all host ports to detect conflicts
    host_port_map: dict[int, list[str]] = {}

    for db_id, cfg in sorted(configs.items()):
        # Required fields
        for field in ("id", "display_name", "connection"):
            if field not in cfg:
                fail(f"{db_id}: missing '{field}' in config.yaml")
                all_ok = False

        conn = cfg.get("connection", {})
        if "protocol" not in conn:
            fail(f"{db_id}: missing connection.protocol")
            all_ok = False

        docker_cfg = cfg.get("docker", {})
        if not docker_cfg.get("image") and not docker_cfg.get("build"):
            fail(f"{db_id}: missing docker.image or docker.build")
            all_ok = False

        # Port mapping validation
        ports = docker_cfg.get("ports", {})
        container_port = int(conn.get("port", 0))
        if ports and container_port:
            port_keys = [int(k) for k in ports.keys()]
            if container_port not in port_keys:
                fail(f"{db_id}: connection.port={container_port} not in docker.ports keys {port_keys} "
                     f"(Docker SDK format: {{container_port: host_port}})")
                all_ok = False

            for v in ports.values():
                hp = int(v)
                host_port_map.setdefault(hp, []).append(db_id)

        # Credential match
        env = docker_cfg.get("environment", {})
        password = conn.get("password", "")
        pw_env_keys = [k for k in env if "PASS" in k.upper()]
        if pw_env_keys and password:
            env_passwords = [str(env[k]) for k in pw_env_keys]
            if password not in env_passwords:
                fail(f"{db_id}: connection.password doesn't match env vars "
                     f"{[(k, env[k]) for k in pw_env_keys]}")
                all_ok = False

        # Schema file exists
        schema_path = Path(cfg.get("_schema_path", ""))
        if not schema_path.exists():
            fail(f"{db_id}: missing schema.sql")
            all_ok = False

    # Host port conflicts
    for hp, db_ids in host_port_map.items():
        if len(db_ids) > 1:
            fail(f"Host port {hp} claimed by multiple databases: {', '.join(db_ids)}")
            all_ok = False

    if all_ok:
        ok(f"All {len(configs)} configs valid (ports, credentials, schemas)")

    return all_ok


# ---------------------------------------------------------------------------
# Loader container compatibility
# ---------------------------------------------------------------------------

def check_loader_loaders(client, configs: dict) -> bool:
    """Verify that loader.py files import fully (with pandas etc.) inside the loader container."""
    header("Loader Container Compatibility")
    all_ok = True

    try:
        loader = client.containers.get("showdown-loader")
        if loader.status != "running":
            skip("Loader container not running — skipping")
            return True
    except docker.errors.NotFound:
        skip("Loader container not found — skipping")
        return True

    for db_id in sorted(configs.keys()):
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
        result = loader.exec_run(["python3", "-c", code], demux=True)
        stdout = (result.output[0] or b"").decode().strip()
        stderr = (result.output[1] or b"").decode().strip()

        if result.exit_code == 0 and stdout == "ok":
            ok(f"{db_id}: full loader import (load + create_schema)")
        else:
            error_msg = stderr or stdout
            for line in error_msg.splitlines():
                if "ModuleNotFoundError" in line or "Error" in line:
                    error_msg = line.strip()
                    break
            fail(f"{db_id}: {error_msg}")
            all_ok = False

    return all_ok


# ---------------------------------------------------------------------------
# Per-database test
# ---------------------------------------------------------------------------

async def test_database(client, db_id: str, cfg: dict, started_containers: list) -> bool:
    header(f"{cfg.get('display_name', db_id)} ({db_id})")

    docker_cfg = cfg.get("docker", {})
    protocol = cfg.get("connection", {}).get("protocol", "")
    all_ok = True

    # Step 1: Start container
    print(f"       Starting container...", end="", flush=True)
    try:
        name = start_container(client, db_id, cfg)
        started_containers.append(name)
        print(f" {name}")
        ok("Container started")
    except Exception as e:
        print()
        fail(f"Container start failed: {e}")
        return False

    # Step 2: Wait for healthy
    print(f"       Waiting for healthy...", end="", flush=True)
    healthy = wait_healthy(client, name, docker_cfg)
    print()
    if healthy:
        ok("Container healthy")
    else:
        fail("Container did not become healthy within timeout")
        return False

    # Brief settle time — some DBs (MySQL) finish init after healthcheck passes
    time.sleep(5)

    # Step 3: TCP reachable from host
    host_port = get_host_port(cfg)
    if host_port:
        # Give port mapping a moment to settle
        reachable = False
        for _ in range(5):
            if tcp_reachable("localhost", host_port):
                reachable = True
                break
            time.sleep(1)
        if reachable:
            ok(f"TCP reachable on localhost:{host_port}")
        else:
            fail(f"TCP not reachable on localhost:{host_port}")
            all_ok = False
    else:
        skip("No host port mapping found")
        return all_ok

    # Step 4: SELECT 1
    test_fn = PROTOCOL_TESTS.get(protocol)
    if test_fn:
        try:
            await test_fn(host_port, cfg)
            ok(f"SELECT 1 via {protocol}")
        except Exception as e:
            fail(f"SELECT 1 via {protocol}: {e}")
            all_ok = False
    else:
        skip(f"No connection test for protocol '{protocol}'")

    return all_ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(description="Smoke test all database containers")
    parser.add_argument("databases", nargs="*", help="Specific databases to test (default: all)")
    parser.add_argument("--teardown", action="store_true", help="Stop containers after testing")
    args = parser.parse_args()

    all_configs = load_all_configs()

    if args.databases:
        for db_id in args.databases:
            if db_id not in all_configs:
                print(f"{RED}Unknown database: {db_id}{RESET}")
                print(f"Available: {', '.join(all_configs.keys())}")
                sys.exit(1)
        configs = {k: v for k, v in all_configs.items() if k in args.databases}
    else:
        configs = all_configs

    print(f"{BOLD}ClickBench Showdown — Smoke Test{RESET}")
    print(f"Testing {len(configs)} database(s): {', '.join(configs.keys())}")

    client = get_docker_client()
    started_containers = []
    results = {}

    # Core services
    core_ok = check_core_services()
    results["core_services"] = core_ok

    # Config validation
    config_ok = check_configs(configs)
    results["config_validation"] = config_ok

    # Backend loader compatibility
    loader_ok = check_backend_loaders(client, configs)
    results["backend_loader_compat"] = loader_ok

    # Loader container compatibility
    loader_full_ok = check_loader_loaders(client, configs)
    results["loader_full_compat"] = loader_full_ok

    # Each database
    for db_id, cfg in configs.items():
        passed = await test_database(client, db_id, cfg, started_containers)
        results[db_id] = passed

    # Teardown if requested
    if args.teardown and started_containers:
        header("Teardown")
        for name in started_containers:
            try:
                stop_container(client, name)
                ok(f"Stopped {name}")
            except Exception as e:
                fail(f"Failed to stop {name}: {e}")

    # Summary
    header("Summary")
    total = len(results)
    passed = sum(1 for v in results.values() if v)
    failed = total - passed

    for name, ok_status in results.items():
        status = f"{GREEN}PASS{RESET}" if ok_status else f"{RED}FAIL{RESET}"
        print(f"  {status}  {name}")

    print()
    if failed == 0:
        print(f"{GREEN}{BOLD}All {total} checks passed.{RESET}")
    else:
        print(f"{RED}{BOLD}{failed}/{total} checks failed.{RESET}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    asyncio.run(main())
