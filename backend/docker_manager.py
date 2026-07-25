"""
Docker container lifecycle manager — start/stop database containers on demand.

Uses the Docker SDK to manage containers programmatically.
The Docker socket must be mounted into the backend container.
"""

import asyncio
import os
import time

import docker
import docker.types

DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "showdown-net")
CONTAINER_PREFIX = "showdown-"


def _get_client() -> docker.DockerClient:
    return docker.from_env()


def _container_name(db_id: str) -> str:
    return f"{CONTAINER_PREFIX}{db_id}"


async def ensure_running(db_id: str, config: dict) -> None:
    """
    Ensure the container for db_id is running and healthy.
    Creates the container from config if it doesn't exist.
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _ensure_running_sync, db_id, config)


def _ensure_network(client, container, name: str) -> None:
    """Make sure the container is on the compose network so other services can reach it."""
    container.reload()
    networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
    if DOCKER_NETWORK not in networks:
        try:
            net = client.networks.get(DOCKER_NETWORK)
            net.connect(container)
            print(f"[docker] Reconnected {name} to {DOCKER_NETWORK}", flush=True)
        except Exception as e:
            print(f"[docker] Failed to reconnect {name} to network: {e}", flush=True)


def _ensure_running_sync(db_id: str, config: dict) -> None:
    client = _get_client()
    docker_cfg = config.get("docker", {})
    name = docker_cfg.get("container_name", _container_name(db_id))

    # Check if container already exists
    try:
        container = client.containers.get(name)
        if container.status == "running":
            _ensure_network(client, container, name)
            return
        # Exists but not running — try to start it
        try:
            container.start()
            _ensure_network(client, container, name)
            _wait_healthy(client, name, docker_cfg)
            return
        except Exception as e:
            # Start failed (e.g. old network gone) — remove and recreate
            print(f"[docker] Removing stale container {name}: {e}", flush=True)
            try:
                container.remove(force=True)
            except Exception:
                pass
    except docker.errors.NotFound:
        pass

    # Create and start
    build_path = docker_cfg.get("build")
    image = docker_cfg.get("image")

    if build_path and not image:
        # Build image from Dockerfile
        import os
        # build_path is relative to project root, resolve from DATABASES_DIR parent
        databases_dir = os.environ.get("DATABASES_DIR", "/app/databases")
        project_root = os.path.dirname(databases_dir.rstrip("/"))
        abs_build_path = os.path.join(project_root, build_path)
        image_tag = f"showdown-{db_id}:latest"
        print(f"[docker] Building image {image_tag} from {build_path}...", flush=True)
        client.images.build(path=abs_build_path, tag=image_tag, rm=True)
        image = image_tag
    elif image:
        # Pull image if needed
        try:
            client.images.get(image)
        except docker.errors.ImageNotFound:
            print(f"[docker] Pulling {image}...", flush=True)
            client.images.pull(image)
    else:
        raise ValueError(f"No image or build path for {db_id}")

    # Build container kwargs
    kwargs = {
        "name": name,
        "image": image,
        "detach": True,
        "network": DOCKER_NETWORK,
    }

    # Environment
    env = docker_cfg.get("environment", {})
    if env:
        kwargs["environment"] = env

    # Ports
    ports = docker_cfg.get("ports", {})
    if ports:
        kwargs["ports"] = {k: v for k, v in ports.items()}

    # Volumes (named volumes)
    volumes = docker_cfg.get("volumes", {})
    if volumes:
        vol_binds = {}
        for vol_name, mount_path in volumes.items():
            # Ensure named volume exists
            try:
                client.volumes.get(vol_name)
            except docker.errors.NotFound:
                client.volumes.create(vol_name)
            vol_binds[vol_name] = {"bind": mount_path, "mode": "rw"}
        kwargs["volumes"] = vol_binds

    # Healthcheck
    hc = docker_cfg.get("healthcheck")
    if hc:
        kwargs["healthcheck"] = {
            "test": hc["test"],
            "interval": int(hc.get("interval", 5)) * 10**9,
            "timeout": int(hc.get("timeout", 5)) * 10**9,
            "retries": int(hc.get("retries", 20)),
        }

    # Ulimits
    ulimits = docker_cfg.get("ulimits", {})
    if ulimits:
        kwargs["ulimits"] = [
            docker.types.Ulimit(name=k, soft=v, hard=v)
            for k, v in ulimits.items()
        ]

    # Command
    command = docker_cfg.get("command")
    if command:
        kwargs["command"] = command

    print(f"[docker] Creating container {name} from {image}...", flush=True)
    container = client.containers.create(**kwargs)
    container.start()

    _wait_healthy(client, name, docker_cfg)
    print(f"[docker] Container {name} is ready.", flush=True)


def _wait_healthy(client, name: str, docker_cfg: dict, max_wait: int = 120) -> None:
    """Wait for container to become healthy."""
    hc = docker_cfg.get("healthcheck")
    if not hc:
        time.sleep(3)
        return

    start = time.time()
    interval = int(hc.get("interval", 5))
    while time.time() - start < max_wait:
        try:
            container = client.containers.get(name)
            health = container.attrs.get("State", {}).get("Health", {}).get("Status")
            if health == "healthy":
                return
            if container.status != "running":
                raise RuntimeError(f"Container {name} stopped unexpectedly")
        except docker.errors.NotFound:
            raise RuntimeError(f"Container {name} disappeared")
        time.sleep(interval)

    raise TimeoutError(f"Container {name} did not become healthy within {max_wait}s")


async def stop_db(db_id: str, config: dict) -> None:
    """Stop and remove the container for a database."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _stop_db_sync, db_id, config)


def _stop_db_sync(db_id: str, config: dict) -> None:
    client = _get_client()
    docker_cfg = config.get("docker", {})
    name = docker_cfg.get("container_name", _container_name(db_id))

    try:
        container = client.containers.get(name)
        print(f"[docker] Stopping container {name}...", flush=True)
        container.stop(timeout=10)
        container.remove()
        print(f"[docker] Container {name} removed.", flush=True)
    except docker.errors.NotFound:
        pass


async def is_running(db_id: str, config: dict) -> bool:
    """Check if a database container is running."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _is_running_sync, db_id, config)


def _is_running_sync(db_id: str, config: dict) -> bool:
    client = _get_client()
    docker_cfg = config.get("docker", {})
    name = docker_cfg.get("container_name", _container_name(db_id))
    try:
        container = client.containers.get(name)
        return container.status == "running"
    except docker.errors.NotFound:
        return False
