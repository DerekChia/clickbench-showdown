"""
Database registry — scans databases/ directory, loads configs, provides factory.
"""

import os
from pathlib import Path

import yaml

from db_runner import create_runner, DBRunner

# databases/ directory is mounted at /app/databases inside the container
DATABASES_DIR = os.environ.get("DATABASES_DIR", "/app/databases")


def _load_config(db_dir: str) -> dict | None:
    """Load config.yaml from a database plugin directory."""
    config_path = os.path.join(db_dir, "config.yaml")
    if not os.path.exists(config_path):
        return None
    with open(config_path) as f:
        return yaml.safe_load(f)


def scan_databases() -> dict[str, dict]:
    """
    Scan the databases/ directory and return a dict of {db_id: config}.
    Each config includes the full path to schema.sql and the db_dir.
    """
    registry = {}
    if not os.path.isdir(DATABASES_DIR):
        return registry

    for entry in sorted(os.listdir(DATABASES_DIR)):
        db_dir = os.path.join(DATABASES_DIR, entry)
        if not os.path.isdir(db_dir):
            continue
        config = _load_config(db_dir)
        if config is None:
            continue
        db_id = config.get("id", entry)
        config["_dir"] = db_dir
        config["_schema_path"] = os.path.join(db_dir, "schema.sql")
        registry[db_id] = config

    return registry


def get_runner(db_id: str, registry: dict[str, dict]) -> DBRunner:
    """Create a runner for the given db_id using its config from the registry.

    The runner gets a copy: handing it the registry's own dict meant anything
    that wrote to runner.config mutated shared global state seen by every other
    runner for that database.
    """
    config = registry.get(db_id)
    if config is None:
        raise ValueError(f"Unknown database: {db_id}")
    return create_runner(db_id, dict(config))


def list_databases(registry: dict[str, dict]) -> list[dict]:
    """Return a list of database info dicts for the API response."""
    return [
        {
            "id": config["id"],
            "display_name": config["display_name"],
            "color": config.get("color", "#888888"),
            "clickbench_repo_path": config.get("clickbench_repo_path", config["id"]),
        }
        for config in registry.values()
    ]
