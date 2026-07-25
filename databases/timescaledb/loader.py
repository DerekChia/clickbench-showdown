"""TimescaleDB data loader — reuses PostgreSQL loader logic (same wire protocol)."""

from databases.postgresql.loader import load, truncate, create_schema  # noqa: F401
