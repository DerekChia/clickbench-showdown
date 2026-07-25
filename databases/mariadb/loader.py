"""MariaDB data loader — reuses MySQL loader logic (same wire protocol)."""

from databases.mysql.loader import load, truncate, create_schema  # noqa: F401
