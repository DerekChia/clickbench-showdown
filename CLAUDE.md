# ClickBench Showdown

Multi-database benchmarking dashboard using the ClickBench suite (43 analytical queries on web analytics data). Supports pairwise comparison of any two databases from a pluggable registry.

## Architecture

3 always-on Docker services + N database containers managed dynamically:

| Service | Port | Tech |
|---------|------|------|
| **dashboard** | 3000 | nginx serving vanilla JS SPA (`dashboard/index.html`) |
| **backend** | 8000 | FastAPI (Python 3.11) — benchmark runner, status API, DB management |
| **loader** | 5000 | Python HTTP server — parquet download + per-DB data loading |
| **DB containers** | varies | Started/stopped on demand via Docker SDK |

## Project Structure

```
databases/                    # Plugin system — one dir per supported DB
  clickhouse/
    config.yaml               # Docker image, connection, healthcheck config
    schema.sql                # DDL from ClickBench
    loader.py                 # DB-specific bulk load logic
  postgresql/
    config.yaml, schema.sql, loader.py
  duckdb/ sqlite/
    config.yaml, schema.sql, loader.py
    docker/                   # Small HTTP SQL server + Dockerfile (built on demand)
  mysql/ mariadb/ monetdb/ timescaledb/
    ...same pattern...

backend/
  main.py                     # FastAPI: benchmark loop, /databases, /select, /status
  db_runner.py                # Abstract DBRunner + concrete runners (HTTP, asyncpg, aiomysql, monetdb)
  db_registry.py              # Scans databases/, provides factory
  query_fetcher.py            # Fetches queries.sql from a pinned ClickBench commit, 24h cache
  docker_manager.py           # Start/stop DB containers via Docker SDK
  requirements.txt, Dockerfile

loader/
  load.sh                     # Downloads parquet files only
  loader_api.py               # HTTP API: POST /load?db=<id>&files=N, POST /reload?files=N, GET /status
  Dockerfile

dashboard/
  index.html                  # Single-file SPA: DB picker dropdowns, dynamic labels/colors
  Dockerfile

docker-compose.yml            # Only backend, dashboard, loader (always-on)
showdown.sh / showdown.ps1    # CLI wrapper
```

## Key Design Decisions

- **Plugin system**: Adding a new DB = create `databases/<id>/` with config.yaml + schema.sql + loader.py
- **Pairwise comparison**: User picks 2 DBs from dashboard, containers started/stopped on demand
- **Queries from ClickBench repo**: Fetched from a **pinned commit** (`CLICKBENCH_COMMIT`), cached 24h
- **Sequential execution**: DB A pass then DB B pass to avoid resource contention
- **Sequential loading**: DB A then DB B, each awaited to completion — the loader
  holds a single global lock, and the backend must know when a load has actually
  *finished* before it can call the benchmark ready
- **Readiness ≠ row counts**: `loader.ready` requires the loader to report idle
  with no error. Row counts climb throughout a load and PostgreSQL finishes with
  `VACUUM ANALYZE`, so a non-zero count proves nothing about being done
- **Rolling 200-query window** for percentile calculations
- **Docker SDK** manages DB containers programmatically (socket mounted); the
  compose network is explicitly named `showdown-net` and all volumes are
  prefixed `showdown-` so nothing depends on the checkout directory's name
- **Single-file dashboard** — no build step, no Node.js. Query labels come from
  the backend (`GET /queries` → `labels_short`), not a second hardcoded copy

## Supported Databases

| Database | Protocol | Runner Class |
|----------|----------|-------------|
| ClickHouse | HTTP | HTTPRunner |
| PostgreSQL | asyncpg | AsyncpgRunner |
| DuckDB | HTTP (custom server) | HTTPRunner |
| MySQL | aiomysql | AioMySQLRunner |
| MariaDB | aiomysql | AioMySQLRunner |
| MonetDB | pymonetdb | MonetDBRunner |
| SQLite | HTTP (custom server) | HTTPRunner |
| TimescaleDB | asyncpg | AsyncpgRunner |

DuckDB and SQLite have no network protocol of their own, so each runs behind a
small Python HTTP SQL server (`databases/<id>/docker/server.py`). Those servers
keep **one persistent connection** — a fresh connection per query would make
every measured query pay cold-cache cost that server-based databases never pay —
and honour an `X-Query-Timeout` header by interrupting the query and answering
`504`, which `HTTPRunner` records as a timeout rather than an error.

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/databases` | GET | List all available databases with status |
| `/select?db_a=X&db_b=Y` | POST | Select 2 DBs, start containers, fetch queries |
| `/status` | GET | Full state (db_a, db_b, loader, running) |
| `/queries` | GET | SQL for both selected DBs |
| `/benchmark/start` | POST | Start benchmark loop |
| `/benchmark/stop` | POST | Stop benchmark |
| `/loader/reload?files=N` | POST | Re-download parquet files |

## Commands

```bash
./showdown.sh start [--files N] [--workers N]
./showdown.sh stop
./showdown.sh reset [-y]
./showdown.sh status
./showdown.sh logs [service]
```

## Adding a New Database

1. Create `databases/<db_id>/config.yaml` with Docker image, connection protocol, ports, healthcheck
2. Create `databases/<db_id>/schema.sql` from ClickBench's `<db>/create.sql`
3. Create `databases/<db_id>/loader.py` with `load()`, `truncate()`, `create_schema()` functions
4. If the connection protocol is new, add a runner class to `backend/db_runner.py`
5. The DB will auto-appear in the dashboard picker on next restart
