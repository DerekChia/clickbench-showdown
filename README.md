# ClickBench Showdown

A live benchmarking dashboard that compares **any two OLAP databases** using the industry-standard [ClickBench](https://clickbench.org) benchmark — 43 analytical queries run continuously against the `hits` web analytics dataset.

Pick any pair from **8 supported databases**, and the system handles container lifecycle, schema creation, data loading, and live performance tracking — all from a single-page dashboard.

Built for SMU IS459 · Big Data Management.

---

## Supported Databases

| Database | Version | Protocol | Container |
|---|---|---|---|
| ClickHouse | 26.3 | HTTP | `showdown-clickhouse` |
| PostgreSQL | 17 | asyncpg | `showdown-postgresql` |
| TimescaleDB | latest (PG 17) | asyncpg | `showdown-timescaledb` |
| MySQL | 8.0 | aiomysql | `showdown-mysql` |
| MariaDB | 11 | aiomysql | `showdown-mariadb` |
| MonetDB | latest | pymonetdb | `showdown-monetdb` |
| DuckDB | 1.1 | HTTP (custom) | `showdown-duckdb` |
| SQLite | 3.x | HTTP (custom) | `showdown-sqlite` |

Every database runs in its own Docker container. DuckDB and SQLite use lightweight Python HTTP SQL servers built from `databases/<id>/docker/Dockerfile`; those servers hold a single persistent connection so measured queries don't pay cold-cache cost that the server-based databases never pay.

TimescaleDB creates `hits` as a time-partitioned hypertable (1-day chunks on `EventTime`) — without that it would be byte-for-byte identical to PostgreSQL and the comparison would be meaningless.

Adding a new database is as simple as creating a `databases/<id>/` directory with three files — see [Adding a New Database](#adding-a-new-database).

---

## What it does

- Select any two databases from the dashboard dropdowns, choose the number of parquet files (1–10, each ~1M rows), and click **Apply**
- Containers are started on demand, schemas created, existing data truncated, and fresh data loaded automatically
- Runs all 43 ClickBench queries sequentially — Database A pass, then Database B pass, then repeat
- Displays live p50 / p90 / p99 latencies per query, speedup ratios, and timeout counts
- Shows a status bar with all databases and their current row counts
- Click any query row to expand the full SQL, per-query performance charts, and one-liner `docker exec` commands to run the query manually
- Changing the file count and clicking **Apply** again truncates and reloads both databases with the new dataset size

---

## Prerequisites

| Requirement | Notes |
|---|---|
| [Docker Desktop](https://docs.docker.com/get-docker/) | v24 or later |
| macOS / Linux | Use `showdown.sh` |
| Windows | Docker Desktop with **WSL 2 backend** enabled · Use `showdown.ps1` |
| Memory | 8 GB RAM recommended (4 GB minimum) |
| Disk | ~3 GB free for dataset + database storage |

---

## Quick start

### macOS / Linux

```bash
# Clone and enter the project
git clone <repo-url>
cd clickbench-showdown

# Make the script executable (first time only)
chmod +x showdown.sh

# Start everything — builds images, downloads 1 parquet file (~1M rows)
./showdown.sh start
```

### Windows (PowerShell)

```powershell
# Allow local scripts to run (first time only)
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

# Start everything
.\showdown.ps1 start
```

Once the services are up, open the dashboard:

```
http://localhost:3000
```

1. Select two databases from the dropdowns and choose the file count (1 = ~1M rows, 10 = ~10M rows)
2. Click **Apply** — containers start, schemas are created, data is loaded automatically
3. Once both databases show row counts in the status bar, click **Start Benchmark**

---

## Commands

```
./showdown.sh <command> [options]
```

| Command | Description |
|---|---|
| `start [--files N] [--workers N]` | Build images, start core services, tail loader logs |
| `stop` | Stop all services (data and cache preserved) |
| `restart` | Stop then start |
| `status` | Show container status |
| `logs [service]` | Tail logs — omit service to follow all |
| `reset [-y]` | Stop containers and delete all database volumes. Cached files in `tmp/` are preserved. |
| `help` | Show usage |

### Options for `start`

| Flag | Default | Description |
|---|---|---|
| `--files N` | `1` | Number of parquet files to load (1–10). Each file ~ 1M rows. |
| `--workers N` | auto | Parallel insert workers for PostgreSQL/TimescaleDB loading. Auto-detects based on available memory. |

```bash
# Load 5 files (~5M rows) with 4 insert workers
./showdown.sh start --files 5 --workers 4
```

---

## Data loading

Every time you click **Apply** on the dashboard, the system:

1. Starts the selected database containers (if not already running)
2. Creates the `hits` table schema in each database
3. Downloads parquet files from the ClickBench CDN (if not cached in `./tmp/`)
4. Truncates any existing data and bulk-loads database A, waiting for it to finish
5. Repeats for database B

The file count dropdown in the header controls how many parquet files are loaded (1–10, each ~1M rows). Changing the count and clicking **Apply** always reloads from scratch.

The **Start** button stays disabled until the loader reports it has *finished* — not merely until rows appear. Row counts climb throughout a load, and the PostgreSQL loader ends with `VACUUM ANALYZE`, so starting a benchmark on a non-zero row count would time queries against a database still doing heavy write work. If a load fails, the error is shown in the banner rather than leaving the dashboard stuck on "loading…".

**Caching** — Parquet downloads and some converted formats are cached in `./tmp/`:

| Database | Cached file | Format |
|---|---|---|
| ClickHouse | (none — loads parquet natively over HTTP) | — |
| DuckDB | (none — reads parquet inside the container) | — |
| PostgreSQL / TimescaleDB | (none — streams parquet straight into `COPY`) | — |
| MySQL / MariaDB | `hits_N.tsv` | Tab-separated |
| MonetDB | `hits_N_monetdb.csv` | Pipe-delimited |
| SQLite | (none — streams parquet into batched `INSERT`s) | — |

Converted files are written to a `.part` file and renamed only on success, so an interrupted run can never leave a truncated file that later runs treat as cached. `reset` preserves the cache; only deleting `./tmp/` manually clears it. New database loaders should follow this pattern — write converted files to `parquet_dir`.

**Queries** are fetched from a pinned ClickBench commit (see `CLICKBENCH_COMMIT` in `backend/query_fetcher.py`) and cached for 24 h, so upstream edits can't change your numbers between runs. Set `CLICKBENCH_COMMIT=main` to track upstream instead.

---

## Resetting

```bash
./showdown.sh reset        # prompts for confirmation
./showdown.sh reset -y     # skips confirmation
```

This stops all containers and removes database volumes. Every volume this project creates is prefixed `showdown-`, so `reset` can never delete another project's data. Parquet files and converted cache files in `./tmp/` are **preserved** so the next start doesn't need to re-download or re-convert.

---

## Services

| Service | URL | Description |
|---|---|---|
| Dashboard | http://localhost:3000 | Live benchmark UI with database picker |
| Backend API | http://localhost:8000 | FastAPI — benchmark runner, DB management, status API |
| Loader | (internal :5000) | Parquet download + per-DB data loading |

Database containers are started/stopped on demand and are not always running.

---

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/databases` | GET | List all databases with status, row counts |
| `/select?db_a=X&db_b=Y&files=N` | POST | Select 2 DBs, start containers, truncate and load N files |
| `/status` | GET | Full benchmark state (selected DBs, loader, query stats) |
| `/queries` | GET | SQL for both selected DBs |
| `/benchmark/start` | POST | Start benchmark loop |
| `/benchmark/stop` | POST | Stop benchmark |
| `/loader/reload?files=N` | POST | Re-download parquet files |

---

## Running queries manually

After selecting databases from the dashboard, you can query them directly. The dashboard also shows **copy-paste `docker exec` commands** when you click any query row to expand it.

**ClickHouse**
```bash
docker exec showdown-clickhouse clickhouse-client --password bench_pass -q 'SELECT count() FROM hits'
```

**PostgreSQL / TimescaleDB**
```bash
docker exec showdown-postgresql psql -U bench_user -d hits -c 'SELECT count(*) FROM hits'
```

**MySQL**
```bash
docker exec showdown-mysql mysql -ubench_user -pbench_pass hits -e 'SELECT count(*) FROM hits'
```

**MariaDB**
```bash
docker exec showdown-mariadb mariadb -ubench_user -pbench_pass hits -e 'SELECT count(*) FROM hits'
```

**MonetDB**
```bash
docker exec showdown-monetdb bash -c "echo -e 'user=monetdb\npassword=bench_pass' > /tmp/.monetdb && DOTMONETDBFILE=/tmp/.monetdb mclient -d monetdb -s \"SELECT count(*) FROM hits\""
```

**DuckDB**
```bash
curl -s -X POST http://localhost:9999/ -d 'SELECT count(*) FROM hits'
```

**SQLite**
```bash
curl -s -X POST http://localhost:9998/ -d 'SELECT count(*) FROM hits'
```

---

## Architecture

```
showdown.sh / showdown.ps1
        │
        └── docker compose (3 always-on services)
              ├── backend       FastAPI — benchmark loop, DB management via Docker SDK
              ├── dashboard     nginx — single-page vanilla JS dashboard
              └── loader        Parquet download + per-DB data loading API

        + 8 database containers (started/stopped on demand)
              ├── showdown-clickhouse    (clickhouse/clickhouse-server:26.3)
              ├── showdown-postgresql    (postgres:17)
              ├── showdown-timescaledb   (timescale/timescaledb:latest-pg17)
              ├── showdown-mysql         (mysql:8.0)
              ├── showdown-mariadb       (mariadb:11)
              ├── showdown-monetdb       (monetdb/monetdb:latest)
              ├── showdown-duckdb        (custom — Python HTTP SQL server)
              └── showdown-sqlite        (custom — Python HTTP SQL server)
```

**Plugin system** — Each database is defined by a `databases/<id>/` directory containing `config.yaml`, `schema.sql`, and `loader.py`. The backend discovers databases at startup by scanning this directory.

**Benchmark loop** — Database A pass (43 queries) → Database B pass (43 queries) → repeat. The two databases are never queried concurrently to avoid resource contention.

**Percentiles** are computed over a rolling window of the last 200 query executions per query per database.

**Docker SDK** — The backend manages database containers programmatically (start, stop, health checks, network reconnection) via the Docker socket mounted into the container.

---

## Project structure

```
clickbench-showdown/
├── databases/                    Plugin system — one dir per supported DB
│   ├── clickhouse/
│   │   ├── config.yaml           Docker image, connection, healthcheck config
│   │   ├── schema.sql            DDL from ClickBench
│   │   └── loader.py             DB-specific bulk load logic
│   ├── duckdb/
│   │   ├── docker/               Custom container (Python HTTP SQL server)
│   │   │   ├── Dockerfile
│   │   │   └── server.py
│   │   ├── config.yaml
│   │   ├── schema.sql
│   │   └── loader.py
│   ├── sqlite/
│   │   ├── docker/               Custom container (Python HTTP SQL server)
│   │   │   ├── Dockerfile
│   │   │   └── server.py
│   │   ├── config.yaml
│   │   ├── schema.sql
│   │   └── loader.py
│   ├── postgresql/ mariadb/ mysql/ monetdb/ timescaledb/
│   │   └── config.yaml, schema.sql, loader.py
├── backend/
│   ├── main.py                   FastAPI app — benchmark loop, status, DB management
│   ├── db_runner.py              Abstract runner + concrete runners per protocol
│   ├── db_registry.py            Scans databases/, provides factory
│   ├── docker_manager.py         Start/stop DB containers via Docker SDK
│   ├── query_fetcher.py          Fetches queries from ClickBench GitHub repo
│   └── requirements.txt
├── dashboard/
│   └── index.html                Single-file SPA — DB picker, live charts
├── loader/
│   ├── load.sh                   Downloads parquet files on startup
│   ├── loader_api.py             HTTP API for on-demand data loading
│   └── Dockerfile
├── tests/
│   ├── test_containers.py        pytest — container health, loader compat, config validation
│   └── smoke_test.py             Standalone — spins up all DBs, verifies connectivity
├── tmp/                          Cache — parquet files + converted TSV/CSV
├── docker-compose.yml            3 always-on services (backend, dashboard, loader)
├── showdown.sh                   macOS / Linux launcher
└── showdown.ps1                  Windows PowerShell launcher
```

---

## Adding a New Database

1. Create `databases/<db_id>/config.yaml` with:
   - `docker.image` (for official images) or `docker.build` (for custom Dockerfiles in `databases/<db_id>/docker/`)
   - Connection protocol, ports, healthcheck, credentials
2. Create `databases/<db_id>/schema.sql` — `CREATE TABLE hits` DDL from [ClickBench](https://github.com/ClickHouse/ClickBench)
3. Create `databases/<db_id>/loader.py` with three functions:
   - `create_schema(config, schema_path)` — execute the DDL (must work in both backend and loader containers)
   - `load(parquet_dir, num_files, config)` — truncate + bulk load data, return row count
   - `truncate(config)` — clear the table
4. If the connection protocol is new, add a runner class to `backend/db_runner.py`
5. Add any loader-only dependencies to `loader/Dockerfile`
6. The database will auto-appear in the dashboard picker on next restart
7. Run `python3 tests/smoke_test.py <db_id>` to verify everything works

---

## Testing

```bash
# Install test dependencies
pip3 install pytest pytest-asyncio httpx asyncpg aiomysql pymonetdb docker pyyaml

# Run pytest suite (requires backend + some containers running)
pytest tests/test_containers.py -v

# Run standalone smoke test (starts all containers automatically)
python3 tests/smoke_test.py

# Test specific databases only
python3 tests/smoke_test.py clickhouse mariadb

# Tear down containers after testing
python3 tests/smoke_test.py --teardown
```

The test suite validates:
- Core services (backend, dashboard, loader) respond correctly
- All `loader.py` files import cleanly in both the backend and loader containers
- Config files have correct port mappings, credentials, and required fields
- Database containers start, pass healthchecks, and accept queries
