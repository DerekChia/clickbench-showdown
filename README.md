# OLAP Workload Database Showdown

A live benchmarking dashboard that compares **ClickHouse 26.3** against **PostgreSQL 17** using the industry-standard [ClickBench](https://clickbench.org) benchmark — 43 analytical queries run continuously against the `hits` web analytics dataset.

Built for SMU IS459 · Big Data Management.

---

## What it does

- Loads a slice of the ClickBench `hits` dataset (1–10 parquet files, ~1–10 M rows) into both databases
- Runs all 43 ClickBench queries in a loop — ClickHouse first, then PostgreSQL, then repeat
- Displays live p50 / p90 / p99 latencies per query, speedup ratios, and timeout counts
- Click any query row to expand the full SQL and per-query performance charts
- Detects row count mismatches between databases and blocks the benchmark until they are resolved
- Supports in-dashboard dataset reload — select file count from the header dropdown and click **↺ Load Data**

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
cd olap-database-showdown

# Make the script executable (first time only)
chmod +x showdown.sh

# Start everything — builds images, loads data, tails loader logs
./showdown.sh start
```

### Windows (PowerShell)

```powershell
# Allow local scripts to run (first time only)
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

# Start everything
.\showdown.ps1 start
```

Once the loader finishes, open the dashboard:

```
http://localhost:3001
```

Press **▶ Start Benchmark** to begin running queries.

---

## Data loading

On first start the loader:

1. Downloads parquet files from the ClickBench CDN into `./tmp/`
2. Loads ClickHouse via HTTP bulk insert
3. Loads PostgreSQL via `COPY FROM STDIN` (fastest bulk-load path)

Downloaded parquet files are cached in `./tmp/` and reused on subsequent restarts — no re-download on `stop`/`start`. Only `reset` clears the cache.

If the two databases end up with different row counts (e.g. due to an interrupted load), the loader automatically truncates and reloads the lagging database. The dashboard shows a red warning banner and keeps the Start Benchmark button disabled until both counts match.

**Reloading from the dashboard** — use the file count dropdown in the header and click **↺ Load Data** to truncate both databases and reload with a different number of files without restarting the stack.

| Database | Approx. load time (5 files, 8 GB RAM) |
|---|---|
| ClickHouse | ~1 min |
| PostgreSQL | ~5–10 min |

---

## Commands

```
./showdown.sh <command> [options]
```

| Command | Description |
|---|---|
| `start [--files N] [--workers N]` | Build images, start all services, tail loader logs |
| `stop` | Stop all services (data and parquet cache preserved) |
| `restart` | Stop then start |
| `status` | Show container status and live row counts |
| `logs [service]` | Tail logs — omit service to follow all |
| `reset [-y]` | Stop containers and **delete all data** (databases + parquet cache) |
| `help` | Show usage |

### Options for `start`

| Flag | Default | Description |
|---|---|---|
| `--files N` | `5` | Number of parquet files to load (1–10). Each file ≈ 1 M rows. |
| `--workers N` | auto | Parallel insert workers for PostgreSQL loading. Auto-detects based on available memory (2 if ≥ 8 GiB, else 1). |

```bash
# Load the full 10-file dataset (~10 M rows) with 4 insert workers
./showdown.sh start --files 10 --workers 4

# Windows equivalent
.\showdown.ps1 start -Files 10 -Workers 4
```

---

## Services

| Service | URL | Description |
|---|---|---|
| Dashboard | http://localhost:3001 | Live benchmark UI |
| Backend API | http://localhost:8000 | FastAPI — `/status`, `/benchmark/start`, `/benchmark/stop`, `/loader/reload` |
| ClickHouse | http://localhost:8123 | ClickHouse HTTP interface |
| PostgreSQL | localhost:5432 | User: `bench_user` · DB: `hits` · Password: `bench_pass` |

---

## Running queries manually

After the services are up you can query either database directly from your terminal.

**ClickHouse**
```bash
docker exec -it showdown-clickhouse \
  clickhouse-client --password bench_pass --query "SELECT count() FROM hits"
```

For an interactive session omit `--query`:
```bash
docker exec -it showdown-clickhouse clickhouse-client --password bench_pass
```

**PostgreSQL**
```bash
docker exec -it showdown-postgres \
  psql -U bench_user -d hits -c "SELECT count(*) FROM hits"
```

For an interactive session omit `-c`:
```bash
docker exec -it showdown-postgres psql -U bench_user -d hits
```

---

## Architecture

```
showdown.sh / showdown.ps1
        │
        └── docker compose
              ├── loader        Downloads parquets → loads CH → loads PG → serves reload API (:5000)
              ├── clickhouse    ClickHouse 26.3
              ├── postgres      PostgreSQL 17
              ├── backend       FastAPI — runs benchmark queries, proxies reload requests
              └── dashboard     nginx — serves single-page benchmark UI
```

**Benchmark loop** (backend): ClickHouse pass (43 queries) → PostgreSQL pass (43 queries) → repeat. The two databases are never queried concurrently to avoid resource contention.

**Percentiles** are computed over a rolling window of the last 200 query executions per query per database.

**Loader lifecycle**: the loader container stays running after the initial data load and listens on port 5000 for reload requests. The backend proxies `POST /loader/reload?files=N` from the dashboard to the loader, which truncates both databases and reloads the requested number of parquet files.

---

## Project structure

```
olap-database-showdown/
├── backend/
│   ├── main.py          FastAPI app — benchmark runner, status poller, reload proxy
│   └── queries.py       All 43 ClickBench queries (CH and PG variants)
├── clickhouse-init/
│   └── 01-schema.sql    ClickHouse hits table DDL
├── dashboard/
│   └── index.html       Single-file vanilla JS dashboard
├── loader/
│   ├── Dockerfile
│   ├── load.sh          Orchestrates download → CH load → PG load → starts API server
│   ├── load_pg.py       Python bulk loader (pyarrow + psycopg2 COPY)
│   └── loader_api.py    HTTP API server — handles on-demand reload requests
├── postgres-init/
│   └── 01-schema.sql    PostgreSQL hits table DDL
├── tmp/                 Parquet file cache — bind-mounted into the loader container
├── docker-compose.yml
├── showdown.sh          macOS / Linux launcher
└── showdown.ps1         Windows PowerShell launcher
```

---

## Resetting

To wipe all data and start fresh:

```bash
./showdown.sh reset        # prompts for confirmation
./showdown.sh reset -y     # skips confirmation

# Windows
.\showdown.ps1 reset -y
```

This removes all Docker volumes (ClickHouse data, PostgreSQL data) and clears the `./tmp/` parquet cache. The next `start` will re-download and reload everything from scratch.
