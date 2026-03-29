#!/usr/bin/env bash
set -euo pipefail

CH_HOST="${CLICKHOUSE_HOST:-clickhouse}"
CH_PORT="${CLICKHOUSE_PORT:-8123}"
CH_USER="${CLICKHOUSE_USER:-default}"
CH_PASS="${CLICKHOUSE_PASSWORD:-bench_pass}"
CH_URL="http://${CH_HOST}:${CH_PORT}/"

PG_HOST="${POSTGRES_HOST:-postgres}"
PG_PORT="${POSTGRES_PORT:-5432}"
PG_USER="${POSTGRES_USER:-bench_user}"
PG_PASS="${POSTGRES_PASSWORD:-bench_pass}"
PG_DB="${POSTGRES_DB:-hits}"

PARQUET_BASE_URL="https://datasets.clickhouse.com/hits_compatible/athena_partitioned"
PARQUET_DIR="/tmp/hits_parquet"
PARQUET_FILES="${PARQUET_FILES:-5}"
PARTS=$(seq 0 $((PARQUET_FILES - 1)))

log() { echo "[loader] $(date '+%H:%M:%S') $*"; }

# ── Wait for ClickHouse ──────────────────────────────────────────────────────
log "Waiting for ClickHouse…"
until curl -sf "${CH_URL}ping" > /dev/null 2>&1; do sleep 3; done
log "ClickHouse is up."

# ── Wait for PostgreSQL ──────────────────────────────────────────────────────
log "Waiting for PostgreSQL…"
export PGPASSWORD="${PG_PASS}"
until psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" -c '\q' > /dev/null 2>&1; do sleep 3; done
log "PostgreSQL is up."

# ── Check existing data ───────────────────────────────────────────────────────
CH_ROWS=$(curl -sf "${CH_URL}?user=${CH_USER}&password=${CH_PASS}" \
    --data "SELECT count() FROM hits" 2>/dev/null | tr -d '[:space:]' || echo "0")

PG_ROWS=$(psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" -t \
    -c "SELECT count(*) FROM hits" 2>/dev/null | tr -d '[:space:]' || echo "0")

log "Current rows — ClickHouse: ${CH_ROWS:-0}, PostgreSQL: ${PG_ROWS:-0}"

if [ "${CH_ROWS:-0}" -gt 0 ] && [ "${PG_ROWS:-0}" -gt 0 ]; then
    if [ "${CH_ROWS}" -eq "${PG_ROWS}" ]; then
        log "Both databases already loaded with ${CH_ROWS} rows each. Nothing to do."
        exit 0
    fi
    log "WARNING: Row count mismatch — ClickHouse: ${CH_ROWS}, PostgreSQL: ${PG_ROWS}."
    if [ "${CH_ROWS}" -lt "${PG_ROWS}" ]; then
        log "Truncating ClickHouse (has fewer rows) and reloading…"
        curl -sf "${CH_URL}?user=${CH_USER}&password=${CH_PASS}" \
            --data "TRUNCATE TABLE hits"
        CH_ROWS=0
    else
        log "Truncating PostgreSQL (has fewer rows) and reloading…"
        psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" \
            -c "TRUNCATE TABLE hits" > /dev/null
        PG_ROWS=0
    fi
fi

# ── Phase 1: Download all parquet files in parallel ───────────────────────────
mkdir -p "$PARQUET_DIR"
log "Phase 1 — downloading ${PARQUET_FILES} parquet file(s) in parallel…"
DL_START=$(date +%s)
for i in $PARTS; do
    FILE="$PARQUET_DIR/hits_${i}.parquet"
    if [ -f "$FILE" ]; then
        log "  hits_${i}.parquet already on disk — skipping."
    else
        (
            log "  Downloading hits_${i}.parquet…"
            curl -sf --retry 5 --retry-delay 3 --retry-max-time 300 \
                -o "$FILE" \
                "${PARQUET_BASE_URL}/hits_${i}.parquet"
            log "  hits_${i}.parquet done."
        ) &
    fi
done
wait
log "Phase 1 done in $(($(date +%s) - DL_START))s."

# ── Phase 2a: Load ClickHouse from local parquet files ────────────────────────
if [ "${CH_ROWS:-0}" -eq 0 ]; then
    log "Phase 2a — loading ClickHouse from local parquet files…"
    CH_START=$(date +%s)
    for i in $PARTS; do
        log "  Inserting hits_${i}.parquet into ClickHouse…"
        curl -sf \
            "${CH_URL}?user=${CH_USER}&password=${CH_PASS}&max_execution_time=3600&input_format_parquet_case_insensitive_column_matching=1&query=INSERT%20INTO%20hits%20FORMAT%20Parquet" \
            --data-binary @"$PARQUET_DIR/hits_${i}.parquet"
    done
    CH_ELAPSED=$(($(date +%s) - CH_START))
    NEW_CH=$(curl -sf "${CH_URL}?user=${CH_USER}&password=${CH_PASS}" \
        --data "SELECT count() FROM hits" | tr -d '[:space:]')
    log "Phase 2a done — ClickHouse: ${NEW_CH} rows in ${CH_ELAPSED}s."
else
    log "ClickHouse already has ${CH_ROWS} rows — skipping."
fi

# ── Phase 2b: Load PostgreSQL from local parquet files ────────────────────────
if [ "${PG_ROWS:-0}" -eq 0 ]; then
    log "Phase 2b — loading PostgreSQL from local parquet files…"
    PG_START=$(date +%s)
    PARQUET_DIR="$PARQUET_DIR" python3 /load_pg.py
    PG_ELAPSED=$(($(date +%s) - PG_START))
    NEW_PG=$(psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" -t \
        -c "SELECT count(*) FROM hits" | tr -d '[:space:]')
    log "Phase 2b done — PostgreSQL: ${NEW_PG} rows in ${PG_ELAPSED}s."
else
    log "PostgreSQL already has ${PG_ROWS} rows — skipping."
fi

log "All data loaded. Benchmark is ready."

# ── Keep the container alive and serve the reload API ─────────────────────────
log "Starting loader API on port 5000…"
exec python3 /loader_api.py
