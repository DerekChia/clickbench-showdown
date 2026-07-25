#!/usr/bin/env bash
set -euo pipefail

PARQUET_BASE_URL="https://datasets.clickhouse.com/hits_compatible/athena_partitioned"
PARQUET_DIR="/tmp/hits_parquet"
PARQUET_FILES="${PARQUET_FILES:-1}"
PARTS=$(seq 0 $((PARQUET_FILES - 1)))

log() { echo "[loader] $(date '+%H:%M:%S') $*"; }

# ── Phase 1: Download all parquet files in parallel ───────────────────────────
mkdir -p "$PARQUET_DIR"
log "Downloading ${PARQUET_FILES} parquet file(s) in parallel..."
DL_START=$(date +%s)
for i in $PARTS; do
    FILE="$PARQUET_DIR/hits_${i}.parquet"
    if [ -f "$FILE" ]; then
        log "  hits_${i}.parquet already on disk - skipping."
    else
        (
            log "  Downloading hits_${i}.parquet..."
            curl -sf --retry 5 --retry-delay 3 --retry-max-time 300 \
                -o "$FILE" \
                "${PARQUET_BASE_URL}/hits_${i}.parquet"
            log "  hits_${i}.parquet done."
        ) &
    fi
done
wait
log "Download phase done in $(($(date +%s) - DL_START))s."

log "All parquet files ready. Starting loader API on port 5000..."

# ── Keep the container alive and serve the reload API ─────────────────────────
exec python3 /loader_api.py
