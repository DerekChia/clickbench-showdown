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
# Download to a .part file and rename only on success. Writing straight to the
# final path meant an interrupted download left a truncated file that every
# later run skipped as "already on disk", with no way to self-heal.
PIDS=""
for i in $PARTS; do
    FILE="$PARQUET_DIR/hits_${i}.parquet"
    if [ -f "$FILE" ]; then
        log "  hits_${i}.parquet already on disk - skipping."
    else
        (
            log "  Downloading hits_${i}.parquet..."
            if curl -sf --retry 5 --retry-delay 3 --retry-max-time 300 \
                    -o "${FILE}.part" \
                    "${PARQUET_BASE_URL}/hits_${i}.parquet"; then
                mv -f "${FILE}.part" "$FILE"
                log "  hits_${i}.parquet done."
            else
                rm -f "${FILE}.part"
                log "  hits_${i}.parquet FAILED."
                exit 1
            fi
        ) &
        PIDS="$PIDS $!"
    fi
done

# Bare `wait` returns 0 regardless of what the subshells did, so set -e never
# tripped on a failed download. Check each PID individually.
DL_FAILED=0
for pid in $PIDS; do
    wait "$pid" || DL_FAILED=1
done
if [ "$DL_FAILED" -ne 0 ]; then
    log "One or more parquet downloads failed. Fix connectivity and restart."
    exit 1
fi
log "Download phase done in $(($(date +%s) - DL_START))s."

log "All parquet files ready. Starting loader API on port 5000..."

# ── Keep the container alive and serve the reload API ─────────────────────────
exec python3 /loader_api.py
