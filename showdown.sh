#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Colours ───────────────────────────────────────────────────────────────────
TEAL='\033[38;5;80m'
BLUE='\033[38;5;75m'
RED='\033[38;5;203m'
AMBER='\033[38;5;215m'
GREEN='\033[38;5;114m'
MU='\033[38;5;67m'
BOLD='\033[1m'
RESET='\033[0m'

banner() {
  echo ""
  echo -e "${TEAL}${BOLD}  ██████╗ ██╗      █████╗ ██████╗     ███████╗██╗  ██╗ ██████╗ ██╗    ██╗██████╗  ██████╗ ██╗    ██╗███╗   ██╗${RESET}"
  echo -e "${BLUE}  ██╔═══██╗██║     ██╔══██╗██╔══██╗    ██╔════╝██║  ██║██╔═══██╗██║    ██║██╔══██╗██╔═══██╗██║    ██║████╗  ██║${RESET}"
  echo -e "${TEAL}  ██║   ██║██║     ███████║██████╔╝    ███████╗███████║██║   ██║██║ █╗ ██║██║  ██║██║   ██║██║ █╗ ██║██╔██╗ ██║${RESET}"
  echo -e "${BLUE}  ██║   ██║██║     ██╔══██║██╔═══╝     ╚════██║██╔══██║██║   ██║██║███╗██║██║  ██║██║   ██║██║███╗██║██║╚██╗██║${RESET}"
  echo -e "${TEAL}  ╚██████╔╝███████╗██║  ██║██║         ███████║██║  ██║╚██████╔╝╚███╔███╔╝██████╔╝╚██████╔╝╚███╔███╔╝██║ ╚████║${RESET}"
  echo -e "${BLUE}   ╚═════╝ ╚══════╝╚═╝  ╚═╝╚═╝         ╚══════╝╚═╝  ╚═╝ ╚═════╝  ╚══╝╚══╝ ╚═════╝  ╚═════╝  ╚══╝╚══╝ ╚═╝  ╚═══╝${RESET}"
  echo ""
  echo -e "  ${MU}ClickBench · 43 queries · ClickHouse 26.3 vs PostgreSQL 17 · IS459${RESET}"
  echo ""
}

log()  { echo -e "  ${GREEN}▶${RESET}  $*"; }
info() { echo -e "  ${BLUE}ℹ${RESET}  $*"; }
warn() { echo -e "  ${AMBER}⚠${RESET}  $*"; }
err()  { echo -e "  ${RED}✖${RESET}  $*"; }
hr()   { echo -e "  ${MU}────────────────────────────────────────────────────${RESET}"; }

# ── Checks ────────────────────────────────────────────────────────────────────
check_docker() {
  if ! command -v docker &>/dev/null; then
    err "Docker is not installed. Get it at https://docs.docker.com/get-docker/"
    exit 1
  fi
  if ! docker info &>/dev/null; then
    err "Docker daemon is not running. Please start Docker Desktop."
    exit 1
  fi
}

# ── Commands ──────────────────────────────────────────────────────────────────
cmd_start() {
  local workers=""
  local files=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --workers|-w) workers="$2"; shift 2 ;;
      --files|-f)   files="$2";   shift 2 ;;
      *) shift ;;
    esac
  done

  if [[ -n "$workers" ]]; then
    if ! [[ "$workers" =~ ^[1-9][0-9]*$ ]]; then
      err "--workers must be a positive integer (got: $workers)"
      exit 1
    fi
    export INSERT_WORKERS="$workers"
    log "Insert workers overridden to ${workers}."
  fi

  if [[ -n "$files" ]]; then
    if ! [[ "$files" =~ ^([1-9]|10)$ ]]; then
      err "--files must be between 1 and 10 (got: $files)"
      exit 1
    fi
    export PARQUET_FILES="$files"
    log "Parquet files set to ${files} (~$((files * 1000000)) rows)."
  fi

  banner
  check_docker
  log "Building images and starting all services…"
  hr
  cd "$DIR"
  $COMPOSE up --build -d
  hr
  echo ""
  log "All services started."
  echo ""
  echo -e "  ${TEAL}${BOLD}Dashboard${RESET}   →  ${BOLD}http://localhost:3001${RESET}"
  echo -e "  ${MU}Backend API${RESET}  →  http://localhost:8000"
  echo -e "  ${MU}ClickHouse${RESET}   →  http://localhost:8123"
  echo -e "  ${MU}PostgreSQL${RESET}   →  localhost:5432"
  echo ""
  echo -e "  ${BOLD}Run a query:${RESET}"
  echo -e "  ${TEAL}ClickHouse${RESET}   docker exec -it showdown-clickhouse clickhouse-client --password bench_pass --query \"SELECT count() FROM hits\""
  echo -e "  ${BLUE}PostgreSQL${RESET}   docker exec -it showdown-postgres psql -U bench_user -d hits -c \"SELECT count(*) FROM hits\""
  echo ""
  local _files="${PARQUET_FILES:-5}"
  warn "The loader is fetching ${_files} parquet file(s) (~$((_files * 1000000)) rows)."
  warn "ClickHouse loads in ~1 min · PostgreSQL loads in ~$((${_files} * 2))–$((${_files} * 4)) min."
  info "Tailing loader logs — press Ctrl+C to detach (services keep running)."
  echo ""
  $COMPOSE logs -f loader
}

cmd_stop() {
  banner
  check_docker
  log "Stopping all services…"
  cd "$DIR"
  $COMPOSE down
  echo ""
  log "All services stopped. Data volumes are preserved."
  info "To also delete all data:  ./showdown.sh reset"
  echo ""
}

cmd_restart() {
  cmd_stop
  cmd_start
}

cmd_status() {
  check_docker
  echo ""
  echo -e "  ${BOLD}Service Status${RESET}"
  hr
  cd "$DIR"
  $COMPOSE ps
  hr
  echo ""

  # Loader progress
  local ch_rows pg_rows
  ch_rows=$(docker exec showdown-clickhouse \
    clickhouse-client --password bench_pass -q "SELECT count() FROM hits" 2>/dev/null || echo "0")
  pg_rows=$(docker exec showdown-postgres \
    psql -U bench_user -d hits -t -c "SELECT count(*) FROM hits" 2>/dev/null | tr -d ' \n' || echo "0")

  echo -e "  ${TEAL}ClickHouse rows${RESET}   ${BOLD}${ch_rows:-0}${RESET}"
  echo -e "  ${BLUE}PostgreSQL rows${RESET}   ${BOLD}${pg_rows:-0}${RESET}"
  echo ""
}

cmd_logs() {
  check_docker
  cd "$DIR"
  local svc="${2:-}"
  if [[ -n "$svc" ]]; then
    $COMPOSE logs -f "$svc"
  else
    $COMPOSE logs -f
  fi
}

cmd_reset() {
  banner
  check_docker
  warn "This will stop all containers AND delete all data volumes (database data + parquet cache)."
  if [[ "${2:-}" == "-y" || "${2:-}" == "--yes" ]]; then
    confirm="y"
  else
    echo -ne "  ${RED}Are you sure? [y/N]${RESET} "
    read -r confirm
  fi
  if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    info "Aborted."
    exit 0
  fi
  cd "$DIR"
  $COMPOSE down -v
  rm -rf "$DIR/tmp"
  mkdir -p "$DIR/tmp"
  log "All containers, volumes, and cached parquet files removed."
  echo ""
}

cmd_help() {
  banner
  echo -e "  ${BOLD}Usage:${RESET}  ./showdown.sh <command> [options]"
  echo ""
  echo -e "  ${TEAL}start [--workers N] [--files N]${RESET}  Build images, start all services, begin data loading"
  echo -e "  ${TEAL}stop${RESET}            Stop all services (data is preserved)"
  echo -e "  ${TEAL}restart${RESET}         Stop then start"
  echo -e "  ${TEAL}status${RESET}          Show container status and row counts"
  echo -e "  ${TEAL}logs [service]${RESET}  Tail logs (all services, or one: loader/backend/clickhouse/postgres/dashboard)"
  echo -e "  ${TEAL}reset [-y]${RESET}      Stop and delete ALL data volumes (use -y to skip confirmation)"
  echo -e "  ${TEAL}help${RESET}            Show this message"
  echo ""
  echo -e "  ${MU}Examples:${RESET}"
  echo -e "    ./showdown.sh start"
  echo -e "    ./showdown.sh start --workers 4           ${MU}# override insert parallelism${RESET}"
  echo -e "    ./showdown.sh start --files 10            ${MU}# load all 10 parquet files (~10 M rows)${RESET}"
  echo -e "    ./showdown.sh start --files 10 --workers 4  ${MU}# combine both${RESET}"
  echo -e "    ./showdown.sh logs loader     ${MU}# watch dataset loading progress${RESET}"
  echo -e "    ./showdown.sh status          ${MU}# check how many rows are loaded${RESET}"
  echo -e "    ./showdown.sh stop"
  echo ""
}

# ── Entry point ───────────────────────────────────────────────────────────────
case "${1:-help}" in
  start)   cmd_start "${@:2}" ;;
  stop)    cmd_stop    ;;
  restart) cmd_restart ;;
  status)  cmd_status  ;;
  logs)    cmd_logs "$@" ;;
  reset)   cmd_reset "$@" ;;
  help|--help|-h) cmd_help ;;
  *)
    err "Unknown command: ${1}"
    cmd_help
    exit 1
    ;;
esac
