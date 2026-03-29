<#
.SYNOPSIS
  OLAP Showdown – Windows launcher (PowerShell + WSL/Docker Desktop)

.DESCRIPTION
  Requires Docker Desktop for Windows with the WSL 2 backend enabled,
  or Docker running inside WSL 2 (accessible via the same 'docker' command).

.EXAMPLE
  .\showdown.ps1 start
  .\showdown.ps1 logs loader
  .\showdown.ps1 status
  .\showdown.ps1 reset -y
#>

param(
    [Parameter(Position=0)] [string]$Command = "help",
    [Parameter(Position=1)] [string]$Arg1    = "",
    [switch]$y,                  # -y  flag for reset
    [switch]$yes,                # --yes flag for reset
    [int]$Workers = 0,           # -Workers N  flag for start
    [int]$Files   = 0            # -Files N    flag for start (1-10)
)

$DIR = Split-Path -Parent $MyInvocation.MyCommand.Path

# ── Colour helpers ────────────────────────────────────────────────────────────
function Write-Teal  { param($m) Write-Host "  $m" -ForegroundColor Cyan    }
function Write-Blue  { param($m) Write-Host "  $m" -ForegroundColor Blue    }
function Write-Green { param($m) Write-Host "  $m" -ForegroundColor Green   }
function Write-Amber { param($m) Write-Host "  $m" -ForegroundColor Yellow  }
function Write-Red   { param($m) Write-Host "  $m" -ForegroundColor Red     }
function Write-Mu    { param($m) Write-Host "  $m" -ForegroundColor DarkCyan}

function log  { param($m) Write-Green  "▶  $m" }
function info { param($m) Write-Blue   "i  $m" }
function warn { param($m) Write-Amber  "!  $m" }
function err  { param($m) Write-Red    "x  $m" }
function hr   { Write-Mu  "────────────────────────────────────────────────────" }

function banner {
    Write-Host ""
    Write-Teal  "  ██████╗ ██╗      █████╗ ██████╗     ███████╗██╗  ██╗ ██████╗ ██╗    ██╗██████╗  ██████╗ ██╗    ██╗███╗   ██╗"
    Write-Blue  "  ██╔═══██╗██║     ██╔══██╗██╔══██╗    ██╔════╝██║  ██║██╔═══██╗██║    ██║██╔══██╗██╔═══██╗██║    ██║████╗  ██║"
    Write-Teal  "  ██║   ██║██║     ███████║██████╔╝    ███████╗███████║██║   ██║██║ █╗ ██║██║  ██║██║   ██║██║ █╗ ██║██╔██╗ ██║"
    Write-Blue  "  ██║   ██║██║     ██╔══██║██╔═══╝     ╚════██║██╔══██║██║   ██║██║███╗██║██║  ██║██║   ██║██║███╗██║██║╚██╗██║"
    Write-Teal  "  ╚██████╔╝███████╗██║  ██║██║         ███████║██║  ██║╚██████╔╝╚███╔███╔╝██████╔╝╚██████╔╝╚███╔███╔╝██║ ╚████║"
    Write-Blue  "   ╚═════╝ ╚══════╝╚═╝  ╚═╝╚═╝         ╚══════╝╚═╝  ╚═╝ ╚═════╝  ╚══╝╚══╝ ╚═════╝  ╚═════╝  ╚══╝╚══╝ ╚═╝  ╚═══╝"
    Write-Host ""
    Write-Mu    "ClickBench · 43 queries · ClickHouse 26.3 vs PostgreSQL 17 · IS459"
    Write-Host ""
}

# ── Docker check ──────────────────────────────────────────────────────────────
function Check-Docker {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        err "Docker is not found. Install Docker Desktop from https://docs.docker.com/get-docker/"
        err "Make sure the WSL 2 backend is enabled in Docker Desktop settings."
        exit 1
    }
    $null = docker info 2>&1
    if ($LASTEXITCODE -ne 0) {
        err "Docker daemon is not running. Please start Docker Desktop."
        exit 1
    }
}

# ── Commands ──────────────────────────────────────────────────────────────────
function Cmd-Start {
    param([int]$WorkerCount = 0, [int]$FileCount = 0)

    if ($WorkerCount -gt 0) {
        $env:INSERT_WORKERS = "$WorkerCount"
        log "Insert workers overridden to $WorkerCount."
    } else {
        $env:INSERT_WORKERS = ""
    }

    if ($FileCount -gt 0) {
        if ($FileCount -lt 1 -or $FileCount -gt 10) {
            err "-Files must be between 1 and 10 (got: $FileCount)"
            exit 1
        }
        $env:PARQUET_FILES = "$FileCount"
        log "Parquet files set to $FileCount (~$($FileCount * 1000000) rows)."
    } else {
        $env:PARQUET_FILES = ""
    }

    banner
    Check-Docker
    log "Building images and starting all services..."
    hr
    Set-Location $DIR
    docker compose up --build -d
    if ($LASTEXITCODE -ne 0) { err "docker compose up failed."; exit 1 }
    hr
    Write-Host ""
    log "All services started."
    Write-Host ""
    Write-Host "  " -NoNewline; Write-Host "Dashboard" -ForegroundColor Cyan -NoNewline; Write-Host "   ->  " -NoNewline; Write-Host "http://localhost:3001" -ForegroundColor White
    Write-Mu    "Backend API  ->  http://localhost:8000"
    Write-Mu    "ClickHouse   ->  http://localhost:8123"
    Write-Mu    "PostgreSQL   ->  localhost:5432"
    Write-Host ""
    Write-Host "  Run a query:" -ForegroundColor White
    Write-Host "  " -NoNewline; Write-Host "ClickHouse" -ForegroundColor Cyan -NoNewline
    Write-Host "   docker exec -it showdown-clickhouse clickhouse-client --password bench_pass --query ""SELECT count() FROM hits"""
    Write-Host "  " -NoNewline; Write-Host "PostgreSQL" -ForegroundColor Blue -NoNewline
    Write-Host "   docker exec -it showdown-postgres psql -U bench_user -d hits -c ""SELECT count(*) FROM hits"""
    Write-Host ""
    $f = if ($env:PARQUET_FILES) { [int]$env:PARQUET_FILES } else { 5 }
    warn "The loader is fetching $f parquet file(s) (~$($f * 1000000) rows)."
    warn "ClickHouse loads in ~1 min  |  PostgreSQL loads in ~$($f * 2)-$($f * 4) min."
    info "Tailing loader logs — press Ctrl+C to detach (services keep running)."
    Write-Host ""
    docker compose logs -f loader
}

function Cmd-Stop {
    banner
    Check-Docker
    log "Stopping all services..."
    Set-Location $DIR
    docker compose down
    Write-Host ""
    log "All services stopped. Data volumes are preserved."
    info "To also delete all data:  .\showdown.ps1 reset"
    Write-Host ""
}

function Cmd-Restart {
    Cmd-Stop
    Cmd-Start
}

function Cmd-Status {
    Check-Docker
    Write-Host ""
    Write-Host "  Service Status" -ForegroundColor White
    hr
    Set-Location $DIR
    docker compose ps
    hr
    Write-Host ""

    $chRows = docker exec showdown-clickhouse `
        clickhouse-client --password bench_pass -q "SELECT count() FROM hits" 2>$null
    if ($LASTEXITCODE -ne 0) { $chRows = "0" }

    $pgRows = docker exec showdown-postgres `
        psql -U bench_user -d hits -t -c "SELECT count(*) FROM hits" 2>$null
    if ($LASTEXITCODE -ne 0) { $pgRows = "0" }
    $pgRows = ($pgRows -replace '\s','')

    Write-Host "  " -NoNewline
    Write-Host "ClickHouse rows" -ForegroundColor Cyan -NoNewline
    Write-Host "   $($chRows.Trim())"

    Write-Host "  " -NoNewline
    Write-Host "PostgreSQL rows" -ForegroundColor Blue -NoNewline
    Write-Host "   $($pgRows.Trim())"

    Write-Host ""
}

function Cmd-Logs {
    param([string]$Svc = "")
    Check-Docker
    Set-Location $DIR
    if ($Svc -ne "") {
        docker compose logs -f $Svc
    } else {
        docker compose logs -f
    }
}

function Cmd-Reset {
    param([bool]$Force = $false)
    banner
    Check-Docker
    warn "This will stop all containers AND delete all data volumes (database data + parquet cache)."
    if (-not $Force) {
        $confirm = Read-Host "  Are you sure? [y/N]"
    } else {
        $confirm = "y"
    }
    if ($confirm -ne "y" -and $confirm -ne "Y") {
        info "Aborted."
        exit 0
    }
    Set-Location $DIR
    docker compose down -v
    $tmpDir = Join-Path $DIR "tmp"
    if (Test-Path $tmpDir) { Remove-Item -Recurse -Force $tmpDir }
    New-Item -ItemType Directory -Path $tmpDir | Out-Null
    log "All containers, volumes, and cached parquet files removed."
    Write-Host ""
}

function Cmd-Help {
    banner
    Write-Host "  Usage:  .\showdown.ps1 <command> [options]" -ForegroundColor White
    Write-Host ""
    Write-Host "  " -NoNewline; Write-Host "start [-Workers N] [-Files N]" -ForegroundColor Cyan -NoNewline; Write-Host " Build images, start all services, begin data loading"
    Write-Host "  " -NoNewline; Write-Host "stop           " -ForegroundColor Cyan -NoNewline; Write-Host " Stop all services (data is preserved)"
    Write-Host "  " -NoNewline; Write-Host "restart        " -ForegroundColor Cyan -NoNewline; Write-Host " Stop then start"
    Write-Host "  " -NoNewline; Write-Host "status         " -ForegroundColor Cyan -NoNewline; Write-Host " Show container status and row counts"
    Write-Host "  " -NoNewline; Write-Host "logs [service] " -ForegroundColor Cyan -NoNewline; Write-Host " Tail logs (all, or one: loader/backend/clickhouse/postgres/dashboard)"
    Write-Host "  " -NoNewline; Write-Host "reset [-y]     " -ForegroundColor Cyan -NoNewline; Write-Host " Stop and delete ALL data volumes (-y to skip confirmation)"
    Write-Host "  " -NoNewline; Write-Host "help           " -ForegroundColor Cyan -NoNewline; Write-Host " Show this message"
    Write-Host ""
    Write-Mu    "Examples:"
    Write-Mu    "  .\showdown.ps1 start"
    Write-Mu    "  .\showdown.ps1 start -Workers 4              # override insert parallelism"
    Write-Mu    "  .\showdown.ps1 start -Files 10              # load all 10 parquet files (~10 M rows)"
    Write-Mu    "  .\showdown.ps1 start -Files 10 -Workers 4   # combine both"
    Write-Mu    "  .\showdown.ps1 logs loader       # watch dataset loading progress"
    Write-Mu    "  .\showdown.ps1 status            # check how many rows are loaded"
    Write-Mu    "  .\showdown.ps1 reset -y          # wipe data without confirmation"
    Write-Mu    "  .\showdown.ps1 stop"
    Write-Host ""
    Write-Host "  Prerequisites:" -ForegroundColor White
    Write-Mu    "  - Docker Desktop for Windows with WSL 2 backend enabled"
    Write-Mu    "  - Run this script from PowerShell (not CMD)"
    Write-Mu    "  - If script execution is blocked, run once:"
    Write-Mu    "      Set-ExecutionPolicy -Scope CurrentUser RemoteSigned"
    Write-Host ""
}

# ── Entry point ───────────────────────────────────────────────────────────────
$forceReset = $y -or $yes

switch ($Command.ToLower()) {
    "start"   { Cmd-Start -WorkerCount $Workers -FileCount $Files }
    "stop"    { Cmd-Stop }
    "restart" { Cmd-Restart }
    "status"  { Cmd-Status }
    "logs"    { Cmd-Logs -Svc $Arg1 }
    "reset"   { Cmd-Reset -Force $forceReset }
    "help"    { Cmd-Help }
    "--help"  { Cmd-Help }
    "-h"      { Cmd-Help }
    default   {
        err "Unknown command: $Command"
        Cmd-Help
        exit 1
    }
}
