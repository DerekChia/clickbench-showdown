<#
.SYNOPSIS
  ClickBench Showdown – Windows launcher (PowerShell + WSL/Docker Desktop)

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
    Write-Mu    "ClickBench · 43 queries · Multi-Database Showdown"
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
        log "Parquet files set to $FileCount (~$($FileCount * 1000000) rows) — pre-downloaded and preselected in the dashboard."
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
    Write-Host "  " -NoNewline; Write-Host "Dashboard" -ForegroundColor Cyan -NoNewline; Write-Host "   ->  " -NoNewline; Write-Host "http://localhost:3000" -ForegroundColor White
    Write-Mu    "Backend API  ->  http://localhost:8000"
    Write-Host ""
    info "Select two databases to compare from the dashboard."
    $f = if ($env:PARQUET_FILES) { [int]$env:PARQUET_FILES } else { 1 }
    warn "The loader will fetch $f parquet file(s) (~$($f * 1000000) rows)."
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
    param([int]$WorkerCount = 0, [int]$FileCount = 0)
    Cmd-Stop
    Cmd-Start -WorkerCount $WorkerCount -FileCount $FileCount
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

    # Database containers are created on demand by the backend, so list whatever
    # is currently up rather than assuming a fixed pair.
    $dbContainers = docker ps --filter "name=showdown-" --format "{{.Names}}`t{{.Status}}"
    if ($dbContainers) {
        Write-Host "  Database containers" -ForegroundColor White
        hr
        $dbContainers | ForEach-Object { Write-Mu $_ }
        Write-Host ""
    }

    info "Check the dashboard at http://localhost:3000 for live row counts."
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
    warn "This will stop all containers AND delete all database volumes. Cached files in tmp/ are preserved."
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

    # Stop and remove dynamically-managed DB containers
    log "Stopping database containers..."
    $names = docker ps -a --filter "name=showdown-" --format "{{.Names}}"
    foreach ($name in $names) {
        docker stop $name 2>&1 | Out-Null
        docker rm $name 2>&1 | Out-Null
    }

    docker compose down -v

    # Remove named volumes created by the DB plugin system. Every one of them is
    # prefixed `showdown-`, so this can never delete another project's data.
    log "Removing database volumes..."
    $vols = docker volume ls --format "{{.Name}}" | Where-Object { $_ -like "showdown-*" }
    foreach ($vol in $vols) {
        docker volume rm $vol 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) { info "Removed volume: $vol" } else { warn "Could not remove volume: $vol" }
    }

    log "All containers and database volumes removed. Parquet files in tmp\ are preserved."
    Write-Host ""
}

function Cmd-Help {
    banner
    Write-Host "  Usage:  .\showdown.ps1 <command> [options]" -ForegroundColor White
    Write-Host ""
    Write-Host "  " -NoNewline; Write-Host "start [-Workers N] [-Files N]" -ForegroundColor Cyan -NoNewline; Write-Host " Build images, start all services, pre-download data"
    Write-Host "  " -NoNewline; Write-Host "stop           " -ForegroundColor Cyan -NoNewline; Write-Host " Stop all services (data is preserved)"
    Write-Host "  " -NoNewline; Write-Host "restart        " -ForegroundColor Cyan -NoNewline; Write-Host " Stop then start (same options as start)"
    Write-Host "  " -NoNewline; Write-Host "status         " -ForegroundColor Cyan -NoNewline; Write-Host " Show container status"
    Write-Host "  " -NoNewline; Write-Host "logs [service] " -ForegroundColor Cyan -NoNewline; Write-Host " Tail logs (all, or one: loader/backend/dashboard)"
    Write-Host "  " -NoNewline; Write-Host "reset [-y]     " -ForegroundColor Cyan -NoNewline; Write-Host " Stop and delete ALL database volumes (-y to skip confirmation)"
    Write-Host "  " -NoNewline; Write-Host "help           " -ForegroundColor Cyan -NoNewline; Write-Host " Show this message"
    Write-Host ""
    Write-Mu    "Examples:"
    Write-Mu    "  .\showdown.ps1 start"
    Write-Mu    "  .\showdown.ps1 start -Workers 4             # override insert parallelism"
    Write-Mu    "  .\showdown.ps1 start -Files 10              # pre-download 10 parquet files (~10 M rows)"
    Write-Mu    "  .\showdown.ps1 start -Files 10 -Workers 4   # combine both"
    Write-Mu    "  .\showdown.ps1 logs loader       # watch dataset loading progress"
    Write-Mu    "  .\showdown.ps1 status            # check which containers are up"
    Write-Mu    "  .\showdown.ps1 reset -y          # wipe database volumes without confirmation"
    Write-Mu    "  .\showdown.ps1 stop"
    Write-Host ""
    Write-Mu    "-Files pre-downloads that many files and preselects them in the"
    Write-Mu    "dashboard; the dropdown there decides how much is actually loaded."
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
    "restart" { Cmd-Restart -WorkerCount $Workers -FileCount $Files }
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
