"""
Fetch and cache ClickBench queries from the official GitHub repository.

Each database in ClickBench has its own queries.sql with 43 queries (one per line).
This module fetches them, caches to disk, and serves from cache when possible.
"""

import os
import time

import httpx

# Pinned to a specific ClickBench commit so query changes upstream don't
# silently alter benchmark results between runs. Verified to carry 43 queries
# for all eight supported databases. Override with the CLICKBENCH_COMMIT env
# var (e.g. "main") to track upstream instead.
CLICKBENCH_COMMIT = os.environ.get(
    "CLICKBENCH_COMMIT", "d352060d03ea816a089b28431aba9c650ad385b4"
)
CLICKBENCH_RAW_URL = (
    f"https://raw.githubusercontent.com/ClickHouse/ClickBench/{CLICKBENCH_COMMIT}"
)
CACHE_DIR = os.environ.get("QUERY_CACHE_DIR", "/tmp/clickbench_query_cache")
CACHE_TTL_SEC = 24 * 3600  # 24 hours

# In-repo snapshot of the pinned commit's queries, used when GitHub is
# unreachable and nothing is cached — otherwise a fresh clone on an air-gapped
# or proxied network can never get past "Setup failed".
BUNDLED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queries_bundled")

QUERY_LABELS = [
    "Total row count",
    "Count where AdvEngineID != 0",
    "Sum AdvEngineID, count, avg ResolutionWidth",
    "Average UserID",
    "Count distinct UserID",
    "Count distinct SearchPhrase",
    "Min/max EventDate",
    "Top AdvEngineID by count",
    "Top regions by unique users",
    "Region rollup: sum, count, avg, distinct users",
    "Top mobile models by unique users",
    "Top mobile phone + model by unique users",
    "Top search phrases by count",
    "Top search phrases by unique users",
    "Top search engine + phrase by count",
    "Top users by event count",
    "Top user + phrase by count",
    "User + phrase count (no ORDER BY)",
    "User + minute + phrase by count",
    "Lookup single UserID",
    "Count rows with 'google' in URL",
    "Top search phrases from google URLs",
    "Top phrases: Google in title, not google.* in URL",
    "Full rows from google URLs (LIMIT 10)",
    "Search phrases ordered by time",
    "Search phrases ordered by phrase",
    "Search phrases ordered by time then phrase",
    "Counters by avg URL length (>100k hits)",
    "Referer domain by avg length (>100k hits)",
    "90x sum of ResolutionWidth offsets",
    "Search engine + IP rollup (search traffic)",
    "WatchID + IP rollup (search traffic)",
    "WatchID + IP rollup (all traffic)",
    "Top URLs by view count",
    "Top URLs by view count (with literal 1)",
    "ClientIP arithmetic group-by",
    "Counter 62: top URLs Jul 2013 (no bounce)",
    "Counter 62: top titles Jul 2013 (no bounce)",
    "Counter 62: linked non-download URLs (offset 1000)",
    "Counter 62: traffic source x URL (offset 1000)",
    "Counter 62: by URLHash + referer hash (offset 100)",
    "Counter 62: by window size + URLHash (offset 10000)",
    "Counter 62: page views per minute (offset 1000)",
]

# Compact variants for the dashboard table, where the long labels don't fit.
# Kept here rather than in the dashboard so there is one source of truth.
QUERY_LABELS_SHORT = [
    "Total row count", "Count AdvEngineID!=0", "Sum/count/avg agg", "Average UserID",
    "Distinct UserID", "Distinct SearchPhrase", "Min/max EventDate", "Top AdvEngineID",
    "Top regions by user", "Region rollup", "Top mobile models", "Top phone+model",
    "Top search phrases", "Top phrases by user", "Top engine+phrase", "Top users",
    "User+phrase (sorted)", "User+phrase (unsorted)", "User+minute+phrase", "Lookup UserID",
    "Count google URLs", "Top phrases (google)", "Google title filter", "Google rows *",
    "Phrases by time", "Phrases a-order", "Phrases time+a", "Counter avg URL len",
    "Referer domain regex", "90x width sums", "Engine+IP (search)", "WatchID+IP (search)",
    "WatchID+IP (all)", "Top URLs", "Top URLs (with 1)", "ClientIP arithmetic",
    "Counter62 top URLs", "Counter62 top titles", "Counter62 linked", "Counter62 sources",
    "Counter62 URLHash", "Counter62 window", "Counter62 per-minute",
]

NUM_QUERIES = 43


def _cache_path(repo_path: str) -> str:
    safe_name = repo_path.replace("/", "_").replace("\\", "_")
    # Commit is part of the filename so re-pinning invalidates old caches.
    return os.path.join(CACHE_DIR, f"{safe_name}_{CLICKBENCH_COMMIT[:12]}_queries.sql")


def _read_cache(repo_path: str) -> list[str] | None:
    """Read queries from cache if fresh enough. Never raises."""
    try:
        path = _cache_path(repo_path)
        if not os.path.exists(path):
            return None
        if time.time() - os.path.getmtime(path) > CACHE_TTL_SEC:
            return None
        return _parse_file(path)
    except OSError:
        return None


def _read_stale_cache(repo_path: str) -> list[str] | None:
    """Read queries from cache regardless of age (fallback). Never raises."""
    try:
        path = _cache_path(repo_path)
        if not os.path.exists(path):
            return None
        return _parse_file(path)
    except OSError:
        return None


def _read_bundled(repo_path: str) -> list[str] | None:
    """Read the in-repo snapshot for a database, if one was shipped."""
    path = os.path.join(BUNDLED_DIR, repo_path, "queries.sql")
    if not os.path.exists(path):
        return None
    return _parse_file(path)


def _parse_file(path: str) -> list[str]:
    with open(path) as f:
        content = f.read()
    return _parse_queries(content)


def _parse_queries(content: str) -> list[str]:
    """Parse queries.sql — one query per line, strip semicolons and whitespace."""
    queries = []
    for line in content.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        if line.endswith(";"):
            line = line[:-1].strip()
        if line:
            queries.append(line)
    return queries


def _write_cache(repo_path: str, content: str) -> None:
    """Best-effort cache write — an unwritable cache dir must not be fatal."""
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(_cache_path(repo_path), "w") as f:
            f.write(content)
    except OSError as e:
        print(f"[queries] Could not cache queries for '{repo_path}': {e}", flush=True)


async def fetch_queries(repo_path: str) -> list[str]:
    """
    Fetch queries for a database from the ClickBench repo.

    Returns a list of 43 SQL query strings.

    Resolution order: fresh cache -> GitHub -> stale cache -> in-repo snapshot.
    """
    # Try fresh cache first
    cached = _read_cache(repo_path)
    if cached is not None:
        return cached

    # Fetch from GitHub
    url = f"{CLICKBENCH_RAW_URL}/{repo_path}/queries.sql"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            content = resp.text
            _write_cache(repo_path, content)
            return _parse_queries(content)
    except Exception:
        # Fall back to stale cache
        stale = _read_stale_cache(repo_path)
        if stale is not None:
            return stale
        # Then the snapshot shipped in the repo, so the app still runs offline.
        bundled = _read_bundled(repo_path)
        if bundled is not None:
            print(
                f"[queries] GitHub unreachable — using bundled snapshot for '{repo_path}'",
                flush=True,
            )
            return bundled
        raise RuntimeError(
            f"Cannot fetch queries for '{repo_path}' from GitHub, and neither a "
            f"cache nor a bundled snapshot is available"
        )
