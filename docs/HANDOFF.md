# LitMarket Build Handoff

Last updated: 2026-05-01

## Current Repo

Working app repo lives in:

```text
/Users/helennayin/GhamutInterview/Build
```

GitHub remote:

```text
https://github.com/HelennaYin/Lit_to_Market.git
```

`main` has local commits not yet pushed at time of writing. Check with:

```bash
cd /Users/helennayin/GhamutInterview/Build
git status --short --branch
```

## What Has Been Built

- `WEBAPP_DESIGN.md`
  - Canonical design/build reference.
  - Do not overwrite unless explicitly requested.

- App skeleton:
  - `backend/`
  - `backend/api/`
  - `backend/pipelines/`
  - `analysis/`
  - `frontend/`
  - `data/`
  - `docs/`

- SQLite foundation:
  - `backend/schema.sql`
  - `backend/database.py`
  - Local DB path: `data/litmarket.db`
  - DB file is ignored by git.

- Viral cache cleaner:
  - `backend/pipelines/clean_viral_cache.py`
  - Reads raw cache from `../Short term analysis/cache`
  - Writes cleaned derived files to `data/cleaned/`
  - Output is ignored by git.
  - Removes journal/venue artifacts such as `Oncology Reports`,
    `Oncology Letters`, and `International Journal of Oncology`.

- Source refresh utility:
  - `backend/pipelines/refresh_sources.py`
  - Refreshes source files, not SQLite.
  - OpenAlex weekly counts use the concept-ID method from
    `Logic_test/Fetch_openalex/fetch_openalex_weekly_v2test.py`.
  - yfinance refresh is resilient to rate limits and keeps existing raw CSVs
    if downloads fail.

- Database seed loader:
  - `backend/pipelines/seed_database.py`
  - Seeds SQLite from current source CSV/JSON files and cleaned viral files.
  - Resets the local DB by default.

## Verified Data State

Weekly OpenAlex counts were refreshed by the user for Clean Energy and
Semiconductors.

Seeded DB verification after running:

```bash
python3 -m backend.pipelines.seed_database
```

Expected current counts:

```text
sectors                    4
market_daily               10201
spy_daily                  2594
abnormal_returns_weekly    2116
publications_weekly        2160
analysis_results           12
papers                     4395
attention_scores           4395
viral_events               204
event_windows              983
viral_event_results        4
radar_thresholds           4
```

Important date ranges:

```text
publications_weekly: all 4 sectors through 2026-04-27
market_daily: through 2026-04-28
viral_events: cleaned historical events, 2025-04-30 through 2026-04-27/28 depending sector
```

Known journal-title artifacts should be zero in seeded `papers`.

## Useful Commands

Clean viral cache:

```bash
cd /Users/helennayin/GhamutInterview/Build
python3 -m backend.pipelines.clean_viral_cache
```

Dry-run source refresh:

```bash
python3 -m backend.pipelines.refresh_sources --dry-run
```

Refresh only OpenAlex weekly source files:

```bash
python3 -m backend.pipelines.refresh_sources --skip-market
```

Refresh only market source files:

```bash
python3 -m backend.pipelines.refresh_sources --skip-openalex
```

Seed local SQLite DB:

```bash
python3 -m backend.pipelines.seed_database
```

Verify table counts:

```bash
python3 - <<'PY'
from backend.database import get_connection

tables = [
    "sectors",
    "publications_weekly",
    "analysis_results",
    "market_daily",
    "spy_daily",
    "papers",
    "attention_scores",
    "viral_events",
    "event_windows",
    "radar_thresholds",
]

with get_connection() as conn:
    for table in tables:
        n = conn.execute(f"select count(*) from {table}").fetchone()[0]
        print(f"{table:28s} {n}")
PY
```

## Important Clarification

There are two data layers:

1. Source CSV/JSON files in `Logic_test` and `Short term analysis`.
2. SQLite app database in `Build/data/litmarket.db`.

`refresh_sources.py` updates source files only.

`seed_database.py` loads source files into SQLite.

Future deployed app refresh should use a DB-native updater, likely:

```text
backend/pipelines/update_database.py
```

That script does not exist yet.

## Next Recommended Step

Build read-only Flask API endpoints against the seeded SQLite DB:

```text
GET /api/sectors
GET /api/sectors/{sector}/overview
GET /api/sectors/{sector}/analysis?signal=pub_zscore
GET /api/sectors/{sector}/viral-analysis
GET /api/viral?sector={sector}&days=5
```

Suggested files:

```text
backend/app.py
backend/api/sectors.py
backend/api/viral.py
requirements.txt
```

After the API is working, build the React frontend against those stable
JSON responses.

## Known Caveats

- `data/litmarket.db` is ignored and currently local only.
- `data/cleaned/` is ignored and derived.
- yfinance may rate-limit; use existing raw CSVs or Colab-downloaded files
  if needed.
- Market data is missing at most a couple recent trading days, acceptable for
  the current demo seed.
- `viral_event_results.p_value` is not recomputed by the seed loader. It
  stores summary CAR+5 context only; formal p-values should come from the
  original analysis scripts or a future stats module.
