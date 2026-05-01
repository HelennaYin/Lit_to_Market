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

Check local work state with:

```bash
cd /Users/helennayin/GhamutInterview/Build
git status --short --branch
```

## Important Build Rule

`WEBAPP_DESIGN.md` is the canonical design/build reference.

Do not overwrite `WEBAPP_DESIGN.md` unless explicitly requested.

## What Exists Now

### App skeleton

```text
analysis/
backend/
backend/api/
backend/pipelines/
data/
docs/
frontend/
```

### SQLite foundation

- `backend/schema.sql`
- `backend/database.py`
- Local DB path: `data/litmarket.db`
- DB file is ignored by git.

### Backend Flask API

Files:

```text
backend/app.py
backend/api/helpers.py
backend/api/sectors.py
backend/api/viral.py
requirements.txt
```

Endpoints:

```text
GET /api/health
GET /api/sectors
GET /api/sectors/{sector}/overview
GET /api/sectors/{sector}/analysis?signal=pub_zscore
GET /api/sectors/{sector}/viral-analysis
GET /api/viral?sector={sector}&days=5
```

Notes:

- `backend/app.py` exposes a Flask app factory and registers API blueprints.
- `backend/api/helpers.py` has shared JSON/date/sector validation helpers.
- `backend/api/sectors.py` serves sector metadata, overview data, and weekly
  publication-momentum analysis.
- `backend/api/viral.py` serves viral-paper feed and viral event-study
  analysis.
- `requirements.txt` currently includes Flask and SciPy.
- SciPy is used for t-tests in viral event-study API summaries.

### React frontend

Files:

```text
frontend/package.json
frontend/package-lock.json
frontend/vite.config.js
frontend/index.html
frontend/src/main.jsx
frontend/src/App.jsx
frontend/src/api.js
frontend/src/format.js
frontend/src/styles.css
```

Frontend stack:

```text
Vite
React
Recharts
lucide-react
```

Pages built:

- Sector Overview
  - Persistent sector selector.
  - Publication z-score / latest weekly count summary.
  - ETF weekly return summary.
  - Publication-market evidence summary.
  - Viral radar summary and feed.
  - Publication signal chart.
  - Current reading panel.

- Deep Analysis
  - Publication Momentum tab.
  - Viral Event Study tab.
  - Signal selector for:
    - `pub_deviation`
    - `pub_zscore`
    - `pub_4w_dev`

- Research Tool
  - Placeholder read-only form.
  - It does not run analysis yet because backend research runner is not built.

Frontend-specific behavior:

- Uses only Flask API responses; no direct SQLite reads.
- Publication momentum visualizations filter out:
  - incomplete current weeks
  - displayed weeks containing January 1 publication data
- Jan 1 filtering is visualization-only for publication momentum charts.
  Do not remove Jan 1 viral-study rows from the DB for this reason.
- Viral radar feed uses the real current date for the `days=5` window.
  On 2026-05-01, that means publication dates from 2026-04-27 through
  2026-05-01.
- Viral event-study chart compares real viral-paper dates against randomized
  control dates.
- Day 0 and 0.0% CAR are emphasized in the viral event-study chart.
- Attention-vs-CAR tooltip includes paper title, Reddit hits, and CAR+5.
- “Absolute abnormal return” is spelled out in the volatility panel.
- Overview now uses “Publication-market evidence” wording instead of the
  unclear “Weekly evidence” framing.

## Data Pipelines and Helper Scripts

### Viral cache cleaner

File:

```text
backend/pipelines/clean_viral_cache.py
```

Purpose:

- Reads raw cached viral files from:

```text
../Short term analysis/cache
```

- Writes cleaned derived files to:

```text
data/cleaned/
```

- Output is ignored by git.
- Removes journal/venue artifacts such as:
  - `Oncology Reports`
  - `Oncology Letters`
  - `International Journal of Oncology`

Important current fix:

- Event windows are cleaned based on their own paper/event identity.
- Do not assume `viral_events.csv` row index and `event_windows.csv event_id`
  refer to the same paper.

### Database seed loader

File:

```text
backend/pipelines/seed_database.py
```

Purpose:

- Seeds SQLite from current source CSV/JSON files and cleaned viral files.
- Resets the local DB by default.

Important current fix:

- Viral event windows are linked to DB `viral_events` by stable identity:

```text
doi + sector + event_date
```

- This fixed a serious sector-mapping bug where event windows from one sector
  were being attached to viral papers from another sector.
- Verified after reseeding:

```text
event_windows where event_windows.sector != viral_events.sector: 0
```

### Source refresh utility

File:

```text
backend/pipelines/refresh_sources.py
```

Purpose:

- Refreshes source files, not SQLite.
- OpenAlex weekly counts use the concept-ID method from:

```text
Logic_test/Fetch_openalex/fetch_openalex_weekly_v2test.py
```

- yfinance refresh is resilient to rate limits and keeps existing raw CSVs if
  downloads fail.

### Frontend helper modules

Files:

```text
frontend/src/api.js
frontend/src/format.js
```

Purpose:

- `api.js` centralizes API calls to Flask.
- Default API base:

```text
http://127.0.0.1:5001
```

- Can be overridden with:

```text
VITE_API_BASE_URL
```

- `format.js` centralizes number, percent, integer, date, title, and signal
  label formatting.

## Verified Data State

Weekly OpenAlex counts were refreshed by the user for Clean Energy and
Semiconductors before this handoff.

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

## Run Commands

Install backend dependencies:

```bash
cd /Users/helennayin/GhamutInterview/Build
python3 -m pip install -r requirements.txt
```

Start Flask API:

```bash
cd /Users/helennayin/GhamutInterview/Build
python3 -m flask --app backend.app run --host 127.0.0.1 --port 5001
```

Install frontend dependencies:

```bash
cd /Users/helennayin/GhamutInterview/Build/frontend
npm install
```

Start React/Vite frontend:

```bash
cd /Users/helennayin/GhamutInterview/Build/frontend
npm run dev
```

Open app:

```text
http://127.0.0.1:5173
```

API health:

```text
http://127.0.0.1:5001/api/health
```

## Verification Commands

Clean viral cache:

```bash
cd /Users/helennayin/GhamutInterview/Build
python3 -m backend.pipelines.clean_viral_cache
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
    "viral_event_results",
    "radar_thresholds",
]

with get_connection() as conn:
    for table in tables:
        n = conn.execute(f"select count(*) from {table}").fetchone()[0]
        print(f"{table:28s} {n}")
PY
```

Verify viral event-window sector links:

```bash
python3 - <<'PY'
from backend.database import get_connection

with get_connection() as conn:
    n = conn.execute("""
        select count(*)
        from event_windows w
        join viral_events v on v.id = w.viral_event_id
        where w.sector != v.sector
    """).fetchone()[0]
    print(n)
PY
```

Expected output:

```text
0
```

Verify API routes:

```bash
python3 - <<'PY'
from backend.app import create_app

app = create_app()
client = app.test_client()

paths = [
    "/api/health",
    "/api/sectors",
    "/api/sectors/ai_tech/overview",
    "/api/sectors/ai_tech/analysis?signal=pub_zscore",
    "/api/sectors/ai_tech/viral-analysis",
    "/api/viral?sector=ai_tech&days=5",
]

for path in paths:
    resp = client.get(path)
    print(path, resp.status_code)
PY
```

Frontend build:

```bash
cd /Users/helennayin/GhamutInterview/Build/frontend
npm run build
```

Known build note:

- Vite reports a chunk-size warning because Recharts and chart dependencies are
  bundled together.
- This is not currently a build failure.

## Useful Source Refresh Commands

Dry-run source refresh:

```bash
cd /Users/helennayin/GhamutInterview/Build
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

## Current Known Caveats

- `data/litmarket.db` is ignored and currently local only.
- `data/cleaned/` is ignored and derived.
- `frontend/node_modules/` and `frontend/dist/` are ignored generated outputs.
- yfinance may rate-limit; use existing raw CSVs or Colab-downloaded files if
  needed.
- Market data is missing at most a couple recent trading days, acceptable for
  the current demo seed.
- `viral_event_results.p_value` is still not recomputed by the seed loader.
  The API now computes CAR+5 and control-test p-values from DB rows at request
  time.
- The randomized control test in `backend/api/viral.py` is deterministic for a
  given sector, but it is currently an API-time computation. A future pipeline
  should persist these results in SQLite.
- The Research Tool page is UI-only. Backend custom research runs are not
  implemented yet.
- The mapped module refactor listed in `WEBAPP_DESIGN.md` is not complete.
  Not yet implemented:
  - `analysis/stats.py`
  - `analysis/signals.py`
  - `analysis/runner.py`
  - `backend/pipelines/market.py`
  - `backend/pipelines/viral_openalex.py`
  - `backend/pipelines/viral_filters.py`
  - `backend/pipelines/viral_attention.py`
  - `backend/pipelines/viral_event_study.py`
  - `backend/pipelines/nightly_radar.py`

## Suggested Next Steps

1. Review frontend in browser at `http://127.0.0.1:5173`.
2. Tighten frontend copy and chart layout based on user feedback.
3. Persist viral control-test results instead of computing them at API time.
4. Build the DB-native update pipeline:

```text
backend/pipelines/update_database.py
```

5. Start the mapped module refactor from `WEBAPP_DESIGN.md`, especially:

```text
analysis/stats.py
analysis/signals.py
analysis/runner.py
backend/pipelines/viral_event_study.py
backend/pipelines/nightly_radar.py
```
