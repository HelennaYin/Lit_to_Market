# LitMarket

LitMarket is an exploratory literature analytics web app for testing whether
scientific publication activity is associated with near-term sector ETF
performance.

The app has three workflows:

- Sector overview with current publication momentum and viral-paper radar.
- Deep analysis for weekly publication signals and viral-paper event studies.
- Custom research runs for user-provided keywords and tickers.

The project uses Flask, React, Recharts, SQLite, OpenAlex, and yfinance.

## Quick Start With Docker

From this directory:

```bash
docker compose up --build
```

Open:

```text
http://127.0.0.1:8080
```

The backend API is also exposed at:

```text
http://127.0.0.1:5001/api/health
```

SQLite data is mounted from:

```text
./data/litmarket.db
```

The repository includes this seeded SQLite database so the app opens
immediately with analysis data. If the file is removed, the backend will create
an empty schema, but the analysis pages will not have meaningful rows until data
is seeded or refreshed.

## Local Development

Backend:

```bash
python3 -m pip install -r requirements.txt
python3 -m flask --app backend.app run --host 127.0.0.1 --port 5001
```

Frontend:

```bash
cd frontend
npm install
VITE_API_BASE_URL=http://127.0.0.1:5001 npm run dev -- --host 127.0.0.1 --port 5173
```

Open:

```text
http://127.0.0.1:5173
```

## Safe Database Refresh

The refresh command is intentionally incremental and non-destructive. It never
deletes `data/litmarket.db`.

Preview what would happen:

```bash
python3 -m backend.pipelines.refresh_database --dry-run --skip-nightly-radar
```

Run a conservative refresh:

```bash
python3 -m backend.pipelines.refresh_database
```

Useful safety controls:

```bash
--max-weekly-weeks 26
--nightly-max-pages 1
--nightly-max-attention-scores 50
--nightly-skip-attention
--skip-nightly-radar
```

The DOI attention cap is a batch size, not a data-loss filter. All fetched
papers are stored in SQLite first; unscored papers remain in the backlog and
can be scored by a later run.

Inside Docker:

```bash
docker compose exec backend python -m backend.pipelines.refresh_database --dry-run --skip-nightly-radar
```

## Nightly Radar

Run only the nightly viral-paper radar:

```bash
python3 -m backend.pipelines.nightly_radar --max-pages 1 --max-attention-scores 50
```

Dry-run one publication day without writes:

```bash
python3 -m backend.pipelines.nightly_radar \
  --date 2026-04-30 \
  --days 1 \
  --dry-run \
  --skip-attention \
  --max-pages 1
```

## Custom Research Runs

Use the Research Tool page in the browser, or call the API:

```bash
curl -s -X POST http://127.0.0.1:5001/api/research/runs \
  -H 'Content-Type: application/json' \
  -d '{"keywords":"machine learning","ticker":"BOTZ","date_start":"2022-01-01","date_end":"2024-01-15"}'
```

Poll the returned run ID:

```bash
curl -s http://127.0.0.1:5001/api/research/runs/RUN_ID
```

## Regenerating Web-App-Matched Robustness Figures

From the workspace root:

```bash
MPLCONFIGDIR=/private/tmp/matplotlib \
python3 "Short term analysis/robustness_check_v2.py"
```

Outputs:

```text
Short term analysis/outputs/10_extended_car_curves_v2.png
Short term analysis/outputs/11_control_test_v2.png
```

## Important Notes

- This is an exploratory analytics app, not a trading system.
- Granger tests are presented as predictive precedence, not proof of
  causality.
- Viral-paper alerts show historical event-study context, not forecasts.
- Refreshes can make many OpenAlex, Reddit, Wikipedia, and yfinance calls.
  Use `--dry-run` and the batch-size flags before long catch-up runs.
