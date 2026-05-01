# LitMarket Web Application Design

This document is the build reference for the LitMarket interview
application. It combines the existing sector-level publication momentum
analysis in `Logic_test` with the viral-paper event-study pipeline in
`Short term analysis` and the nightly alert concept in
`nightly_radar_plan.md`.

The application is an exploratory literature analytics platform, not a
trading system. It should report both positive and null findings honestly.

---

## 1. Product Purpose

LitMarket explores whether scientific publication activity is associated
with near-term sector ETF performance.

The app has two connected analytical modes:

1. **Publication Momentum Analysis**
   - Weekly sector-level publication counts.
   - Weekly ETF abnormal returns adjusted against SPY.
   - Lag correlation, rolling correlation, Granger tests, and CAR after
     publication-volume surges.
   - Source code: `Logic_test/stats.py`, `Logic_test/signals.py`,
     `Logic_test/runner.py`, and `Logic_test/03_analysis_modular.ipynb`.

2. **Viral Paper Radar**
   - Individual recent papers by sector.
   - Composite Attention Score (CAS) from Reddit DOI mentions,
     Wikipedia DOI mentions, and citation velocity.
   - Daily market-model event study around viral paper publication dates.
   - Nightly alert generation using historical viral-event thresholds.
   - Source code: `Short term analysis/fetch_openalex.py`,
     `Short term analysis/filter_papers.py`,
     `Short term analysis/fetch_attention.py`,
     `Short term analysis/clean_align.py`,
     `Short term analysis/event_study_plots.py`,
     `Short term analysis/analysis.py`,
     `Short term analysis/robustness_check.py`, and
     `nightly_radar_plan.md`.

The app serves:

- **Explorers** who want to inspect the four analyzed sectors:
  AI/Tech, Biotech/Pharma, Clean Energy, and Semiconductors.
- **Researchers** who want to run the same weekly publication-momentum
  analysis on custom keyword/title searches and a selected ticker.

---

## 2. Sector Definitions

The app uses one sector selector shared across all pages.

| Sector key | Label | Current weekly ETF | Current viral ETF | Notes |
|---|---|---|---|---|
| `ai_tech` | AI/Tech | BOTZ | AIQ | Keep AIQ for existing viral cached data. Switch viral pipeline to BOTZ later. |
| `biotech_pharma` | Biotech/Pharma | XBI | XBI | |
| `clean_energy` | Clean Energy | ICLN | ICLN | |
| `semiconductors` | Semiconductors | SOXX | SOXX | |

Keyword/title search is the canonical publication fetch method. Do not
build the app around OpenAlex concept IDs. The current OpenAlex scripts
use `title.search` keyword filters because concept IDs became less
reliable for this project.

Initial sector keywords should follow the existing scripts:

- `biotech_pharma`: oncology, genomics, immunotherapy, drug discovery,
  CRISPR
- `ai_tech`: artificial intelligence, machine learning, deep learning,
  large language model, neural network
- `clean_energy`: photovoltaic, renewable energy, battery storage,
  wind energy, solar cell
- `semiconductors`: semiconductor, integrated circuit, transistor, VLSI,
  chip fabrication

---

## 3. Page Structure

The app has three pages. At any time the user is looking at one selected
sector. There is no side-by-side sector comparison page.

### Page 1: Sector Overview

Purpose: a fast, glanceable summary for the selected sector.

Top controls:

- Persistent sector selector.
- Default sector: AI/Tech.
- Last updated timestamp from the database.

Sector summary card:

- Sector label and ETF ticker.
- Current weekly publication z-score versus trailing 52-week baseline.
- Sparkline of the selected publication signal over the last 12 weeks.
- Latest ETF weekly return.
- Weekly evidence status from `analysis_results`.
- Viral radar status from `radar_signals`.

The summary card must not make unsupported directional claims. It should
use these labels:

- `No weekly evidence`: weekly analysis does not support a directional
  relationship.
- `Weekly signal elevated`: selected publication signal is above its
  historical surge threshold and the stored analysis supports showing the
  relevant historical result.
- `Viral paper detected`: at least one recent paper passed the nightly
  viral threshold.
- `Insufficient data`: not enough publication or market data for a
  reliable classification.

Viral radar feed:

- Shows papers detected in the last 5 days for the selected sector.
- Each row displays title, DOI, publication date, CAS, Reddit hits,
  Wikipedia hits, citation velocity, detection lag, and historical CAR+5
  context.
- Directional language must be framed as historical event-study context:
  "Historically, viral papers in this sector had mean CAR+5 of X%."
- Do not label individual papers as certain bullish or bearish predictions.

Required row actions:

- Link to DOI when DOI is present.
- Expand row to show event-study details and paper metadata.

### Page 2: Deep Analysis

Purpose: show the statistical evidence behind the selected sector.

The page has two tabs.

#### Tab A: Publication Momentum

This tab follows `Logic_test/03_analysis_modular.ipynb` and the modular
code in `Logic_test`.

Controls:

- Sector selector.
- Signal selector:
  - `pub_deviation`: deviation from median.
  - `pub_zscore`: 52-week rolling z-score.
  - `pub_4w_dev`: 4-week rolling sum deviation.

Charts and panels:

1. **Publication Signal Over Time**
   - Weekly selected signal.
   - Surge events marked as vertical lines.
   - 75th percentile surge threshold as a dotted line.

2. **Rolling Correlation**
   - Rolling 52-week Pearson correlation between signal at time T and
     abnormal return at T + best lag.
   - Positive and negative periods visually distinguished.

3. **Lag Correlation**
   - Lags from -4 to +12 weeks when available in stored results.
   - Bar height is Pearson r.
   - Color by significance:
     - Bonferroni significant.
     - p < 0.05 but not Bonferroni.
     - not significant.

4. **CAR After Publication Surges**
   - Mean CAR for +1, +2, +4, +8, and +12 week windows.
   - Error bars show 95% confidence intervals.
   - Bars colored by stored significance flag.

5. **Granger Result Panel**
   - Shows F-statistic and p-value for lags 1 through 6.
   - Best lag highlighted.
   - Interpretation should say "predictive precedence", not true
     causality.
   - Uses weekly abnormal returns/log returns as in the notebook method,
     not viral-paper CAR.

6. **Stationarity Warning**
   - If selected signal ADF p > 0.05, show a warning that Granger results
     may be unreliable.

Do not hardcode any claim such as "AI/Tech is the only significant
sector." Read all claims from the seeded `analysis_results` table. The
current cached JSONs may change after reruns.

#### Tab B: Viral Paper Event Study

This tab follows `Short term analysis`.

Charts and panels:

1. **Sector CAR Curve**
   - Sector-specific version of `outputs/01_car_curves.png`.
   - Uses daily event windows from `event_windows`.
   - Shows mean CAR by trading day relative to viral-paper publication.

2. **CAR+5 Distribution**
   - Sector-specific distribution of CAR at day +5.
   - Shows n, mean, t-statistic, and p-value.

3. **Attention vs CAR+5**
   - Scatter of Reddit hits versus CAR+5.
   - Include regression line only if there are enough observations and
     Reddit hits have variance.
   - CAS can be shown as a secondary column, but the existing code found
     Reddit hits more interpretable for the scatter.

4. **Volatility Event Study**
   - Pre-event mean absolute AR over days -3 to -1.
   - Post-event mean absolute AR over days +1 to +5.
   - Paired t-test result.

5. **Robustness / Control Test**
   - Sector-specific version of the control test from
     `Short term analysis/outputs/11_control_test.png`.
   - Compare real viral-paper event dates with randomized control dates
     from the same sector's price history.
   - Include whether the real-vs-control comparison supports an
     event-driven interpretation.

### Page 3: Research Tool

Purpose: run the weekly publication-momentum analysis for a custom
scientific domain and ticker.

Inputs:

- Keyword/title terms: multi-value input, one search phrase per line.
  Example: `large language model`, `retrieval augmented generation`.
- Ticker: ETF or stock symbol validated with yfinance.
- Date range:
  - Minimum start date: 2015-01-01.
  - Minimum range: 2 years.
  - Maximum end date: today.

Run behavior:

- Validate inputs.
- Fetch publication counts from OpenAlex using keyword/title search.
- Fetch market data from yfinance.
- Fetch SPY for market baseline.
- Compute weekly abnormal returns.
- Compute `pub_deviation`, `pub_zscore`, and `pub_4w_dev`.
- Run the same weekly analysis functions used for known sectors.
- Save result JSON and run metadata to SQLite.

Results:

- Same charts as Page 2, Tab A.
- Custom research runs do not run the viral-paper radar unless that is
  added later as a separate feature.

Persistence:

- Each completed run receives a UUID.
- URL format: `/research/{run_id}`.
- The frontend polls status while the background task runs.

---

## 4. Viral Radar Logic

This section is authoritative for the nightly radar implementation.

### Historical Viral Event Construction

The existing historical pipeline does this:

1. Fetch recent papers for each sector using OpenAlex `title.search`.
2. Keep papers with DOI.
3. Apply recency-aware citation filtering:
   - Papers under 30 days old: keep all.
   - Papers 30-90 days old: keep if `cited_by_count >= 3`.
   - Papers over 90 days old: keep if `cited_by_count >= 10`.
4. Score attention:
   - `reddit_hits`: DOI search on Reddit within 14 days after publication.
   - `wiki_hits`: DOI search in Wikipedia, age-gated to papers under 60
     days old.
   - `cit_velocity`: `cited_by_count / age_days`.
5. Compute CAS:

```python
CAS = reddit_hits * 25 + wiki_hits * 10 + cit_velocity
```

6. Flag historical viral events as the top 5% CAS per sector, using the
   existing `flag_viral_events()` behavior.
7. Build daily event windows around each event:
   - Pre window: 3 trading days.
   - Post window: 5 trading days.
   - Event day: nearest trading day to publication date, within 5 days.
8. Compute daily abnormal returns with a market model:

```text
R_sector,t = alpha + beta * R_SPY,t + epsilon_t
AR_t = R_sector,t - (alpha_hat + beta_hat * R_SPY,t)
CAR_t = cumulative sum of AR_t inside the event window
```

The beta estimation window is trading days -200 to -20 before the event,
with at least 60 observations required. If not enough observations exist,
the code falls back to a simple mean baseline.

### Nightly Alert Threshold

Do not confuse these two thresholds:

- **Historical viral event definition**: top 5% CAS per sector.
- **Nightly radar alert threshold**: 30th percentile of CAS scores among
  that sector's historical top-5% viral events.

In other words:

1. Use the historical pipeline to identify each sector's top-5% CAS viral
   event set.
2. For each sector, compute:

```python
nightly_threshold = historical_viral_events[sector].cas.quantile(0.30)
```

3. A newly detected nightly paper triggers a radar alert if:

```python
new_paper.cas >= nightly_threshold
```

This keeps the nightly threshold aligned with the historical viral-event
population without changing the historical top-5% event definition.

### Nightly Job Flow

The nightly job should run once per day, after market and publication
data refresh.

```text
1. Fetch papers published yesterday from OpenAlex for all configured sectors.
2. Store new papers in SQLite.
3. Score all unscored papers in the DB using Reddit DOI, Wikipedia DOI,
   and citation velocity.
4. Compute or load each sector's historical nightly threshold.
5. Flag papers whose CAS is above the sector threshold.
6. Attach historical CAR+5 context from the sector event study.
7. Insert alert rows into radar_signals.
```

Signal output:

- Binary: viral paper detected in sector.
- Quantified: historical mean CAR+5, n events, p-value, and days
  remaining in the 5-trading-day event window.
- Caveat: this is pattern-based historical context, not causal prediction.

### Viral Paper Data Quality Filters

Before inserting candidate papers into `papers`, `viral_events`, or
`radar_signals`, apply filters to avoid journal-level records and other
non-paper artifacts.

Required filters:

- Require a DOI.
- Require a non-empty title.
- Require publication date.
- Drop records where OpenAlex `type` is available and not one of:
  `article`, `preprint`, `posted-content`, `review`, or `proceedings-article`.
- If `primary_location.source.display_name` is available, drop records
  where normalized title equals normalized source display name.
- Drop obvious venue-title records. Initial examples from the cached data:
  - `International Journal of Oncology`
  - `Journal of Gastrointestinal Oncology`
  - `Oncology Reports`
  - `Oncology Letters`
  - `Molecular and Clinical Oncology`
  - `International Journal of Artificial Intelligence & Applications`
  - `Semiconductor Physics, Quantum Electronics and Optoelectronics`
- Drop records where the title starts with generic venue patterns such as
  `Journal of ` or `International Journal of ` unless OpenAlex metadata
  strongly identifies it as an article.

Implementation note: prefer metadata-based filters first. Use the explicit
title denylist only as a backup for cached data that lacks rich metadata.

---

## 5. Data Architecture

SQLite is the app data store. The file is `data/litmarket.db` and is
mounted as a Docker volume.

### Tables

```sql
sectors (
    sector        TEXT PRIMARY KEY,
    label         TEXT NOT NULL,
    weekly_ticker TEXT NOT NULL,
    viral_ticker  TEXT NOT NULL,
    keywords_json TEXT NOT NULL,
    created_at    DATETIME NOT NULL,
    updated_at    DATETIME NOT NULL
);

market_daily (
    id         INTEGER PRIMARY KEY,
    sector     TEXT NOT NULL,
    ticker     TEXT NOT NULL,
    date       DATE NOT NULL,
    open       REAL,
    high       REAL,
    low        REAL,
    close      REAL NOT NULL,
    volume     INTEGER,
    log_return REAL,
    created_at DATETIME NOT NULL,
    UNIQUE(ticker, date)
);

spy_daily (
    id         INTEGER PRIMARY KEY,
    date       DATE NOT NULL UNIQUE,
    open       REAL,
    high       REAL,
    low        REAL,
    close      REAL NOT NULL,
    volume     INTEGER,
    log_return REAL,
    created_at DATETIME NOT NULL
);

publications_weekly (
    id            INTEGER PRIMARY KEY,
    sector        TEXT NOT NULL,
    week_start    DATE NOT NULL,
    pub_count     INTEGER NOT NULL,
    pub_deviation REAL,
    pub_zscore    REAL,
    pub_4w_dev    REAL,
    created_at    DATETIME NOT NULL,
    UNIQUE(sector, week_start)
);

abnormal_returns_weekly (
    id              INTEGER PRIMARY KEY,
    sector          TEXT NOT NULL,
    ticker          TEXT NOT NULL,
    week_start      DATE NOT NULL,
    log_return      REAL NOT NULL,
    spy_return      REAL,
    abnormal_return REAL,
    alpha           REAL,
    beta            REAL,
    r_squared       REAL,
    created_at      DATETIME NOT NULL,
    UNIQUE(sector, ticker, week_start)
);

abnormal_returns_daily (
    id              INTEGER PRIMARY KEY,
    sector          TEXT NOT NULL,
    ticker          TEXT NOT NULL,
    date            DATE NOT NULL,
    log_return      REAL NOT NULL,
    spy_return      REAL,
    abnormal_return REAL,
    alpha           REAL,
    beta            REAL,
    r_squared       REAL,
    method          TEXT NOT NULL,
    created_at      DATETIME NOT NULL,
    UNIQUE(sector, ticker, date, method)
);

analysis_results (
    id          INTEGER PRIMARY KEY,
    sector      TEXT NOT NULL,
    signal_col  TEXT NOT NULL,
    result_json TEXT NOT NULL,
    computed_at DATETIME NOT NULL,
    UNIQUE(sector, signal_col)
);

papers (
    id                  INTEGER PRIMARY KEY,
    paper_id            TEXT,
    doi                 TEXT,
    title               TEXT NOT NULL,
    publication_date    DATE NOT NULL,
    sector              TEXT NOT NULL,
    keyword             TEXT,
    openalex_type       TEXT,
    source_display_name TEXT,
    cited_by_count      INTEGER,
    is_filtered_out     INTEGER NOT NULL DEFAULT 0,
    filter_reason       TEXT,
    detected_date       DATE,
    created_at          DATETIME NOT NULL,
    updated_at          DATETIME NOT NULL,
    UNIQUE(doi, sector)
);

attention_scores (
    id              INTEGER PRIMARY KEY,
    paper_id_fk     INTEGER NOT NULL,
    reddit_hits     INTEGER NOT NULL DEFAULT 0,
    wiki_hits       INTEGER NOT NULL DEFAULT 0,
    citation_count  INTEGER NOT NULL DEFAULT 0,
    cit_velocity    REAL NOT NULL DEFAULT 0,
    age_days        INTEGER,
    cas             REAL NOT NULL DEFAULT 0,
    scored_at       DATETIME NOT NULL,
    FOREIGN KEY(paper_id_fk) REFERENCES papers(id),
    UNIQUE(paper_id_fk)
);

viral_events (
    id              INTEGER PRIMARY KEY,
    paper_id_fk     INTEGER NOT NULL,
    sector          TEXT NOT NULL,
    event_date      DATE NOT NULL,
    cas             REAL NOT NULL,
    threshold_type  TEXT NOT NULL, -- historical_top_5pct or nightly_30pct_of_historical_viral
    threshold_value REAL NOT NULL,
    is_historical   INTEGER NOT NULL DEFAULT 0,
    created_at      DATETIME NOT NULL,
    FOREIGN KEY(paper_id_fk) REFERENCES papers(id)
);

event_windows (
    id           INTEGER PRIMARY KEY,
    viral_event_id INTEGER NOT NULL,
    sector       TEXT NOT NULL,
    ticker       TEXT NOT NULL,
    event_date   DATE NOT NULL,
    date         DATE NOT NULL,
    day_relative INTEGER NOT NULL,
    log_return   REAL,
    spy_return   REAL,
    alpha_hat    REAL,
    beta_hat     REAL,
    r_squared    REAL,
    ar           REAL,
    car          REAL,
    method       TEXT NOT NULL,
    FOREIGN KEY(viral_event_id) REFERENCES viral_events(id),
    UNIQUE(viral_event_id, date)
);

viral_event_results (
    id          INTEGER PRIMARY KEY,
    sector      TEXT NOT NULL,
    result_json TEXT NOT NULL,
    computed_at DATETIME NOT NULL,
    UNIQUE(sector)
);

radar_thresholds (
    id              INTEGER PRIMARY KEY,
    sector          TEXT NOT NULL UNIQUE,
    threshold_value REAL NOT NULL,
    source_quantile REAL NOT NULL DEFAULT 0.30,
    source_event_set TEXT NOT NULL DEFAULT 'historical_top_5pct_cas',
    n_source_events INTEGER NOT NULL,
    computed_at     DATETIME NOT NULL
);

radar_signals (
    id                 INTEGER PRIMARY KEY,
    paper_id_fk        INTEGER NOT NULL,
    sector             TEXT NOT NULL,
    signal_date        DATE NOT NULL,
    publication_date   DATE NOT NULL,
    detection_lag_days INTEGER,
    cas                REAL NOT NULL,
    threshold_value    REAL NOT NULL,
    historical_car_5d  REAL,
    historical_n       INTEGER,
    historical_pval    REAL,
    days_remaining     INTEGER,
    status             TEXT NOT NULL, -- active, expired, dismissed
    created_at         DATETIME NOT NULL,
    FOREIGN KEY(paper_id_fk) REFERENCES papers(id),
    UNIQUE(paper_id_fk, signal_date)
);

research_runs (
    id              TEXT PRIMARY KEY,
    keywords_json   TEXT NOT NULL,
    ticker          TEXT NOT NULL,
    date_start      DATE NOT NULL,
    date_end        DATE NOT NULL,
    status          TEXT NOT NULL, -- pending, running, complete, failed
    progress_json   TEXT,
    result_json     TEXT,
    error_message   TEXT,
    submitted_at    DATETIME NOT NULL,
    completed_at    DATETIME
);
```

---

## 6. API Contract

The backend is Flask. The frontend is React.

Required endpoints:

```text
GET /api/sectors
GET /api/sectors/{sector}/overview
GET /api/sectors/{sector}/analysis?signal=pub_zscore
GET /api/sectors/{sector}/viral-analysis
GET /api/viral?sector={sector}&days=5
POST /api/research/run
GET /api/research/{run_id}/status
GET /api/research/{run_id}/results
```

`GET /api/sectors/{sector}/overview` returns:

```json
{
  "sector": "ai_tech",
  "label": "AI/Tech",
  "weekly_ticker": "BOTZ",
  "viral_ticker": "AIQ",
  "last_updated": "2026-05-01T02:00:00",
  "weekly": {
    "current_signal_col": "pub_zscore",
    "current_signal_value": 1.23,
    "latest_weekly_return": 0.012,
    "evidence_status": "No weekly evidence",
    "interpretation": "No statistically significant weekly relationship is currently supported by the stored analysis."
  },
  "viral": {
    "active_signal_count": 2,
    "threshold_value": 25.04,
    "historical_car_5d": 0.007,
    "historical_n": 33,
    "historical_pval": 0.12,
    "interpretation": "Two recent papers crossed the sector's nightly viral threshold. Historical CAR+5 context is shown below."
  }
}
```

`GET /api/viral` returns rows:

```json
[
  {
    "title": "Paper title",
    "doi": "10.xxxx/example",
    "sector": "ai_tech",
    "publication_date": "2026-04-30",
    "signal_date": "2026-05-01",
    "cas": 50.1,
    "reddit_hits": 2,
    "wiki_hits": 0,
    "cit_velocity": 0.1,
    "detection_lag_days": 1,
    "historical_car_5d": 0.007,
    "historical_n": 33,
    "historical_pval": 0.12,
    "days_remaining": 4
  }
]
```

---

## 7. Engineering Structure

Target repo layout:

```text
litmarket/
  backend/
    app.py
    scheduler.py
    database.py
    api/
      sectors.py
      viral.py
      research.py
    pipelines/
      weekly_openalex.py
      market.py
      weekly_analysis.py
      viral_openalex.py
      viral_attention.py
      viral_event_study.py
      nightly_radar.py
      validate.py
  frontend/
    public/
      index.html
    src/
      App.jsx
      pages/
        Overview.jsx
        Analysis.jsx
        Research.jsx
      components/
        SectorSelector.jsx
        SectorCard.jsx
        ViralFeed.jsx
        SignalChart.jsx
        RollingCorrChart.jsx
        LagCorrChart.jsx
        CARChart.jsx
        GrangerTable.jsx
        StationarityWarning.jsx
        ViralCarCurve.jsx
        ViralDistribution.jsx
        ViralControlTest.jsx
        ResearchForm.jsx
        ProgressBar.jsx
  analysis/
    stats.py
    signals.py
    runner.py
  data/
    litmarket.db
  docs/
    ANALYSIS_METHODOLOGY.md
    WEBAPP_DESIGN.md
  Dockerfile
  docker-compose.yml
  requirements.txt
  README.md
```

Existing code should be adapted rather than rewritten from scratch.

Mapping:

| Existing file | Web app module |
|---|---|
| `Logic_test/stats.py` | `analysis/stats.py` |
| `Logic_test/signals.py` | `analysis/signals.py` |
| `Logic_test/runner.py` | `analysis/runner.py`, modified to return JSON and save to DB |
| `Logic_test/aggregate_market.py` | `backend/pipelines/market.py` weekly aggregation reference |
| `Short term analysis/fetch_openalex.py` | `backend/pipelines/viral_openalex.py` |
| `Short term analysis/filter_papers.py` | `backend/pipelines/viral_openalex.py` or `viral_filters.py` |
| `Short term analysis/fetch_attention.py` | `backend/pipelines/viral_attention.py` |
| `Short term analysis/clean_align.py` | `backend/pipelines/viral_event_study.py` |
| `Short term analysis/analysis.py` | `backend/pipelines/viral_event_study.py` result generation |
| `Short term analysis/robustness_check.py` | `backend/pipelines/viral_event_study.py` control-test generation |
| `nightly_radar_plan.md` | `backend/pipelines/nightly_radar.py` |

---

## 8. Technology Choices

| Component | Technology | Rationale |
|---|---|---|
| Backend API | Flask | Existing analysis is Python. |
| Frontend | React | Componentized chart-heavy UI. |
| Charts | Recharts | Lightweight and sufficient for line, bar, scatter, and table views. |
| Database | SQLite | Single-file persistence for interview deployment. |
| Scheduler | APScheduler | Simple nightly jobs inside backend process. |
| Containerization | Docker Compose | One-command local review. |

Docker Compose services:

- `backend`: Flask API, scheduler, SQLite access.
- `frontend`: React build served by nginx, proxies `/api/*` to backend.

For interview usability, ship a seeded SQLite database so reviewers can
open the app immediately without waiting for API calls. The scheduler can
refresh data after startup.

---

## 9. Evidence and Interpretation Rules

The app should be conservative.

Weekly publication momentum:

- Report Granger, correlation, and CAR results as exploratory evidence.
- Use "predictive precedence" for Granger, not "causality".
- If ADF fails for the selected signal, show the stationarity warning.
- Do not generate a directional statement unless the selected stored
  result supports it.
- Null results are displayed as findings.

Viral radar:

- A nightly alert means a paper crossed the sector threshold derived from
  historical top-5% CAS events.
- Historical CAR+5 is context, not a forecast.
- If historical CAR+5 is not significant, show the p-value and phrase the
  interpretation as weak/inconclusive.
- If the control test looks similar to the real event curve, warn that the
  effect may reflect market momentum rather than paper-specific impact.

Plain-English copy should sit next to every major chart, but avoid
overclaiming. The strongest acceptable language is:

> "Historically, events like this were followed by..."

Avoid:

> "This paper will move the ETF..."

---

## 10. Build Order

Recommended implementation sequence:

1. Create backend SQLite schema and seed loader from existing CSV/JSON
   outputs.
2. Build read-only API endpoints for sectors, weekly analysis, viral feed,
   and viral analysis.
3. Build React pages using seeded DB only.
4. Add nightly radar pipeline against SQLite.
5. Add custom research runs.
6. Add Docker Compose and README.
7. Add data-quality filters to the viral OpenAlex ingestion before any
   nightly alerts are generated.

This order gives a working demo early while keeping the more fragile API
fetching and background jobs isolated until the static app surfaces are
stable.
