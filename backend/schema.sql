PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sectors (
    sector        TEXT PRIMARY KEY,
    label         TEXT NOT NULL,
    weekly_ticker TEXT NOT NULL,
    viral_ticker  TEXT NOT NULL,
    keywords_json TEXT NOT NULL,
    created_at    DATETIME NOT NULL,
    updated_at    DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS market_daily (
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

CREATE TABLE IF NOT EXISTS spy_daily (
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

CREATE TABLE IF NOT EXISTS publications_weekly (
    id            INTEGER PRIMARY KEY,
    sector        TEXT NOT NULL,
    week_start    DATE NOT NULL,
    pub_count     INTEGER NOT NULL,
    pub_deviation REAL,
    pub_zscore    REAL,
    pub_4w_dev    REAL,
    created_at    DATETIME NOT NULL,
    UNIQUE(sector, week_start),
    FOREIGN KEY(sector) REFERENCES sectors(sector)
);

CREATE TABLE IF NOT EXISTS abnormal_returns_weekly (
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
    UNIQUE(sector, ticker, week_start),
    FOREIGN KEY(sector) REFERENCES sectors(sector)
);

CREATE TABLE IF NOT EXISTS abnormal_returns_daily (
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
    UNIQUE(sector, ticker, date, method),
    FOREIGN KEY(sector) REFERENCES sectors(sector)
);

CREATE TABLE IF NOT EXISTS analysis_results (
    id          INTEGER PRIMARY KEY,
    sector      TEXT NOT NULL,
    signal_col  TEXT NOT NULL,
    result_json TEXT NOT NULL,
    computed_at DATETIME NOT NULL,
    UNIQUE(sector, signal_col),
    FOREIGN KEY(sector) REFERENCES sectors(sector)
);

CREATE TABLE IF NOT EXISTS papers (
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
    UNIQUE(doi, sector),
    FOREIGN KEY(sector) REFERENCES sectors(sector)
);

CREATE TABLE IF NOT EXISTS attention_scores (
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

CREATE TABLE IF NOT EXISTS viral_events (
    id              INTEGER PRIMARY KEY,
    paper_id_fk     INTEGER NOT NULL,
    sector          TEXT NOT NULL,
    event_date      DATE NOT NULL,
    cas             REAL NOT NULL,
    threshold_type  TEXT NOT NULL,
    threshold_value REAL NOT NULL,
    is_historical   INTEGER NOT NULL DEFAULT 0,
    created_at      DATETIME NOT NULL,
    FOREIGN KEY(paper_id_fk) REFERENCES papers(id),
    FOREIGN KEY(sector) REFERENCES sectors(sector)
);

CREATE TABLE IF NOT EXISTS event_windows (
    id             INTEGER PRIMARY KEY,
    viral_event_id INTEGER NOT NULL,
    sector         TEXT NOT NULL,
    ticker         TEXT NOT NULL,
    event_date     DATE NOT NULL,
    date           DATE NOT NULL,
    day_relative   INTEGER NOT NULL,
    log_return     REAL,
    spy_return     REAL,
    alpha_hat      REAL,
    beta_hat       REAL,
    r_squared      REAL,
    ar             REAL,
    car            REAL,
    method         TEXT NOT NULL,
    FOREIGN KEY(viral_event_id) REFERENCES viral_events(id),
    FOREIGN KEY(sector) REFERENCES sectors(sector),
    UNIQUE(viral_event_id, date)
);

CREATE TABLE IF NOT EXISTS viral_event_results (
    id          INTEGER PRIMARY KEY,
    sector      TEXT NOT NULL,
    result_json TEXT NOT NULL,
    computed_at DATETIME NOT NULL,
    UNIQUE(sector),
    FOREIGN KEY(sector) REFERENCES sectors(sector)
);

CREATE TABLE IF NOT EXISTS radar_thresholds (
    id               INTEGER PRIMARY KEY,
    sector           TEXT NOT NULL UNIQUE,
    threshold_value  REAL NOT NULL,
    source_quantile  REAL NOT NULL DEFAULT 0.30,
    source_event_set TEXT NOT NULL DEFAULT 'historical_top_5pct_cas',
    n_source_events  INTEGER NOT NULL,
    computed_at      DATETIME NOT NULL,
    FOREIGN KEY(sector) REFERENCES sectors(sector)
);

CREATE TABLE IF NOT EXISTS radar_signals (
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
    status             TEXT NOT NULL,
    created_at         DATETIME NOT NULL,
    FOREIGN KEY(paper_id_fk) REFERENCES papers(id),
    FOREIGN KEY(sector) REFERENCES sectors(sector),
    UNIQUE(paper_id_fk, signal_date)
);

CREATE TABLE IF NOT EXISTS research_runs (
    id             TEXT PRIMARY KEY,
    keywords_json  TEXT NOT NULL,
    ticker         TEXT NOT NULL,
    date_start     DATE NOT NULL,
    date_end       DATE NOT NULL,
    status         TEXT NOT NULL,
    progress_json  TEXT,
    result_json    TEXT,
    error_message  TEXT,
    submitted_at   DATETIME NOT NULL,
    completed_at   DATETIME
);

CREATE INDEX IF NOT EXISTS idx_market_daily_sector_date
    ON market_daily(sector, date);

CREATE INDEX IF NOT EXISTS idx_market_daily_ticker_date
    ON market_daily(ticker, date);

CREATE INDEX IF NOT EXISTS idx_spy_daily_date
    ON spy_daily(date);

CREATE INDEX IF NOT EXISTS idx_publications_weekly_sector_week
    ON publications_weekly(sector, week_start);

CREATE INDEX IF NOT EXISTS idx_abnormal_returns_weekly_sector_week
    ON abnormal_returns_weekly(sector, week_start);

CREATE INDEX IF NOT EXISTS idx_abnormal_returns_daily_sector_date
    ON abnormal_returns_daily(sector, date);

CREATE INDEX IF NOT EXISTS idx_papers_sector_publication_date
    ON papers(sector, publication_date);

CREATE INDEX IF NOT EXISTS idx_papers_doi
    ON papers(doi);

CREATE INDEX IF NOT EXISTS idx_attention_scores_cas
    ON attention_scores(cas);

CREATE INDEX IF NOT EXISTS idx_viral_events_sector_event_date
    ON viral_events(sector, event_date);

CREATE INDEX IF NOT EXISTS idx_event_windows_event_day
    ON event_windows(viral_event_id, day_relative);

CREATE INDEX IF NOT EXISTS idx_radar_signals_sector_signal_date
    ON radar_signals(sector, signal_date);

CREATE INDEX IF NOT EXISTS idx_research_runs_status
    ON research_runs(status);

