"""Safely refresh LitMarket SQLite data in place.

This is intentionally incremental and non-destructive. It never deletes the
SQLite database. Expensive API work is opt-in/bounded where possible, and
source-refresh failures leave the existing database usable.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

from analysis.research_runner import VALID_SIGNALS, analyze_signal
from backend.database import get_connection, get_db_path, init_db
from backend.pipelines.clean_viral_cache import DEFAULT_OUTPUT_DIR, clean_cache
from backend.pipelines.nightly_radar import run_nightly_radar
from backend.pipelines.seed_database import (
    CLEANED_DIR,
    SECTORS,
    SHORT_CACHE_DIR,
    count_tables,
    seed_attention_scores,
    seed_event_windows,
    seed_papers,
    seed_radar_thresholds,
    seed_sectors,
    seed_viral_event_results,
    to_float,
)


SIGNAL_LABELS = {
    "pub_deviation": "Deviation from median",
    "pub_zscore": "Z-score (52w rolling)",
    "pub_4w_dev": "4-week rolling deviation",
}

OPENALEX_BASE = "https://api.openalex.org"
OPENALEX_SLEEP = 0.2
REQUEST_TIMEOUT = 30


def refresh_database(
    *,
    dry_run: bool = False,
    skip_sources: bool = False,
    skip_weekly: bool = False,
    skip_market: bool = False,
    skip_viral_seed: bool = True,
    skip_nightly_radar: bool = False,
    nightly_days: int = 1,
    nightly_max_pages: int = 1,
    nightly_max_attention_scores: int | None = None,
    nightly_skip_attention: bool = False,
    force_current_week: bool = False,
    market_start: str = "2016-01-01",
    market_end: str | None = None,
    max_weekly_weeks: int = 26,
    sectors: tuple[str, ...] = tuple(SECTORS.keys()),
) -> dict[str, Any]:
    """Refresh the local SQLite DB without deleting existing data."""
    init_db()
    timestamp = now_iso()
    market_end = market_end or date.today().isoformat()

    summary: dict[str, Any] = {
        "started_at": timestamp,
        "database": str(get_db_path()),
        "dry_run": dry_run,
        "source_refresh": {},
        "sqlite": {},
        "analysis_results": {},
        "nightly_radar": None,
    }

    with get_connection() as conn:
        if dry_run:
            summary["sqlite"]["sectors"] = conn.execute("SELECT COUNT(*) FROM sectors").fetchone()[0]
        else:
            summary["sqlite"]["sectors"] = seed_sectors(conn)
        if not skip_sources:
            summary["source_refresh"] = refresh_sqlite_sources(
                conn,
                dry_run=dry_run,
                skip_weekly=skip_weekly,
                skip_market=skip_market,
                force_current_week=force_current_week,
                market_start=market_start,
                market_end=market_end,
                max_weekly_weeks=max_weekly_weeks,
                sectors=sectors,
            )

        if dry_run:
            summary["sqlite"]["note"] = "dry-run: SQLite writes skipped"
            return finish_summary(summary)

        if not skip_market:
            summary["sqlite"]["abnormal_returns_weekly"] = recompute_weekly_abnormal_returns(conn, sectors)
        if not skip_weekly:
            summary["sqlite"]["publications_weekly"] = recompute_publication_signals(conn, sectors)
        summary["analysis_results"] = recompute_weekly_analysis(conn, sectors)

        if not skip_viral_seed:
            ensure_cleaned_viral_inputs()
            paper_ids = seed_papers(conn)
            summary["sqlite"]["papers"] = len(paper_ids)
            summary["sqlite"]["attention_scores"] = seed_attention_scores(conn, paper_ids)
            # Do not call seed_viral_events here: it appends historical events
            # and is intended for full seeded rebuilds. Refresh thresholds from
            # existing historical rows instead.
            summary["sqlite"]["event_windows"] = seed_event_windows_from_existing(conn)
            summary["sqlite"]["viral_event_results"] = seed_viral_event_results(conn)
            summary["sqlite"]["radar_thresholds"] = refresh_existing_radar_thresholds(conn)

        conn.commit()
        summary["sqlite"]["table_counts"] = count_tables(conn)

    if not skip_nightly_radar:
        summary["nightly_radar"] = run_nightly_radar(
            target_date=None,
            days=nightly_days,
            dry_run=False,
            max_pages=nightly_max_pages,
            skip_attention=nightly_skip_attention,
            max_attention_scores=nightly_max_attention_scores,
        )

    return finish_summary(summary)


def refresh_sqlite_sources(
    conn,
    *,
    dry_run: bool,
    skip_weekly: bool,
    skip_market: bool,
    force_current_week: bool,
    market_start: str,
    market_end: str,
    max_weekly_weeks: int,
    sectors: tuple[str, ...],
) -> dict[str, Any]:
    """Refresh source data directly into SQLite using SQLite as truth."""
    result: dict[str, Any] = {}
    target_week = monday_for(date.today()) if force_current_week else last_completed_week_start()
    if not skip_weekly:
        result["weekly"] = refresh_weekly_publications_sqlite(
            conn,
            sectors=sectors,
            target_week=target_week,
            max_weekly_weeks=max_weekly_weeks,
            dry_run=dry_run,
        )
    if not skip_market:
        result["market"] = refresh_market_sqlite(
            conn,
            sectors=sectors,
            market_start=market_start,
            market_end=market_end,
            dry_run=dry_run,
        )
    return result


def refresh_weekly_publications_sqlite(
    conn,
    *,
    sectors: tuple[str, ...],
    target_week: date,
    max_weekly_weeks: int,
    dry_run: bool,
) -> dict[str, Any]:
    summary: dict[str, Any] = {"target_week": target_week.isoformat(), "sectors": {}}
    sector_rows = sector_configs(conn)
    plans: dict[str, list[date]] = {}
    for sector in sectors:
        latest = latest_publication_week(conn, sector)
        start = monday_for(date(2016, 1, 1)) if latest is None else latest + timedelta(days=7)
        weeks = []
        current = start
        while current <= target_week:
            weeks.append(current)
            current += timedelta(days=7)
        plans[sector] = weeks
        summary["sectors"][sector] = {
            "latest_sqlite_week": latest.isoformat() if latest else None,
            "missing_weeks": len(weeks),
        }

    too_large = {
        sector: len(weeks)
        for sector, weeks in plans.items()
        if max_weekly_weeks >= 0 and len(weeks) > max_weekly_weeks
    }
    if too_large:
        raise RuntimeError(
            "Weekly OpenAlex refresh would fetch too many missing weeks from SQLite state: "
            f"{too_large}. Re-run with --max-weekly-weeks set higher, "
            "or use --skip-weekly to leave publication counts unchanged."
        )
    if dry_run:
        return summary

    timestamp = now_iso()
    for sector, weeks in plans.items():
        keywords = sector_rows[sector]["keywords"]
        for week_start in weeks:
            pub_count = count_openalex_title_week(keywords, week_start)
            conn.execute(
                """
                INSERT INTO publications_weekly (
                    sector, week_start, pub_count, created_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(sector, week_start) DO UPDATE SET
                    pub_count=excluded.pub_count,
                    created_at=excluded.created_at
                """,
                (sector, week_start.isoformat(), pub_count, timestamp),
            )
            time.sleep(OPENALEX_SLEEP)
    return summary


def refresh_market_sqlite(
    conn,
    *,
    sectors: tuple[str, ...],
    market_start: str,
    market_end: str,
    dry_run: bool,
) -> dict[str, Any]:
    configs = sector_configs(conn)
    tickers = sorted({configs[sector]["weekly_ticker"] for sector in sectors} | {"SPY"})
    latest_dates = {ticker: latest_market_date(conn, ticker) for ticker in tickers}
    summary = {
        "target_range": [market_start, market_end],
        "tickers": {
            ticker: {"latest_sqlite_date": latest_dates[ticker].isoformat() if latest_dates[ticker] else None}
            for ticker in tickers
        },
    }
    if dry_run:
        return summary

    import yfinance as yf

    for ticker in tickers:
        latest = latest_dates[ticker]
        start_date = parse_date(market_start)
        if latest is not None:
            start_date = max(start_date, latest - timedelta(days=10))
        raw = yf.download(
            ticker,
            start=start_date.isoformat(),
            end=market_end,
            auto_adjust=True,
            progress=False,
            threads=False,
        )
        if raw is None or raw.empty:
            summary["tickers"][ticker]["status"] = "no_rows_returned"
            continue
        daily = normalize_yfinance_single(raw, ticker)
        upsert_market_daily(conn, daily, ticker, configs)
        summary["tickers"][ticker]["downloaded_rows"] = int(len(daily))
    return summary


def recompute_publication_signals(conn, sectors: tuple[str, ...]) -> int:
    updated = 0
    for sector in sectors:
        rows = conn.execute(
            """
            SELECT week_start, pub_count
            FROM publications_weekly
            WHERE sector = ?
            ORDER BY week_start
            """,
            (sector,),
        ).fetchall()
        if not rows:
            continue
        df = pd.DataFrame([dict(row) for row in rows])
        counts = df["pub_count"].astype(float)
        df["pub_deviation"] = counts - counts.median()
        rolling_mean = counts.rolling(52, min_periods=26).mean()
        rolling_std = counts.rolling(52, min_periods=26).std()
        df["pub_zscore"] = (counts - rolling_mean) / rolling_std
        rolling_sum = counts.rolling(4, min_periods=1).sum()
        df["pub_4w_dev"] = rolling_sum - rolling_sum.median()
        for _, row in df.iterrows():
            conn.execute(
                """
                UPDATE publications_weekly
                SET pub_deviation = ?, pub_zscore = ?, pub_4w_dev = ?
                WHERE sector = ? AND week_start = ?
                """,
                (
                    clean_float(row["pub_deviation"]),
                    clean_float(row["pub_zscore"]),
                    clean_float(row["pub_4w_dev"]),
                    sector,
                    row["week_start"][:10],
                ),
            )
            updated += 1
    return updated


def recompute_weekly_abnormal_returns(conn, sectors: tuple[str, ...]) -> int:
    configs = sector_configs(conn)
    timestamp = now_iso()
    updated = 0
    for sector in sectors:
        ticker = configs[sector]["weekly_ticker"]
        rows = conn.execute(
            """
            SELECT date, log_return
            FROM market_daily
            WHERE ticker = ?
            ORDER BY date
            """,
            (ticker,),
        ).fetchall()
        spy_rows = conn.execute(
            """
            SELECT date, log_return
            FROM spy_daily
            ORDER BY date
            """
        ).fetchall()
        sector_weekly = weekly_from_daily_rows(rows, "log_return")
        spy_weekly = weekly_from_daily_rows(spy_rows, "spy_return")
        merged = pd.merge(sector_weekly, spy_weekly, on="week_start", how="inner").dropna()
        if len(merged) < 2:
            continue
        alpha, beta, r_squared = fit_ols(
            [(row.spy_return, row.log_return) for row in merged.itertuples()]
        )
        for row in merged.itertuples():
            abnormal = row.log_return - (alpha + beta * row.spy_return)
            conn.execute(
                """
                INSERT INTO abnormal_returns_weekly (
                    sector, ticker, week_start, log_return, spy_return,
                    abnormal_return, alpha, beta, r_squared, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sector, ticker, week_start) DO UPDATE SET
                    log_return=excluded.log_return,
                    spy_return=excluded.spy_return,
                    abnormal_return=excluded.abnormal_return,
                    alpha=excluded.alpha,
                    beta=excluded.beta,
                    r_squared=excluded.r_squared,
                    created_at=excluded.created_at
                """,
                (
                    sector,
                    ticker,
                    row.week_start,
                    row.log_return,
                    row.spy_return,
                    abnormal,
                    alpha,
                    beta,
                    r_squared,
                    timestamp,
                ),
            )
            updated += 1
    return updated


def sector_configs(conn) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT sector, weekly_ticker, viral_ticker, keywords_json
        FROM sectors
        """
    ).fetchall()
    return {
        row["sector"]: {
            "weekly_ticker": row["weekly_ticker"],
            "viral_ticker": row["viral_ticker"],
            "keywords": json.loads(row["keywords_json"] or "[]"),
        }
        for row in rows
    }


def latest_publication_week(conn, sector: str) -> date | None:
    row = conn.execute(
        "SELECT MAX(week_start) AS latest FROM publications_weekly WHERE sector = ?",
        (sector,),
    ).fetchone()
    return parse_date(row["latest"]) if row and row["latest"] else None


def latest_market_date(conn, ticker: str) -> date | None:
    table = "spy_daily" if ticker == "SPY" else "market_daily"
    row = conn.execute(
        f"SELECT MAX(date) AS latest FROM {table} WHERE ticker = ?" if ticker != "SPY" else "SELECT MAX(date) AS latest FROM spy_daily",
        (ticker,) if ticker != "SPY" else (),
    ).fetchone()
    return parse_date(row["latest"]) if row and row["latest"] else None


def count_openalex_title_week(keywords: list[str], week_start: date) -> int:
    title_query = "|".join(clean_openalex_value(keyword) for keyword in keywords)
    params = {
        "filter": (
            f"title.search:{title_query},"
            f"from_publication_date:{week_start.isoformat()},"
            f"to_publication_date:{(week_start + timedelta(days=6)).isoformat()}"
        ),
        "per-page": 1,
        "select": "id",
        "mailto": "research@litmarket.io",
    }
    response = requests.get(f"{OPENALEX_BASE}/works", params=params, timeout=REQUEST_TIMEOUT)
    if not response.ok:
        raise RuntimeError(f"OpenAlex {response.status_code}: {response.text[:180]}")
    return int(response.json().get("meta", {}).get("count", 0))


def normalize_yfinance_single(raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
    df = raw.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    close_col = "Adj Close" if "Adj Close" in df.columns else "Close"
    out = pd.DataFrame(
        {
            "date": pd.to_datetime(df.index).tz_localize(None),
            "open": df["Open"],
            "high": df["High"],
            "low": df["Low"],
            "close": df[close_col],
            "volume": df["Volume"] if "Volume" in df.columns else np.nan,
        }
    ).dropna(subset=["close"])
    out = out.sort_values("date").reset_index(drop=True)
    out["log_return"] = np.log(out["close"] / out["close"].shift(1))
    out["ticker"] = ticker
    return out


def upsert_market_daily(
    conn,
    daily: pd.DataFrame,
    ticker: str,
    configs: dict[str, dict[str, Any]],
) -> None:
    timestamp = now_iso()
    if ticker == "SPY":
        for row in daily.itertuples():
            conn.execute(
                """
                INSERT INTO spy_daily (date, open, high, low, close, volume, log_return, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    log_return=excluded.log_return,
                    created_at=excluded.created_at
                """,
                (
                    row.date.date().isoformat(),
                    clean_float(row.open),
                    clean_float(row.high),
                    clean_float(row.low),
                    clean_float(row.close),
                    int(row.volume) if not pd.isna(row.volume) else None,
                    clean_float(row.log_return),
                    timestamp,
                ),
            )
        return

    ticker_to_sector = {
        cfg["weekly_ticker"]: sector
        for sector, cfg in configs.items()
    }
    sector = ticker_to_sector[ticker]
    for row in daily.itertuples():
        conn.execute(
            """
            INSERT INTO market_daily (
                sector, ticker, date, open, high, low, close,
                volume, log_return, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker, date) DO UPDATE SET
                sector=excluded.sector,
                open=excluded.open,
                high=excluded.high,
                low=excluded.low,
                close=excluded.close,
                volume=excluded.volume,
                log_return=excluded.log_return,
                created_at=excluded.created_at
            """,
            (
                sector,
                ticker,
                row.date.date().isoformat(),
                clean_float(row.open),
                clean_float(row.high),
                clean_float(row.low),
                clean_float(row.close),
                int(row.volume) if not pd.isna(row.volume) else None,
                clean_float(row.log_return),
                timestamp,
            ),
        )


def weekly_from_daily_rows(rows: list[Any], return_col: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["week_start", return_col])
    df = pd.DataFrame([dict(row) for row in rows])
    df["date"] = pd.to_datetime(df["date"])
    df["week_start"] = (df["date"] - pd.to_timedelta(df["date"].dt.weekday, unit="D")).dt.normalize()
    weekly = (
        df.groupby("week_start", as_index=False)
        .agg(close_return=("log_return", "sum"))
        .sort_values("week_start")
    )
    weekly[return_col] = weekly["close_return"]
    weekly["week_start"] = weekly["week_start"].dt.date.astype(str)
    return weekly[["week_start", return_col]]


def fit_ols(pairs: list[tuple[float, float]]) -> tuple[float, float, float]:
    pairs = [(x, y) for x, y in pairs if x is not None and y is not None]
    if len(pairs) < 2:
        return 0.0, 0.0, 0.0
    xs = [x for x, _ in pairs]
    ys = [y for _, y in pairs]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    var_x = sum((x - mean_x) ** 2 for x in xs)
    if var_x == 0:
        return mean_y, 0.0, 0.0
    cov = sum((x - mean_x) * (y - mean_y) for x, y in pairs)
    beta = cov / var_x
    alpha = mean_y - beta * mean_x
    ss_tot = sum((y - mean_y) ** 2 for y in ys)
    ss_res = sum((y - (alpha + beta * x)) ** 2 for x, y in pairs)
    r_squared = 1 - (ss_res / ss_tot) if ss_tot else 0.0
    return alpha, beta, r_squared


def monday_for(day: date) -> date:
    return day - timedelta(days=day.weekday())


def last_completed_week_start(today: date | None = None) -> date:
    today = today or date.today()
    return monday_for(today) - timedelta(days=7)


def parse_date(value: str) -> date:
    return datetime.strptime(value[:10], "%Y-%m-%d").date()


def clean_openalex_value(value: str) -> str:
    return " ".join(str(value).replace(",", " ").split())


def clean_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(number) or np.isinf(number):
        return None
    return number


def recompute_weekly_analysis(conn, sectors: tuple[str, ...]) -> dict[str, Any]:
    """Recompute analysis_results from current SQLite weekly rows."""
    timestamp = now_iso()
    summary: dict[str, Any] = {}
    for sector in sectors:
        rows = conn.execute(
            """
            SELECT p.week_start, p.pub_count, p.pub_deviation, p.pub_zscore,
                   p.pub_4w_dev, r.ticker, r.log_return, r.spy_return,
                   r.abnormal_return
            FROM publications_weekly p
            JOIN abnormal_returns_weekly r
              ON r.sector = p.sector AND r.week_start = p.week_start
            WHERE p.sector = ?
            ORDER BY p.week_start
            """,
            (sector,),
        ).fetchall()
        df = pd.DataFrame([dict(row) for row in rows])
        if df.empty:
            summary[sector] = {"status": "skipped", "reason": "no overlapping weekly rows"}
            continue
        df["week_start"] = pd.to_datetime(df["week_start"])
        ticker = str(df["ticker"].dropna().iloc[0]) if df["ticker"].notna().any() else ""
        sector_summary = {}
        for signal in VALID_SIGNALS:
            try:
                result = analyze_signal(df, signal, ticker)
                result["sector"] = sector
                result["label"] = SIGNAL_LABELS.get(signal, signal)
            except Exception as exc:
                sector_summary[signal] = {"status": "failed", "error": str(exc)}
                continue
            conn.execute(
                """
                INSERT INTO analysis_results (sector, signal_col, result_json, computed_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(sector, signal_col) DO UPDATE SET
                    result_json=excluded.result_json,
                    computed_at=excluded.computed_at
                """,
                (sector, signal, json.dumps(result), timestamp),
            )
            sector_summary[signal] = {
                "status": "updated",
                "n_obs": result.get("n_obs"),
                "date_range": result.get("date_range"),
            }
        summary[sector] = sector_summary
    return summary


def ensure_cleaned_viral_inputs() -> None:
    required = [
        CLEANED_DIR / "filtered_papers_clean.csv",
        CLEANED_DIR / "attention_scores_clean.csv",
        CLEANED_DIR / "event_windows_clean.csv",
        CLEANED_DIR / "event_windows_complete_day5_clean.csv",
    ]
    if all(path.exists() for path in required):
        return
    clean_cache(SHORT_CACHE_DIR, DEFAULT_OUTPUT_DIR)


def seed_event_windows_from_existing(conn) -> int:
    """Upsert cleaned windows only for historical events already in SQLite."""
    rows = read_clean_csv(CLEANED_DIR / "event_windows_clean.csv")
    event_ids = existing_event_ids_by_identity(conn)
    ticker_by_sector = {sector: cfg["viral_ticker"] for sector, cfg in SECTORS.items()}
    inserted = 0
    for row in rows:
        sector = row.get("sector")
        event_id = event_ids.get(event_key(row))
        if event_id is None or sector not in SECTORS:
            continue
        conn.execute(
            """
            INSERT INTO event_windows (
                viral_event_id, sector, ticker, event_date, date,
                day_relative, log_return, spy_return, alpha_hat, beta_hat,
                r_squared, ar, car, method
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(viral_event_id, date) DO UPDATE SET
                day_relative=excluded.day_relative,
                log_return=excluded.log_return,
                spy_return=excluded.spy_return,
                alpha_hat=excluded.alpha_hat,
                beta_hat=excluded.beta_hat,
                r_squared=excluded.r_squared,
                ar=excluded.ar,
                car=excluded.car,
                method=excluded.method
            """,
            (
                event_id,
                sector,
                ticker_by_sector[sector],
                row["event_date"][:10],
                row["date"][:10],
                to_int(row.get("day_relative")),
                to_float(row.get("log_return")),
                to_float(row.get("spy_return")),
                to_float(row.get("alpha_hat")),
                to_float(row.get("beta_hat")),
                to_float(row.get("r_squared")),
                to_float(row.get("AR")),
                to_float(row.get("CAR")),
                row.get("method") or "market_model_OLS",
            ),
        )
        inserted += 1
    return inserted


def refresh_existing_radar_thresholds(conn) -> int:
    rows = conn.execute(
        """
        SELECT sector, cas
        FROM viral_events
        WHERE is_historical = 1
        ORDER BY sector, cas
        """
    ).fetchall()
    cas_by_sector: dict[str, list[float]] = {}
    for row in rows:
        cas_by_sector.setdefault(row["sector"], []).append(float(row["cas"]))
    seed_radar_thresholds(conn, cas_by_sector)
    return len(cas_by_sector)


def existing_event_ids_by_identity(conn) -> dict[tuple[str, str, str], int]:
    rows = conn.execute(
        """
        SELECT v.id, p.doi, v.sector, v.event_date
        FROM viral_events v
        JOIN papers p ON p.id = v.paper_id_fk
        WHERE v.is_historical = 1
        """
    ).fetchall()
    return {
        (norm_doi(row["doi"]), row["sector"], row["event_date"][:10]): int(row["id"])
        for row in rows
    }


def event_key(row: dict[str, str]) -> tuple[str, str, str]:
    return (
        norm_doi(row.get("doi")),
        str(row.get("sector") or ""),
        str(row.get("event_date") or row.get("publication_date") or "")[:10],
    )


def read_clean_csv(path: Path) -> list[dict[str, str]]:
    import csv

    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def norm_doi(value: object) -> str:
    doi = str(value or "").strip()
    doi = doi.removeprefix("https://doi.org/")
    doi = doi.removeprefix("http://doi.org/")
    return doi.lower()


def to_int(value: object) -> int | None:
    number = to_float(value)
    return int(number) if number is not None else None


def finish_summary(summary: dict[str, Any]) -> dict[str, Any]:
    summary["completed_at"] = now_iso()
    return summary


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Show planned refreshes without SQLite writes.")
    parser.add_argument("--skip-sources", action="store_true", help="Do not refresh source CSV files before SQLite updates.")
    parser.add_argument("--skip-weekly", action="store_true", help="Skip publication weekly count refresh and DB upsert.")
    parser.add_argument("--skip-market", action="store_true", help="Skip market data refresh and DB upsert.")
    parser.add_argument(
        "--refresh-viral-seed",
        action="store_true",
        help="Also refresh cached historical viral support files if Build/data/cleaned is present. Off by default in Docker.",
    )
    parser.add_argument("--skip-nightly-radar", action="store_true", help="Skip the incremental nightly radar job.")
    parser.add_argument("--nightly-days", type=int, default=1, help="Publication days for nightly radar lookback.")
    parser.add_argument("--nightly-max-pages", type=int, default=1, help="OpenAlex pages per keyword for nightly radar.")
    parser.add_argument(
        "--nightly-max-attention-scores",
        type=int,
        default=-1,
        help="Maximum unscored papers to score per sector during nightly radar. Use -1 for no cap.",
    )
    parser.add_argument("--nightly-skip-attention", action="store_true", help="Insert nightly papers without scoring attention.")
    parser.add_argument("--force-current-week", action="store_true", help="Include the current incomplete publication week.")
    parser.add_argument("--market-start", default="2016-01-01")
    parser.add_argument("--market-end", default=date.today().isoformat())
    parser.add_argument(
        "--max-weekly-weeks",
        type=int,
        default=26,
        help="Abort if any sector needs more than this many missing weekly OpenAlex fetches. Use -1 for no cap.",
    )
    parser.add_argument("--sectors", nargs="+", choices=tuple(SECTORS.keys()), default=list(SECTORS.keys()))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = refresh_database(
        dry_run=args.dry_run,
        skip_sources=args.skip_sources,
        skip_weekly=args.skip_weekly,
        skip_market=args.skip_market,
        skip_viral_seed=not args.refresh_viral_seed,
        skip_nightly_radar=args.skip_nightly_radar,
        nightly_days=max(args.nightly_days, 1),
        nightly_max_pages=max(args.nightly_max_pages, 1),
        nightly_max_attention_scores=None if args.nightly_max_attention_scores < 0 else args.nightly_max_attention_scores,
        nightly_skip_attention=args.nightly_skip_attention,
        force_current_week=args.force_current_week,
        market_start=args.market_start,
        market_end=args.market_end,
        max_weekly_weeks=args.max_weekly_weeks,
        sectors=tuple(args.sectors),
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
