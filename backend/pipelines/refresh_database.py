"""Safely refresh LitMarket SQLite data in place.

This is intentionally incremental and non-destructive. It never deletes the
SQLite database. Expensive API work is opt-in/bounded where possible, and
source-refresh failures leave the existing database usable.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from analysis.research_runner import VALID_SIGNALS, analyze_signal
from backend.database import get_connection, get_db_path, init_db
from backend.pipelines import refresh_sources
from backend.pipelines.clean_viral_cache import DEFAULT_OUTPUT_DIR, clean_cache
from backend.pipelines.nightly_radar import run_nightly_radar
from backend.pipelines.seed_database import (
    CLEANED_DIR,
    SECTORS,
    SHORT_CACHE_DIR,
    count_tables,
    seed_analysis_results,
    seed_attention_scores,
    seed_event_windows,
    seed_market_daily,
    seed_papers,
    seed_publications_weekly,
    seed_radar_thresholds,
    seed_sectors,
    seed_viral_event_results,
    seed_weekly_abnormal_returns,
    to_float,
)


SIGNAL_LABELS = {
    "pub_deviation": "Deviation from median",
    "pub_zscore": "Z-score (52w rolling)",
    "pub_4w_dev": "4-week rolling deviation",
}


def refresh_database(
    *,
    dry_run: bool = False,
    skip_sources: bool = False,
    skip_weekly: bool = False,
    skip_market: bool = False,
    skip_viral_seed: bool = False,
    skip_nightly_radar: bool = False,
    nightly_days: int = 1,
    nightly_max_pages: int = 1,
    nightly_max_attention_scores: int | None = 50,
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

    if not skip_sources:
        summary["source_refresh"] = refresh_source_files(
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

    with get_connection() as conn:
        summary["sqlite"]["sectors"] = seed_sectors(conn)
        if not skip_market:
            market_count, spy_count = seed_market_daily(conn)
            summary["sqlite"]["market_daily"] = market_count
            summary["sqlite"]["spy_daily"] = spy_count
            summary["sqlite"]["abnormal_returns_weekly"] = seed_weekly_abnormal_returns(conn)
        if not skip_weekly:
            summary["sqlite"]["publications_weekly"] = seed_publications_weekly(conn)
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


def refresh_source_files(
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
    """Refresh CSV source files using the existing source refresher."""
    result: dict[str, Any] = {}
    target_week = (
        refresh_sources.monday_for(date.today())
        if force_current_week
        else refresh_sources.last_completed_week_start()
    )
    if not skip_weekly:
        result["weekly_target_week"] = target_week.isoformat()
        result["weekly_sectors"] = list(sectors)
        missing_by_sector = {
            sector: len(refresh_sources.make_week_plan(sector, target_week).weeks_to_fetch)
            for sector in sectors
        }
        result["weekly_missing_weeks"] = missing_by_sector
        too_large = {
            sector: count
            for sector, count in missing_by_sector.items()
            if max_weekly_weeks >= 0 and count > max_weekly_weeks
        }
        if too_large:
            raise RuntimeError(
                "Weekly OpenAlex refresh would fetch too many missing weeks: "
                f"{too_large}. Re-run with --max-weekly-weeks set higher, "
                "or use --skip-weekly to leave cached weekly counts unchanged."
            )
        refresh_sources.refresh_weekly_counts(sectors, target_week, dry_run=dry_run)
    if not skip_market:
        result["market_range"] = [market_start, market_end]
        refresh_sources.refresh_market_data(
            market_start,
            market_end,
            dry_run=dry_run,
            retries=3,
            retry_sleep=60,
            fail_fast=False,
        )
    return result


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
    parser.add_argument("--skip-viral-seed", action="store_true", help="Skip cached historical viral data upserts.")
    parser.add_argument("--skip-nightly-radar", action="store_true", help="Skip the incremental nightly radar job.")
    parser.add_argument("--nightly-days", type=int, default=1, help="Publication days for nightly radar lookback.")
    parser.add_argument("--nightly-max-pages", type=int, default=1, help="OpenAlex pages per keyword for nightly radar.")
    parser.add_argument(
        "--nightly-max-attention-scores",
        type=int,
        default=50,
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
        skip_viral_seed=args.skip_viral_seed,
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
