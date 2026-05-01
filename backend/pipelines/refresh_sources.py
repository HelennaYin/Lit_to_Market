"""Refresh source datasets used to seed LitMarket.

This script updates the research source files in `Logic_test/cache` and
`Logic_test/stock/raw` without touching the SQLite database. It is meant
to be run before rebuilding a seeded DB or before rerunning the weekly
analysis notebook/scripts.

Policy:
  - Weekly OpenAlex counts refresh only when a new week has started.
    By default it fetches through the most recently completed Sunday, not
    the currently incomplete week.
  - Market data refreshes from yfinance and then rebuilds daily/weekly/
    monthly aggregate CSVs using the same convention as
    `Logic_test/aggregate_market.py`.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = PROJECT_ROOT.parent
LOGIC_DIR = WORKSPACE_ROOT / "Logic_test"
LOGIC_CACHE_DIR = LOGIC_DIR / "cache"
STOCK_RAW_DIR = LOGIC_DIR / "stock" / "raw"

OPENALEX_BASE = "https://api.openalex.org"
OPENALEX_EMAIL = os.environ.get("OPENALEX_EMAIL", "research@litmarket.io")
OPENALEX_SLEEP = float(os.environ.get("OPENALEX_SLEEP", "0.2"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))

APP_SECTORS = ("biotech_pharma", "ai_tech", "clean_energy", "semiconductors")

SECTOR_CONCEPTS = {
    "biotech_pharma": [
        ("C203014093", "Oncology"),
        ("C54355233", "Genomics"),
        ("C185592680", "Drug discovery"),
        ("C126322002", "Immunology"),
    ],
    "ai_tech": [
        ("C154945302", "Artificial intelligence"),
        ("C119857082", "Machine learning"),
        ("C204321447", "Natural language processing"),
        ("C108827166", "Deep learning"),
    ],
    "clean_energy": [
        ("C543025899", "Photovoltaics"),
        ("C185783690", "Battery"),
        ("C124952713", "Renewable energy"),
    ],
    "semiconductors": [
        ("C44155884", "Semiconductor"),
        ("C33923547", "Integrated circuit"),
        ("C41625074", "Materials science"),
        ("C114614502", "VLSI"),
    ],
}

MARKET_SOURCES = {
    "biotech_pharma": ("XBI", STOCK_RAW_DIR / "xbi_ohlcv.csv"),
    "ai_tech": ("BOTZ", STOCK_RAW_DIR / "botz_ohlcv.csv"),
    "spy": ("SPY", STOCK_RAW_DIR / "spy_ohlcv.csv"),
    "clean_energy": ("ICLN", STOCK_RAW_DIR / "icln_ohlcv.csv"),
    "semiconductors": ("SOXX", STOCK_RAW_DIR / "soxx_ohlcv.csv"),
}


@dataclass(frozen=True)
class RefreshPlan:
    sector: str
    cache_file: Path
    latest_cached_week: date | None
    target_week: date
    weeks_to_fetch: list[date]


def parse_date(value: str) -> date:
    return datetime.strptime(value[:10], "%Y-%m-%d").date()


def monday_for(day: date) -> date:
    return day - timedelta(days=day.weekday())


def last_completed_week_start(today: date | None = None) -> date:
    today = today or date.today()
    return monday_for(today) - timedelta(days=7)


def week_end_for(week_start: date) -> date:
    return week_start + timedelta(days=6)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv_rows(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def latest_week_in_cache(path: Path) -> date | None:
    rows = read_csv_rows(path)
    weeks = [parse_date(row["week_start"]) for row in rows if row.get("week_start")]
    return max(weeks) if weeks else None


def make_week_plan(sector: str, target_week: date) -> RefreshPlan:
    cache_file = LOGIC_CACHE_DIR / f"weekly_{sector}.csv"
    latest = latest_week_in_cache(cache_file)

    if latest is None:
        start = date(2016, 1, 1)
        start = monday_for(start)
    else:
        start = latest + timedelta(days=7)

    weeks = []
    current = start
    while current <= target_week:
        weeks.append(current)
        current += timedelta(days=7)

    return RefreshPlan(sector, cache_file, latest, target_week, weeks)


def openalex_get(path: str, params: dict[str, Any], retries: int = 5) -> dict[str, Any] | None:
    query = urlencode({**params, "mailto": OPENALEX_EMAIL})
    url = f"{OPENALEX_BASE}{path}?{query}"
    wait = 5

    for attempt in range(1, retries + 1):
        request = Request(url, headers={"User-Agent": "LitMarket/0.1"})
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            if attempt == retries:
                raise RuntimeError(f"OpenAlex request failed after {retries} attempts: {exc}") from exc
            print(f"OpenAlex request failed ({attempt}/{retries}); waiting {wait}s: {exc}")
            time.sleep(wait)
            wait *= 2

    return None


def count_concept_week(concept_id: str, week_start: date) -> int:
    params = {
        "filter": (
            f"concepts.id:{concept_id},"
            f"from_publication_date:{week_start.isoformat()},"
            f"to_publication_date:{week_end_for(week_start).isoformat()}"
        ),
        "per-page": 1,
        "select": "id",
    }
    data = openalex_get("/works", params)
    if data is None:
        return 0
    return int(data.get("meta", {}).get("count", 0))


def refresh_weekly_counts(sectors: tuple[str, ...], target_week: date, dry_run: bool) -> None:
    for sector in sectors:
        plan = make_week_plan(sector, target_week)
        print(
            f"[weekly:{sector}] latest={plan.latest_cached_week} "
            f"target={plan.target_week} missing={len(plan.weeks_to_fetch)}"
        )

        if not plan.weeks_to_fetch:
            continue
        if dry_run:
            continue

        rows = read_csv_rows(plan.cache_file)
        existing_by_week = {
            row["week_start"][:10]: row
            for row in rows
            if row.get("week_start")
        }

        for week_start in plan.weeks_to_fetch:
            week_count = 0
            for concept_id, concept_name in SECTOR_CONCEPTS[sector]:
                n = count_concept_week(concept_id, week_start)
                week_count += n
                print(f"  {week_start} | {concept_name:30s} -> {n}")
                time.sleep(OPENALEX_SLEEP)

            existing_by_week[week_start.isoformat()] = {
                "week_start": week_start.isoformat(),
                "pub_count": str(week_count),
                "sector": sector,
            }
            sorted_rows = [
                existing_by_week[key]
                for key in sorted(existing_by_week)
            ]
            write_csv_rows(plan.cache_file, sorted_rows, ["week_start", "pub_count", "sector"])
            print(f"  saved {plan.cache_file} ({len(sorted_rows)} rows)")

    if not dry_run:
        rebuild_combined_publication_counts(sectors)


def rebuild_combined_publication_counts(sectors: tuple[str, ...]) -> None:
    weekly_rows: list[dict[str, Any]] = []
    for sector in sectors:
        weekly_rows.extend(read_csv_rows(LOGIC_CACHE_DIR / f"weekly_{sector}.csv"))

    weekly_rows = sorted(weekly_rows, key=lambda row: (row["sector"], row["week_start"]))
    write_csv_rows(
        LOGIC_CACHE_DIR / "all_weekly_counts.csv",
        weekly_rows,
        ["week_start", "pub_count", "sector"],
    )

    monthly_totals: dict[tuple[str, str], int] = {}
    for row in weekly_rows:
        week_start = parse_date(row["week_start"])
        month = week_start.replace(day=1).isoformat()
        key = (row["sector"], month)
        monthly_totals[key] = monthly_totals.get(key, 0) + int(float(row["pub_count"]))

    by_sector: dict[str, list[dict[str, Any]]] = {}
    for (sector, month), count in sorted(monthly_totals.items()):
        by_sector.setdefault(sector, []).append({
            "sector": sector,
            "publication_month": month,
            "pub_count": count,
        })

    monthly_rows: list[dict[str, Any]] = []
    for sector, rows in by_sector.items():
        for idx, row in enumerate(rows):
            trailing = rows[max(0, idx - 2):idx + 1]
            ma3 = sum(item["pub_count"] for item in trailing) / len(trailing)
            row["pub_count_ma3"] = round(ma3, 6)
            monthly_rows.append(row)

    monthly_rows = sorted(monthly_rows, key=lambda row: (row["sector"], row["publication_month"]))
    write_csv_rows(
        LOGIC_CACHE_DIR / "all_monthly_counts.csv",
        monthly_rows,
        ["sector", "publication_month", "pub_count", "pub_count_ma3"],
    )
    print(f"rebuilt combined publication counts: {len(weekly_rows)} weekly rows, {len(monthly_rows)} monthly rows")


def refresh_market_data(
    start: str,
    end: str,
    dry_run: bool,
    retries: int,
    retry_sleep: int,
    fail_fast: bool,
) -> None:
    print(f"[market] target range {start} -> {end}")
    if dry_run:
        for sector, (ticker, path) in MARKET_SOURCES.items():
            print(f"  would download {ticker} for {sector} -> {path}")
        return

    try:
        import yfinance as yf
    except ImportError as exc:
        raise SystemExit(
            "yfinance is required for market refresh. Install it in your conda env "
            "or run only weekly OpenAlex refresh with --skip-market."
        ) from exc

    STOCK_RAW_DIR.mkdir(parents=True, exist_ok=True)
    failed: list[str] = []

    for sector, (ticker, path) in MARKET_SOURCES.items():
        ok = False
        for attempt in range(1, retries + 1):
            print(f"  downloading {ticker} ({sector}) attempt {attempt}/{retries}")
            try:
                df = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)
            except Exception as exc:
                df = None
                print(f"    yfinance error for {ticker}: {exc}")

            if df is not None and not df.empty:
                df.to_csv(path)
                print(f"    saved {path}")
                ok = True
                break

            if attempt < retries:
                print(f"    no rows for {ticker}; waiting {retry_sleep}s before retry")
                time.sleep(retry_sleep)

        if not ok:
            failed.append(ticker)
            message = f"yfinance returned no rows for {ticker}"
            if path.exists():
                print(f"    WARNING: {message}; keeping existing {path}")
            elif fail_fast:
                raise RuntimeError(message)
            else:
                print(f"    WARNING: {message}; no existing raw file found")

    rebuild_market_aggregates()
    if failed:
        print(
            "market refresh completed with stale/missing tickers: "
            + ", ".join(failed)
        )


def rebuild_market_aggregates() -> None:
    try:
        import numpy as np
        import pandas as pd
    except ImportError as exc:
        raise SystemExit("pandas and numpy are required to rebuild market aggregates.") from exc

    daily_frames = []
    for sector, (ticker, path) in MARKET_SOURCES.items():
        df = pd.read_csv(path, header=[0, 1], index_col=0)
        df.columns = [col[0].lower() for col in df.columns]
        df.index.name = "date"
        df = df.reset_index()
        df["date"] = pd.to_datetime(df["date"])
        df["sector"] = sector
        df["ticker"] = ticker
        df = df.sort_values("date").reset_index(drop=True)
        df["log_return"] = np.log(df["close"] / df["close"].shift(1))
        daily_frames.append(df)

    daily = pd.concat(daily_frames, ignore_index=True)
    weekly = daily.copy()
    weekly["week_start"] = (
        weekly["date"] - pd.to_timedelta(weekly["date"].dt.weekday, unit="D")
    ).dt.normalize()
    weekly = (
        weekly.groupby(["sector", "ticker", "week_start"], as_index=False)
        .agg(close=("close", "last"))
        .sort_values(["sector", "week_start"])
    )
    weekly["log_return"] = weekly.groupby("sector")["close"].transform(lambda s: np.log(s / s.shift(1)))
    weekly = weekly.dropna(subset=["log_return"]).reset_index(drop=True)

    monthly = daily.copy()
    monthly["month"] = monthly["date"].dt.to_period("M")
    monthly = (
        monthly.groupby(["sector", "ticker", "month"], as_index=False)
        .agg(close=("close", "last"))
        .sort_values(["sector", "month"])
    )
    monthly["log_return"] = monthly.groupby("sector")["close"].transform(lambda s: np.log(s / s.shift(1)))
    monthly["month"] = monthly["month"].dt.to_timestamp()
    monthly = monthly.dropna(subset=["log_return"]).reset_index(drop=True)

    LOGIC_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    daily.to_csv(LOGIC_CACHE_DIR / "all_market_daily.csv", index=False)
    weekly.to_csv(LOGIC_CACHE_DIR / "all_market_weekly.csv", index=False)
    monthly.to_csv(LOGIC_CACHE_DIR / "all_market_monthly.csv", index=False)
    print(f"rebuilt market aggregates: {len(daily)} daily, {len(weekly)} weekly, {len(monthly)} monthly rows")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-openalex", action="store_true")
    parser.add_argument("--skip-market", action="store_true")
    parser.add_argument("--force-current-week", action="store_true", help="Fetch through the current Monday even if the week is incomplete.")
    parser.add_argument("--market-start", default="2016-01-01")
    parser.add_argument("--market-end", default=date.today().isoformat(), help="yfinance end date is exclusive.")
    parser.add_argument("--market-retries", type=int, default=3)
    parser.add_argument("--market-retry-sleep", type=int, default=60)
    parser.add_argument(
        "--market-fail-fast",
        action="store_true",
        help="Abort if any ticker fails instead of keeping existing raw CSVs.",
    )
    parser.add_argument("--sectors", nargs="+", choices=APP_SECTORS, default=list(APP_SECTORS))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    sectors = tuple(args.sectors)
    target_week = monday_for(date.today()) if args.force_current_week else last_completed_week_start()

    if not args.skip_openalex:
        refresh_weekly_counts(sectors, target_week, dry_run=args.dry_run)

    if not args.skip_market:
        refresh_market_data(
            args.market_start,
            args.market_end,
            dry_run=args.dry_run,
            retries=args.market_retries,
            retry_sleep=args.market_retry_sleep,
            fail_fast=args.market_fail_fast,
        )


if __name__ == "__main__":
    main()
