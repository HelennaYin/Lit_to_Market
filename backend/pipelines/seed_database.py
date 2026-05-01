"""Seed the LitMarket SQLite database from current local source files."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.database import get_connection, get_db_path, init_db
from backend.pipelines.clean_viral_cache import DEFAULT_OUTPUT_DIR, clean_cache


PROJECT_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = PROJECT_ROOT.parent
LOGIC_CACHE_DIR = WORKSPACE_ROOT / "Logic_test" / "cache"
LOGIC_RESULTS_DIR = WORKSPACE_ROOT / "Logic_test" / "results"
SHORT_CACHE_DIR = WORKSPACE_ROOT / "Short term analysis" / "cache"
CLEANED_DIR = DEFAULT_OUTPUT_DIR

SECTORS = {
    "biotech_pharma": {
        "label": "Biotech/Pharma",
        "weekly_ticker": "XBI",
        "viral_ticker": "XBI",
        "keywords": ["oncology", "genomics", "immunotherapy", "drug discovery", "CRISPR"],
    },
    "ai_tech": {
        "label": "AI/Tech",
        "weekly_ticker": "BOTZ",
        "viral_ticker": "AIQ",
        "keywords": ["artificial intelligence", "machine learning", "deep learning", "large language model", "neural network"],
    },
    "clean_energy": {
        "label": "Clean Energy",
        "weekly_ticker": "ICLN",
        "viral_ticker": "ICLN",
        "keywords": ["photovoltaic", "renewable energy", "battery storage", "wind energy", "solar cell"],
    },
    "semiconductors": {
        "label": "Semiconductors",
        "weekly_ticker": "SOXX",
        "viral_ticker": "SOXX",
        "keywords": ["semiconductor", "integrated circuit", "transistor", "VLSI", "chip fabrication"],
    },
}


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def to_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(n):
        return None
    return n


def to_int(value: object) -> int | None:
    n = to_float(value)
    return int(n) if n is not None else None


def norm_doi(value: object) -> str:
    doi = str(value or "").strip()
    doi = doi.removeprefix("https://doi.org/")
    doi = doi.removeprefix("http://doi.org/")
    return doi.lower()


def median(values: list[float]) -> float | None:
    clean = [v for v in values if v is not None]
    return statistics.median(clean) if clean else None


def rolling_mean_std(values: list[float], idx: int, window: int, min_periods: int) -> tuple[float | None, float | None]:
    start = max(0, idx - window + 1)
    sample = [v for v in values[start:idx + 1] if v is not None]
    if len(sample) < min_periods:
        return None, None
    mean = sum(sample) / len(sample)
    std = statistics.stdev(sample) if len(sample) > 1 else 0.0
    return mean, std


def quantile(values: list[float], q: float) -> float | None:
    clean = sorted(v for v in values if v is not None)
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    pos = (len(clean) - 1) * q
    lower = math.floor(pos)
    upper = math.ceil(pos)
    if lower == upper:
        return clean[int(pos)]
    weight = pos - lower
    return clean[lower] * (1 - weight) + clean[upper] * weight


def ensure_cleaned_inputs() -> None:
    required = [
        CLEANED_DIR / "filtered_papers_clean.csv",
        CLEANED_DIR / "attention_scores_clean.csv",
        CLEANED_DIR / "viral_events_clean.csv",
        CLEANED_DIR / "event_windows_clean.csv",
        CLEANED_DIR / "event_windows_complete_day5_clean.csv",
    ]
    if all(path.exists() for path in required):
        return
    print("cleaned viral cache missing; running clean_viral_cache first")
    clean_cache(SHORT_CACHE_DIR, CLEANED_DIR)


def reset_database() -> Path:
    db_path = get_db_path()
    if db_path.exists():
        db_path.unlink()
    return init_db(db_path)


def seed_sectors(conn) -> int:
    timestamp = now_iso()
    for sector, cfg in SECTORS.items():
        conn.execute(
            """
            INSERT INTO sectors (
                sector, label, weekly_ticker, viral_ticker,
                keywords_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sector) DO UPDATE SET
                label=excluded.label,
                weekly_ticker=excluded.weekly_ticker,
                viral_ticker=excluded.viral_ticker,
                keywords_json=excluded.keywords_json,
                updated_at=excluded.updated_at
            """,
            (
                sector,
                cfg["label"],
                cfg["weekly_ticker"],
                cfg["viral_ticker"],
                json.dumps(cfg["keywords"]),
                timestamp,
                timestamp,
            ),
        )
    return len(SECTORS)


def seed_market_daily(conn) -> tuple[int, int]:
    rows = read_csv(LOGIC_CACHE_DIR / "all_market_daily.csv")
    timestamp = now_iso()
    sector_count = 0
    spy_count = 0

    for row in rows:
        sector = row["sector"]
        ticker = row["ticker"]
        values = (
            row["date"][:10],
            to_float(row.get("open")),
            to_float(row.get("high")),
            to_float(row.get("low")),
            to_float(row.get("close")),
            to_int(row.get("volume")),
            to_float(row.get("log_return")),
            timestamp,
        )
        if sector == "spy":
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
                values,
            )
            spy_count += 1
        else:
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
                    row["date"][:10],
                    to_float(row.get("open")),
                    to_float(row.get("high")),
                    to_float(row.get("low")),
                    to_float(row.get("close")),
                    to_int(row.get("volume")),
                    to_float(row.get("log_return")),
                    timestamp,
                ),
            )
            sector_count += 1
    return sector_count, spy_count


def seed_weekly_abnormal_returns(conn) -> int:
    rows = read_csv(LOGIC_CACHE_DIR / "all_market_weekly.csv")
    by_sector: dict[str, list[dict[str, str]]] = defaultdict(list)
    spy_by_week: dict[str, float] = {}

    for row in rows:
        if row["sector"] == "spy":
            value = to_float(row.get("log_return"))
            if value is not None:
                spy_by_week[row["week_start"][:10]] = value
        else:
            by_sector[row["sector"]].append(row)

    timestamp = now_iso()
    inserted = 0
    for sector, sector_rows in by_sector.items():
        pairs = []
        for row in sector_rows:
            sector_return = to_float(row.get("log_return"))
            spy_return = spy_by_week.get(row["week_start"][:10])
            if sector_return is not None and spy_return is not None:
                pairs.append((spy_return, sector_return))

        alpha, beta, r_squared = fit_ols(pairs)

        for row in sector_rows:
            sector_return = to_float(row.get("log_return"))
            spy_return = spy_by_week.get(row["week_start"][:10])
            if sector_return is None or spy_return is None:
                continue
            abnormal = sector_return - (alpha + beta * spy_return)
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
                    row["ticker"],
                    row["week_start"][:10],
                    sector_return,
                    spy_return,
                    abnormal,
                    alpha,
                    beta,
                    r_squared,
                    timestamp,
                ),
            )
            inserted += 1
    return inserted


def fit_ols(pairs: list[tuple[float, float]]) -> tuple[float, float, float]:
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


def seed_publications_weekly(conn) -> int:
    timestamp = now_iso()
    inserted = 0
    for sector in SECTORS:
        path = LOGIC_CACHE_DIR / f"weekly_{sector}.csv"
        rows = sorted(read_csv(path), key=lambda row: row["week_start"])
        counts = [float(row["pub_count"]) for row in rows]
        base_median = median(counts) or 0.0
        rolling_sums = [
            sum(counts[max(0, idx - 3):idx + 1])
            for idx in range(len(counts))
        ]
        rolling_sum_median = median(rolling_sums) or 0.0

        for idx, row in enumerate(rows):
            count = float(row["pub_count"])
            roll_mean, roll_std = rolling_mean_std(counts, idx, window=52, min_periods=26)
            pub_zscore = None
            if roll_mean is not None and roll_std and roll_std > 0:
                pub_zscore = (count - roll_mean) / roll_std

            conn.execute(
                """
                INSERT INTO publications_weekly (
                    sector, week_start, pub_count, pub_deviation,
                    pub_zscore, pub_4w_dev, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sector, week_start) DO UPDATE SET
                    pub_count=excluded.pub_count,
                    pub_deviation=excluded.pub_deviation,
                    pub_zscore=excluded.pub_zscore,
                    pub_4w_dev=excluded.pub_4w_dev,
                    created_at=excluded.created_at
                """,
                (
                    sector,
                    row["week_start"][:10],
                    int(count),
                    count - base_median,
                    pub_zscore,
                    rolling_sums[idx] - rolling_sum_median,
                    timestamp,
                ),
            )
            inserted += 1
    return inserted


def seed_analysis_results(conn) -> int:
    timestamp = now_iso()
    inserted = 0
    for path in sorted(LOGIC_RESULTS_DIR.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        sector = data.get("sector")
        signal_col = data.get("signal_col")
        if sector not in SECTORS or not signal_col:
            continue
        conn.execute(
            """
            INSERT INTO analysis_results (sector, signal_col, result_json, computed_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(sector, signal_col) DO UPDATE SET
                result_json=excluded.result_json,
                computed_at=excluded.computed_at
            """,
            (sector, signal_col, json.dumps(data), timestamp),
        )
        inserted += 1
    return inserted


def seed_papers(conn) -> dict[tuple[str, str], int]:
    timestamp = now_iso()
    paper_ids: dict[tuple[str, str], int] = {}
    rows = read_csv(CLEANED_DIR / "filtered_papers_clean.csv")

    for row in rows:
        doi = norm_doi(row.get("doi"))
        sector = row["sector"]
        if not doi or sector not in SECTORS:
            continue
        paper_id = upsert_paper(conn, row, doi, sector, timestamp)
        paper_ids[(doi, sector)] = paper_id
    return paper_ids


def upsert_paper(conn, row: dict[str, str], doi: str, sector: str, timestamp: str) -> int:
    conn.execute(
        """
        INSERT INTO papers (
            paper_id, doi, title, publication_date, sector, keyword,
            openalex_type, source_display_name, cited_by_count,
            is_filtered_out, filter_reason, detected_date, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, ?, ?, ?)
        ON CONFLICT(doi, sector) DO UPDATE SET
            paper_id=excluded.paper_id,
            title=excluded.title,
            publication_date=excluded.publication_date,
            keyword=excluded.keyword,
            cited_by_count=excluded.cited_by_count,
            updated_at=excluded.updated_at
        """,
        (
            row.get("paper_id"),
            doi,
            row.get("title") or "(untitled)",
            (row.get("publication_date") or row.get("event_date") or "")[:10],
            sector,
            row.get("keyword"),
            row.get("openalex_type") or row.get("type"),
            row.get("source_display_name"),
            to_int(row.get("cited_by_count") or row.get("citation_count")),
            timestamp[:10],
            timestamp,
            timestamp,
        ),
    )
    db_row = conn.execute(
        "SELECT id FROM papers WHERE doi = ? AND sector = ?",
        (doi, sector),
    ).fetchone()
    return int(db_row["id"])


def seed_attention_scores(conn, paper_ids: dict[tuple[str, str], int]) -> int:
    timestamp = now_iso()
    rows = read_csv(CLEANED_DIR / "attention_scores_clean.csv")
    paper_ids_by_doi: dict[str, list[int]] = defaultdict(list)
    for (doi, _sector), paper_id in paper_ids.items():
        paper_ids_by_doi[doi].append(paper_id)

    inserted = 0
    for row in rows:
        doi = norm_doi(row.get("doi"))
        for paper_id in paper_ids_by_doi.get(doi, []):
            conn.execute(
                """
                INSERT INTO attention_scores (
                    paper_id_fk, reddit_hits, wiki_hits, citation_count,
                    cit_velocity, age_days, cas, scored_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(paper_id_fk) DO UPDATE SET
                    reddit_hits=excluded.reddit_hits,
                    wiki_hits=excluded.wiki_hits,
                    citation_count=excluded.citation_count,
                    cit_velocity=excluded.cit_velocity,
                    age_days=excluded.age_days,
                    cas=excluded.cas,
                    scored_at=excluded.scored_at
                """,
                (
                    paper_id,
                    to_int(row.get("reddit_hits")) or 0,
                    to_int(row.get("wiki_hits")) or 0,
                    to_int(row.get("citation_count")) or 0,
                    to_float(row.get("cit_velocity")) or 0.0,
                    to_int(row.get("age_days")),
                    to_float(row.get("cas")) or 0.0,
                    timestamp,
                ),
            )
            inserted += 1
    return inserted


def event_key(row: dict[str, str]) -> tuple[str, str, str]:
    return (
        norm_doi(row.get("doi")),
        str(row.get("sector") or ""),
        str(row.get("event_date") or row.get("publication_date") or "")[:10],
    )


def seed_viral_events(conn, paper_ids: dict[tuple[str, str], int]) -> dict[tuple[str, str, str], int]:
    timestamp = now_iso()
    rows = read_csv(CLEANED_DIR / "viral_events_clean.csv")
    cas_by_sector: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        cas = to_float(row.get("cas"))
        if row.get("sector") in SECTORS and cas is not None:
            cas_by_sector[row["sector"]].append(cas)

    threshold_by_sector = {
        sector: min(values) if values else 0.0
        for sector, values in cas_by_sector.items()
    }

    event_id_map: dict[tuple[str, str, str], int] = {}
    for row in rows:
        doi = norm_doi(row.get("doi"))
        sector = row["sector"]
        if not doi or sector not in SECTORS:
            continue
        paper_id = paper_ids.get((doi, sector))
        if paper_id is None:
            paper_id = upsert_paper(conn, row, doi, sector, timestamp)
            paper_ids[(doi, sector)] = paper_id

        conn.execute(
            """
            INSERT INTO viral_events (
                paper_id_fk, sector, event_date, cas, threshold_type,
                threshold_value, is_historical, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 1, ?)
            """,
            (
                paper_id,
                sector,
                (row.get("event_date") or row.get("publication_date"))[:10],
                to_float(row.get("cas")) or 0.0,
                "historical_top_5pct",
                threshold_by_sector.get(sector, 0.0),
                timestamp,
            ),
        )
        db_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        event_id_map[event_key(row)] = int(db_id)

    seed_radar_thresholds(conn, cas_by_sector)
    return event_id_map


def seed_radar_thresholds(conn, cas_by_sector: dict[str, list[float]]) -> int:
    timestamp = now_iso()
    inserted = 0
    for sector, values in cas_by_sector.items():
        threshold = quantile(values, 0.30)
        if threshold is None:
            continue
        conn.execute(
            """
            INSERT INTO radar_thresholds (
                sector, threshold_value, source_quantile,
                source_event_set, n_source_events, computed_at
            )
            VALUES (?, ?, 0.30, 'historical_top_5pct_cas', ?, ?)
            ON CONFLICT(sector) DO UPDATE SET
                threshold_value=excluded.threshold_value,
                n_source_events=excluded.n_source_events,
                computed_at=excluded.computed_at
            """,
            (sector, threshold, len(values), timestamp),
        )
        inserted += 1
    return inserted


def seed_event_windows(conn, event_id_map: dict[tuple[str, str, str], int]) -> int:
    rows = read_csv(CLEANED_DIR / "event_windows_clean.csv")
    inserted = 0
    ticker_by_sector = {sector: cfg["viral_ticker"] for sector, cfg in SECTORS.items()}

    for row in rows:
        sector = row.get("sector")
        viral_event_id = event_id_map.get(event_key(row))
        if viral_event_id is None or sector not in SECTORS:
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
                viral_event_id,
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


def seed_viral_event_results(conn) -> int:
    rows = read_csv(CLEANED_DIR / "event_windows_complete_day5_clean.csv")
    day5_by_sector: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        if row.get("day_relative") == "5":
            car = to_float(row.get("CAR"))
            if car is not None:
                day5_by_sector[row["sector"]].append(car)

    timestamp = now_iso()
    inserted = 0
    for sector, cars in day5_by_sector.items():
        result = {
            "sector": sector,
            "car_5d": {
                "n_events": len(cars),
                "mean_car": sum(cars) / len(cars) if cars else None,
                "min_car": min(cars) if cars else None,
                "max_car": max(cars) if cars else None,
                "p_value": None,
                "note": "p_value not recomputed in seed loader; use source analysis scripts for formal tests.",
            },
        }
        conn.execute(
            """
            INSERT INTO viral_event_results (sector, result_json, computed_at)
            VALUES (?, ?, ?)
            ON CONFLICT(sector) DO UPDATE SET
                result_json=excluded.result_json,
                computed_at=excluded.computed_at
            """,
            (sector, json.dumps(result), timestamp),
        )
        inserted += 1
    return inserted


def count_tables(conn) -> dict[str, int]:
    tables = [
        "sectors",
        "market_daily",
        "spy_daily",
        "publications_weekly",
        "abnormal_returns_weekly",
        "analysis_results",
        "papers",
        "attention_scores",
        "viral_events",
        "event_windows",
        "viral_event_results",
        "radar_thresholds",
    ]
    return {
        table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in tables
    }


def seed_database(reset: bool = True) -> dict[str, int]:
    ensure_cleaned_inputs()
    db_path = reset_database() if reset else init_db()
    print(f"seeding database: {db_path}")

    with get_connection(db_path) as conn:
        summary: dict[str, int] = {}
        summary["sectors"] = seed_sectors(conn)
        market_count, spy_count = seed_market_daily(conn)
        summary["market_daily"] = market_count
        summary["spy_daily"] = spy_count
        summary["abnormal_returns_weekly"] = seed_weekly_abnormal_returns(conn)
        summary["publications_weekly"] = seed_publications_weekly(conn)
        summary["analysis_results"] = seed_analysis_results(conn)
        paper_ids = seed_papers(conn)
        summary["papers"] = len(paper_ids)
        summary["attention_scores"] = seed_attention_scores(conn, paper_ids)
        event_id_map = seed_viral_events(conn, paper_ids)
        summary["viral_events"] = len(event_id_map)
        summary["event_windows"] = seed_event_windows(conn, event_id_map)
        summary["viral_event_results"] = seed_viral_event_results(conn)
        conn.commit()
        summary.update({f"table:{key}": value for key, value in count_tables(conn).items()})
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-reset", action="store_true", help="Do not delete the existing SQLite DB before seeding.")
    args = parser.parse_args()

    summary = seed_database(reset=not args.no_reset)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
