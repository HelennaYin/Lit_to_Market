"""Run the nightly viral-paper radar against SQLite.

The job follows the app design:
  1. Fetch newly published OpenAlex papers by sector/title keywords.
  2. Apply paper-quality filters before alerting.
  3. Score unscored DOI papers with Reddit, Wikipedia, and citation velocity.
  4. Compare CAS with each sector's historical nightly threshold.
  5. Insert alert rows into viral_events and radar_signals.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import date, datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Any

import requests

from backend.database import get_connection, init_db
from backend.pipelines.clean_viral_cache import (
    ALLOWED_OPENALEX_TYPES,
    VENUE_PREFIXES,
    VENUE_TITLES,
    normalize_text,
)
from backend.pipelines.seed_database import quantile


OPENALEX_BASE = "https://api.openalex.org"
WIKI_API = "https://en.wikipedia.org/w/api.php"
REDDIT_SEARCH = "https://www.reddit.com/search.json"
REQUEST_TIMEOUT = 30
OPENALEX_SLEEP = 0.2
ATTENTION_SLEEP = 1.0
WINDOW_DAYS = 14
WIKI_MAX_AGE_DAYS = 60
REDDIT_MAX_RETRY = 2
HEADERS = {
    "User-Agent": "LitMarket-Research-Bot/1.0 (academic; contact: research@example.com)"
}


def run_nightly_radar(
    target_date: date | None = None,
    days: int = 1,
    dry_run: bool = False,
    max_pages: int = 5,
    skip_attention: bool = False,
    max_attention_scores: int | None = None,
) -> dict[str, Any]:
    init_db()
    end_date = target_date or (date.today() - timedelta(days=1))
    start_date = end_date - timedelta(days=max(days, 1) - 1)
    timestamp = now_iso()

    summary: dict[str, Any] = {
        "target_window": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "days": days,
        },
        "dry_run": dry_run,
        "sectors": {},
        "inserted_papers": 0,
        "scored_papers": 0,
        "alerts": 0,
    }

    with get_connection() as conn:
        sectors = load_sectors(conn)
        ensure_radar_thresholds(conn, timestamp, dry_run=dry_run)

        for sector in sectors:
            fetched = fetch_openalex_sector(
                sector,
                start_date=start_date,
                end_date=end_date,
                max_pages=max_pages,
            )
            kept, removed = filter_papers(fetched, detected_date=end_date)
            inserted_ids = []
            if not dry_run:
                for paper in kept:
                    paper_id = upsert_paper(conn, paper, timestamp, detected_date=end_date)
                    inserted_ids.append(paper_id)

            summary["inserted_papers"] += len(inserted_ids)
            scored_ids = []
            if not dry_run and not skip_attention:
                scored_ids = score_unscored_papers(
                    conn,
                    sector["sector"],
                    timestamp,
                    max_scores=max_attention_scores,
                )
                summary["scored_papers"] += len(scored_ids)

            alerts = []
            if not dry_run:
                alerts = flag_alerts(conn, sector["sector"], timestamp, signal_date=end_date)
                summary["alerts"] += len(alerts)

            summary["sectors"][sector["sector"]] = {
                "label": sector["label"],
                "fetched": len(fetched),
                "kept": len(kept),
                "filtered_out": len(removed),
                "filtered_reasons": reason_counts(removed),
                "inserted_or_updated": len(inserted_ids),
                "scored": len(scored_ids),
                "alerts": len(alerts),
            }

    return summary


def load_sectors(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT sector, label, viral_ticker, keywords_json
        FROM sectors
        ORDER BY sector
        """
    ).fetchall()
    sectors = []
    for row in rows:
        sectors.append(
            {
                "sector": row["sector"],
                "label": row["label"],
                "viral_ticker": row["viral_ticker"],
                "keywords": json.loads(row["keywords_json"] or "[]"),
            }
        )
    return sectors


def fetch_openalex_sector(
    sector: dict[str, Any],
    start_date: date,
    end_date: date,
    max_pages: int,
) -> list[dict[str, Any]]:
    papers = []
    seen = set()
    for keyword in sector["keywords"]:
        cursor = "*"
        page = 0
        while cursor and page < max_pages:
            params = {
                "filter": (
                    f"title.search:{clean_openalex_value(keyword)},"
                    f"from_publication_date:{start_date.isoformat()},"
                    f"to_publication_date:{end_date.isoformat()},"
                    "has_doi:true"
                ),
                "select": (
                    "id,doi,title,publication_date,cited_by_count,type,"
                    "primary_location"
                ),
                "per-page": 200,
                "cursor": cursor,
                "mailto": os.getenv("OPENALEX_EMAIL", "research@litmarket.io"),
            }
            data = openalex_get(params)
            for paper in data.get("results", []):
                doi = normalize_doi(paper.get("doi"))
                key = (doi, sector["sector"])
                if not doi or key in seen:
                    continue
                seen.add(key)
                papers.append(
                    {
                        "paper_id": paper.get("id"),
                        "doi": doi,
                        "title": paper.get("title") or "",
                        "publication_date": paper.get("publication_date"),
                        "sector": sector["sector"],
                        "keyword": keyword,
                        "openalex_type": paper.get("type"),
                        "source_display_name": source_display_name(paper),
                        "cited_by_count": int(paper.get("cited_by_count") or 0),
                    }
                )
            cursor = data.get("meta", {}).get("next_cursor")
            page += 1
            time.sleep(OPENALEX_SLEEP)
    return papers


def openalex_get(params: dict[str, Any], retries: int = 5) -> dict[str, Any]:
    wait = 5
    for attempt in range(retries):
        try:
            response = requests.get(
                f"{OPENALEX_BASE}/works",
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            if response.status_code == 429 or response.status_code >= 500:
                time.sleep(wait)
                wait *= 2
                continue
            if not response.ok:
                raise RuntimeError(f"OpenAlex {response.status_code}: {response.text[:180]}")
            return response.json()
        except requests.RequestException as exc:
            if attempt == retries - 1:
                raise RuntimeError(f"OpenAlex request failed: {exc}") from exc
            time.sleep(wait)
            wait *= 2
    raise RuntimeError("OpenAlex unavailable after retries")


def filter_papers(
    papers: list[dict[str, Any]],
    detected_date: date,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    kept = []
    removed = []
    for paper in papers:
        reason = paper_filter_reason(paper, detected_date)
        if reason:
            removed.append({**paper, "filter_reason": reason})
        else:
            kept.append(paper)
    return kept, removed


def paper_filter_reason(paper: dict[str, Any], detected_date: date) -> str | None:
    doi = normalize_doi(paper.get("doi"))
    title = normalize_text(paper.get("title"))
    source = normalize_text(paper.get("source_display_name"))
    openalex_type = normalize_text(paper.get("openalex_type"))
    publication_date = parse_date(paper.get("publication_date"))
    cited_by_count = int(paper.get("cited_by_count") or 0)

    if not doi:
        return "missing_doi"
    if not title:
        return "missing_title"
    if publication_date is None:
        return "missing_publication_date"
    if openalex_type and openalex_type not in ALLOWED_OPENALEX_TYPES:
        return f"unsupported_openalex_type:{openalex_type}"
    if source and title == source:
        return "title_equals_source"
    if title in VENUE_TITLES:
        return "known_venue_title"
    if title.startswith(VENUE_PREFIXES):
        return "venue_title_prefix"

    age_days = max((detected_date - publication_date).days, 0)
    if 30 <= age_days <= 90 and cited_by_count < 3:
        return "citation_filter_30_90d"
    if age_days > 90 and cited_by_count < 10:
        return "citation_filter_over_90d"
    return None


def upsert_paper(
    conn,
    paper: dict[str, Any],
    timestamp: str,
    detected_date: date,
) -> int:
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
            openalex_type=excluded.openalex_type,
            source_display_name=excluded.source_display_name,
            cited_by_count=excluded.cited_by_count,
            detected_date=excluded.detected_date,
            updated_at=excluded.updated_at
        """,
        (
            paper.get("paper_id"),
            normalize_doi(paper.get("doi")),
            paper.get("title") or "(untitled)",
            str(paper.get("publication_date"))[:10],
            paper["sector"],
            paper.get("keyword"),
            paper.get("openalex_type"),
            paper.get("source_display_name"),
            int(paper.get("cited_by_count") or 0),
            detected_date.isoformat(),
            timestamp,
            timestamp,
        ),
    )
    row = conn.execute(
        "SELECT id FROM papers WHERE doi = ? AND sector = ?",
        (normalize_doi(paper.get("doi")), paper["sector"]),
    ).fetchone()
    return int(row["id"])


def score_unscored_papers(
    conn,
    sector: str,
    timestamp: str,
    max_scores: int | None = None,
) -> list[int]:
    limit_clause = "LIMIT ?" if max_scores is not None and max_scores >= 0 else ""
    params: tuple[Any, ...] = (sector,)
    if limit_clause:
        params = (sector, max_scores)
    rows = conn.execute(
        f"""
        SELECT p.id, p.doi, p.publication_date, p.cited_by_count
        FROM papers p
        LEFT JOIN attention_scores a ON a.paper_id_fk = p.id
        WHERE p.sector = ?
          AND p.is_filtered_out = 0
          AND a.id IS NULL
        ORDER BY p.publication_date DESC, p.id
        {limit_clause}
        """,
        params,
    ).fetchall()
    scored_ids = []
    for row in rows:
        pub_date = parse_date(row["publication_date"])
        age_days = max((date.today() - pub_date).days, 0) if pub_date else None
        reddit_hits = query_reddit(row["doi"], pub_date)
        time.sleep(ATTENTION_SLEEP)
        wiki_hits = query_wikipedia(row["doi"], pub_date)
        time.sleep(ATTENTION_SLEEP)
        citation_count = int(row["cited_by_count"] or 0)
        cit_velocity = round(citation_count / max(age_days or 1, 1), 4)
        cas = round(reddit_hits * 25.0 + wiki_hits * 10.0 + cit_velocity, 4)
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
                row["id"],
                reddit_hits,
                wiki_hits,
                citation_count,
                cit_velocity,
                age_days,
                cas,
                timestamp,
            ),
        )
        scored_ids.append(int(row["id"]))
    return scored_ids


def query_reddit(doi: str, pub_date: date | None, retry: int = 0) -> int:
    if not doi or pub_date is None:
        return 0
    pub_unix = int(datetime.combine(pub_date, datetime.min.time()).timestamp())
    end_unix = pub_unix + WINDOW_DAYS * 86400
    try:
        response = requests.get(
            REDDIT_SEARCH,
            params={
                "q": doi,
                "sort": "relevance",
                "limit": 25,
                "after": pub_unix,
                "before": end_unix,
            },
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        if response.status_code == 403:
            return 0
        if response.status_code == 429:
            if retry >= REDDIT_MAX_RETRY:
                return 0
            time.sleep(60 * (retry + 1))
            return query_reddit(doi, pub_date, retry + 1)
        if not response.ok:
            return 0
        total = 0
        for post in response.json().get("data", {}).get("children", []):
            score = min(post.get("data", {}).get("score", 1), 500)
            total += 3 if score > 100 else (2 if score > 10 else 1)
        return total
    except requests.RequestException:
        return 0


def query_wikipedia(doi: str, pub_date: date | None) -> int:
    if not doi:
        return 0
    if pub_date and (date.today() - pub_date).days > WIKI_MAX_AGE_DAYS:
        return 0
    try:
        response = requests.get(
            WIKI_API,
            params={
                "action": "query",
                "list": "search",
                "srsearch": doi,
                "srnamespace": 0,
                "srlimit": 5,
                "format": "json",
            },
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        if not response.ok:
            return 0
        return len(response.json().get("query", {}).get("search", []))
    except requests.RequestException:
        return 0


def ensure_radar_thresholds(conn, timestamp: str, dry_run: bool = False) -> None:
    rows = conn.execute(
        """
        SELECT sector, COUNT(*) AS n, GROUP_CONCAT(cas) AS cas_values
        FROM viral_events
        WHERE is_historical = 1
        GROUP BY sector
        """
    ).fetchall()
    for row in rows:
        values = [float(value) for value in str(row["cas_values"] or "").split(",") if value]
        threshold = quantile(values, 0.30)
        if threshold is None or dry_run:
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
            (row["sector"], threshold, int(row["n"]), timestamp),
        )


def flag_alerts(conn, sector: str, timestamp: str, signal_date: date) -> list[int]:
    threshold_row = conn.execute(
        "SELECT threshold_value FROM radar_thresholds WHERE sector = ?",
        (sector,),
    ).fetchone()
    if threshold_row is None:
        return []
    threshold = float(threshold_row["threshold_value"])
    context = historical_context(conn, sector)
    rows = conn.execute(
        """
        SELECT p.id AS paper_id, p.publication_date, a.cas
        FROM papers p
        JOIN attention_scores a ON a.paper_id_fk = p.id
        WHERE p.sector = ?
          AND p.is_filtered_out = 0
          AND p.detected_date = ?
          AND a.cas >= ?
        ORDER BY a.cas DESC
        """,
        (sector, signal_date.isoformat(), threshold),
    ).fetchall()
    alert_ids = []
    for row in rows:
        viral_event_id = upsert_nightly_viral_event(
            conn,
            paper_id=int(row["paper_id"]),
            sector=sector,
            event_date=str(row["publication_date"])[:10],
            cas=float(row["cas"]),
            threshold=threshold,
            timestamp=timestamp,
        )
        detection_lag = detection_lag_days(row["publication_date"], signal_date)
        conn.execute(
            """
            INSERT INTO radar_signals (
                paper_id_fk, sector, signal_date, publication_date,
                detection_lag_days, cas, threshold_value,
                historical_car_5d, historical_n, historical_pval,
                days_remaining, status, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'triggered', ?)
            ON CONFLICT(paper_id_fk, signal_date) DO UPDATE SET
                cas=excluded.cas,
                threshold_value=excluded.threshold_value,
                historical_car_5d=excluded.historical_car_5d,
                historical_n=excluded.historical_n,
                historical_pval=excluded.historical_pval,
                days_remaining=excluded.days_remaining,
                status=excluded.status,
                created_at=excluded.created_at
            """,
            (
                row["paper_id"],
                sector,
                signal_date.isoformat(),
                str(row["publication_date"])[:10],
                detection_lag,
                float(row["cas"]),
                threshold,
                context.get("mean_car_5d"),
                context.get("n_events"),
                context.get("p_value"),
                max(5 - (detection_lag or 0), 0),
                timestamp,
            ),
        )
        alert_ids.append(viral_event_id)
    return alert_ids


def upsert_nightly_viral_event(
    conn,
    paper_id: int,
    sector: str,
    event_date: str,
    cas: float,
    threshold: float,
    timestamp: str,
) -> int:
    row = conn.execute(
        """
        SELECT id
        FROM viral_events
        WHERE paper_id_fk = ?
          AND sector = ?
          AND event_date = ?
          AND is_historical = 0
        """,
        (paper_id, sector, event_date),
    ).fetchone()
    if row:
        conn.execute(
            """
            UPDATE viral_events
            SET cas = ?, threshold_value = ?, created_at = ?
            WHERE id = ?
            """,
            (cas, threshold, timestamp, row["id"]),
        )
        return int(row["id"])

    conn.execute(
        """
        INSERT INTO viral_events (
            paper_id_fk, sector, event_date, cas, threshold_type,
            threshold_value, is_historical, created_at
        )
        VALUES (?, ?, ?, ?, 'nightly_30pct_of_historical_viral', ?, 0, ?)
        """,
        (paper_id, sector, event_date, cas, threshold, timestamp),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def historical_context(conn, sector: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT result_json FROM viral_event_results WHERE sector = ?",
        (sector,),
    ).fetchone()
    if row is not None:
        result = json.loads(row["result_json"] or "{}")
        car_5d = result.get("car_5d") or {}
        return {
            "mean_car_5d": car_5d.get("mean_car"),
            "n_events": car_5d.get("n_events"),
            "p_value": car_5d.get("p_value"),
        }
    fallback = conn.execute(
        """
        SELECT COUNT(*) AS n, AVG(w.car) AS mean_car_5d
        FROM event_windows w
        JOIN viral_events v ON v.id = w.viral_event_id
        WHERE v.sector = ?
          AND v.is_historical = 1
          AND w.day_relative = 5
        """,
        (sector,),
    ).fetchone()
    return {
        "mean_car_5d": fallback["mean_car_5d"] if fallback else None,
        "n_events": fallback["n"] if fallback else None,
        "p_value": None,
    }


def detection_lag_days(publication_date: str, signal_date: date) -> int | None:
    parsed = parse_date(publication_date)
    if parsed is None:
        return None
    return max((signal_date - parsed).days, 0)


def source_display_name(paper: dict[str, Any]) -> str | None:
    location = paper.get("primary_location") or {}
    source = location.get("source") or {}
    return source.get("display_name")


def normalize_doi(value: Any) -> str:
    doi = unescape(str(value or "")).strip()
    doi = doi.removeprefix("https://doi.org/")
    doi = doi.removeprefix("http://doi.org/")
    return doi.lower()


def parse_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def clean_openalex_value(value: str) -> str:
    return " ".join(str(value).replace(",", " ").split())


def reason_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        reason = str(row.get("filter_reason") or "unknown")
        counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items()))


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=parse_date_arg, default=None, help="Target publication date, YYYY-MM-DD. Defaults to yesterday.")
    parser.add_argument("--days", type=int, default=1, help="Number of publication days ending at --date.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and filter, but do not write SQLite.")
    parser.add_argument("--max-pages", type=int, default=5, help="Maximum OpenAlex cursor pages per keyword.")
    parser.add_argument("--skip-attention", action="store_true", help="Insert papers but skip Reddit/Wikipedia scoring.")
    parser.add_argument(
        "--max-attention-scores",
        type=int,
        default=None,
        help="Maximum unscored papers to score per sector. Use -1 for no cap.",
    )
    return parser.parse_args()


def parse_date_arg(value: str) -> date:
    parsed = parse_date(value)
    if parsed is None:
        raise argparse.ArgumentTypeError("Use YYYY-MM-DD")
    return parsed


def main() -> None:
    args = parse_args()
    summary = run_nightly_radar(
        target_date=args.date,
        days=max(args.days, 1),
        dry_run=args.dry_run,
        max_pages=max(args.max_pages, 1),
        skip_attention=args.skip_attention,
        max_attention_scores=None if args.max_attention_scores is None or args.max_attention_scores < 0 else args.max_attention_scores,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
