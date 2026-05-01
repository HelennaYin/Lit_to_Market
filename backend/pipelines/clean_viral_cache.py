"""Clean cached viral-paper CSVs before seeding SQLite.

This script is intentionally non-destructive. It reads the existing
`Short term analysis/cache` files and writes cleaned derivatives under
`Build/data/cleaned`.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = PROJECT_ROOT.parent
DEFAULT_CACHE_DIR = WORKSPACE_ROOT / "Short term analysis" / "cache"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "cleaned"

VENUE_TITLES = {
    "international journal of oncology",
    "journal of gastrointestinal oncology",
    "oncology reports",
    "oncology letters",
    "molecular and clinical oncology",
    "journal of the advanced practitioner in oncology",
    "international journal of artificial intelligence & applications",
    "semiconductor physics, quantum electronics and optoelectronics",
}

VENUE_PREFIXES = (
    "journal of ",
    "international journal of ",
)

ALLOWED_OPENALEX_TYPES = {
    "",
    "article",
    "preprint",
    "posted-content",
    "review",
    "proceedings-article",
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def normalize_text(value: object) -> str:
    text = unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def normalized_doi(row: dict[str, str]) -> str:
    doi = str(row.get("doi") or "").strip()
    doi = doi.removeprefix("https://doi.org/")
    doi = doi.removeprefix("http://doi.org/")
    return doi.lower()


def score_value(row: dict[str, str]) -> float:
    for col in ("cas", "cited_by_count", "citation_count"):
        try:
            return float(row.get(col) or 0)
        except ValueError:
            continue
    return 0.0


def venue_filter_reason(row: dict[str, str]) -> str | None:
    doi = normalized_doi(row)
    title = normalize_text(row.get("title"))
    source = normalize_text(row.get("source_display_name"))
    openalex_type = normalize_text(row.get("openalex_type") or row.get("type"))

    if not doi:
        return "missing_doi"
    if not title:
        return "missing_title"
    if not str(row.get("publication_date") or row.get("event_date") or "").strip():
        return "missing_publication_date"
    if openalex_type and openalex_type not in ALLOWED_OPENALEX_TYPES:
        return f"unsupported_openalex_type:{openalex_type}"
    if source and title == source:
        return "title_equals_source"
    if title in VENUE_TITLES:
        return "known_venue_title"
    if title.startswith(VENUE_PREFIXES):
        return "venue_title_prefix"
    return None


def dedupe_by_key(
    rows: list[dict[str, str]],
    key_fields: tuple[str, ...],
) -> tuple[list[dict[str, str]], int]:
    """Deduplicate rows, keeping the row with the largest available score."""
    best: dict[tuple[str, ...], dict[str, str]] = {}
    duplicates = 0

    for row in rows:
        key_parts = []
        for field in key_fields:
            if field == "doi":
                key_parts.append(normalized_doi(row))
            else:
                key_parts.append(str(row.get(field) or "").strip().lower())
        key = tuple(key_parts)

        if not all(key):
            key = tuple(key_parts + [str(len(best) + duplicates)])

        if key in best:
            duplicates += 1
            if score_value(row) > score_value(best[key]):
                best[key] = row
        else:
            best[key] = row

    return list(best.values()), duplicates


def clean_paper_rows(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    kept = []
    removed = []

    for row in rows:
        reason = venue_filter_reason(row)
        if reason:
            row = dict(row)
            row["filter_reason"] = reason
            removed.append(row)
        else:
            kept.append(row)

    return kept, removed


def complete_event_ids(event_windows: list[dict[str, str]]) -> set[str]:
    days_by_event: dict[str, set[int]] = defaultdict(set)
    for row in event_windows:
        event_id = str(row.get("event_id") or "")
        try:
            day = int(float(row.get("day_relative") or ""))
        except ValueError:
            continue
        days_by_event[event_id].add(day)
    return {event_id for event_id, days in days_by_event.items() if 5 in days}


def clean_cache(cache_dir: Path, output_dir: Path) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    audit: dict[str, object] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_cache_dir": str(cache_dir),
        "output_dir": str(output_dir),
    }

    viral_events = read_csv(cache_dir / "viral_events.csv")
    # event_windows.csv uses the original viral_events row index as event_id.
    for idx, row in enumerate(viral_events):
        row["event_id"] = str(idx)

    viral_kept, viral_removed = clean_paper_rows(viral_events)
    viral_kept, viral_duplicates = dedupe_by_key(viral_kept, ("doi", "sector"))
    removed_dois = {normalized_doi(row) for row in viral_removed}

    viral_fieldnames = ["event_id"] + [f for f in viral_events[0].keys() if f != "event_id"]
    write_csv(output_dir / "viral_events_clean.csv", viral_kept, viral_fieldnames)
    write_csv(
        output_dir / "viral_events_removed.csv",
        viral_removed,
        [*viral_fieldnames, "filter_reason"],
    )

    event_windows = read_csv(cache_dir / "event_windows.csv")
    event_windows_clean, event_windows_removed = clean_paper_rows(event_windows)
    event_windows_clean, event_window_duplicates = dedupe_by_key(
        event_windows_clean,
        ("doi", "sector", "event_date", "day_relative"),
    )
    complete_ids = complete_event_ids(event_windows_clean)
    event_windows_complete = [
        row for row in event_windows_clean
        if str(row.get("event_id") or "") in complete_ids
    ]
    write_csv(output_dir / "event_windows_clean.csv", event_windows_clean, list(event_windows[0].keys()))
    write_csv(
        output_dir / "event_windows_removed.csv",
        event_windows_removed,
        [*list(event_windows[0].keys()), "filter_reason"],
    )
    write_csv(
        output_dir / "event_windows_complete_day5_clean.csv",
        event_windows_complete,
        list(event_windows[0].keys()),
    )

    filtered_papers = read_csv(cache_dir / "filtered_papers.csv")
    papers_kept, papers_removed = clean_paper_rows(filtered_papers)
    papers_kept, paper_duplicates = dedupe_by_key(papers_kept, ("doi", "sector"))
    write_csv(output_dir / "filtered_papers_clean.csv", papers_kept, list(filtered_papers[0].keys()))
    write_csv(
        output_dir / "filtered_papers_removed.csv",
        papers_removed,
        [*list(filtered_papers[0].keys()), "filter_reason"],
    )

    attention_scores = read_csv(cache_dir / "attention_scores.csv")
    attention_clean = [
        row for row in attention_scores
        if normalized_doi(row) not in removed_dois
    ]
    attention_clean, attention_duplicates = dedupe_by_key(attention_clean, ("doi",))
    attention_fieldnames = [
        "doi",
        "reddit_hits",
        "wiki_hits",
        "citation_count",
        "cit_velocity",
        "age_days",
        "cas",
    ]
    write_csv(output_dir / "attention_scores_clean.csv", attention_clean, attention_fieldnames)

    audit.update({
        "viral_events": {
            "source_rows": len(viral_events),
            "kept_rows": len(viral_kept),
            "removed_rows": len(viral_removed),
            "duplicate_rows_removed": viral_duplicates,
            "removed_reasons": reason_counts(viral_removed),
        },
        "event_windows": {
            "source_rows": len(event_windows),
            "clean_rows": len(event_windows_clean),
            "removed_rows": len(event_windows_removed),
            "duplicate_rows_removed": event_window_duplicates,
            "complete_day5_rows": len(event_windows_complete),
            "clean_event_count": len({row.get("event_id") for row in event_windows_clean}),
            "complete_day5_event_count": len(complete_ids),
            "removed_reasons": reason_counts(event_windows_removed),
        },
        "filtered_papers": {
            "source_rows": len(filtered_papers),
            "kept_rows": len(papers_kept),
            "removed_rows": len(papers_removed),
            "duplicate_rows_removed": paper_duplicates,
            "removed_reasons": reason_counts(papers_removed),
        },
        "attention_scores": {
            "source_rows": len(attention_scores),
            "kept_rows": len(attention_clean),
            "duplicate_rows_removed": attention_duplicates,
            "dropped_due_to_removed_viral_doi": len(attention_scores) - len(attention_clean) - attention_duplicates,
        },
    })

    (output_dir / "viral_cleaning_audit.json").write_text(
        json.dumps(audit, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return audit


def reason_counts(rows: list[dict[str, str]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        reason = str(row.get("filter_reason") or "unknown")
        counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items()))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    audit = clean_cache(args.cache_dir, args.output_dir)
    print(json.dumps(audit, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
