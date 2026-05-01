"""Viral-paper radar and event-study endpoints."""

from __future__ import annotations

import statistics
from collections import defaultdict
from typing import Any

from flask import Blueprint, jsonify, request

from backend.api.helpers import (
    clamp_days,
    doi_url,
    get_sector_or_404,
    parse_json,
    recent_start_date,
    row_to_dict,
)
from backend.database import get_connection


bp = Blueprint("viral", __name__, url_prefix="/api")


@bp.get("/viral")
def viral_feed():
    sector = request.args.get("sector")
    days = clamp_days(request.args.get("days"), default=5)
    return jsonify(get_recent_viral_events(sector=sector, days=days))


@bp.get("/sectors/<sector>/viral-analysis")
def viral_analysis(sector: str):
    get_sector_or_404(sector)
    with get_connection() as conn:
        summary_row = conn.execute(
            """
            SELECT result_json, computed_at
            FROM viral_event_results
            WHERE sector = ?
            """,
            (sector,),
        ).fetchone()
        curve_rows = conn.execute(
            """
            SELECT day_relative, COUNT(*) AS n,
                   AVG(ar) AS mean_ar, AVG(car) AS mean_car,
                   MIN(car) AS min_car, MAX(car) AS max_car
            FROM event_windows
            WHERE sector = ?
            GROUP BY day_relative
            ORDER BY day_relative
            """,
            (sector,),
        ).fetchall()
        car5_rows = conn.execute(
            """
            SELECT v.id AS viral_event_id, p.title, p.doi, p.publication_date,
                   a.reddit_hits, a.wiki_hits, a.cas, a.cit_velocity,
                   w.car AS car_5d
            FROM event_windows w
            JOIN viral_events v ON v.id = w.viral_event_id
            JOIN papers p ON p.id = v.paper_id_fk
            LEFT JOIN attention_scores a ON a.paper_id_fk = p.id
            WHERE w.sector = ? AND w.day_relative = 5
            ORDER BY w.car DESC
            """,
            (sector,),
        ).fetchall()
        volatility_rows = conn.execute(
            """
            SELECT viral_event_id,
                   AVG(CASE WHEN day_relative BETWEEN -3 AND -1 THEN ABS(ar) END) AS pre_abs_ar,
                   AVG(CASE WHEN day_relative BETWEEN 1 AND 5 THEN ABS(ar) END) AS post_abs_ar
            FROM event_windows
            WHERE sector = ?
            GROUP BY viral_event_id
            HAVING pre_abs_ar IS NOT NULL AND post_abs_ar IS NOT NULL
            ORDER BY viral_event_id
            """,
            (sector,),
        ).fetchall()

    car5 = [row_to_dict(row) for row in car5_rows]
    for item in car5:
        item["doi_url"] = doi_url(item.get("doi"))

    return jsonify(
        {
            "sector": sector,
            "computed_at": summary_row["computed_at"] if summary_row else None,
            "summary": parse_json(summary_row["result_json"], {}) if summary_row else {},
            "car_curve": [row_to_dict(row) for row in curve_rows],
            "car_5d_distribution": {
                "events": car5,
                "stats": _distribution_stats([row.get("car_5d") for row in car5]),
            },
            "attention_vs_car_5d": car5,
            "volatility_event_study": _volatility_summary(volatility_rows),
            "control_test": {
                "available": False,
                "note": "Control-test outputs are not present in the seeded SQLite schema yet.",
            },
        }
    )


def get_recent_viral_events(sector: str | None = None, days: int = 5) -> dict[str, Any]:
    params: list[Any] = []
    sector_filter = ""
    if sector:
        get_sector_or_404(sector)
        sector_filter = "WHERE v.sector = ?"
        params.append(sector)

    with get_connection() as conn:
        anchor_row = conn.execute(
            f"SELECT MAX(v.event_date) AS latest_event_date FROM viral_events v {sector_filter}",
            params,
        ).fetchone()
        latest_event_date = anchor_row["latest_event_date"] if anchor_row else None
        start_date = recent_start_date(latest_event_date, days)
        if start_date is None:
            return {
                "sector": sector,
                "window": {"days": days, "start_date": None, "end_date": None},
                "events": [],
            }

        rows = conn.execute(
            f"""
            SELECT v.id AS viral_event_id, v.sector, v.event_date, v.cas,
                   v.threshold_value, p.id AS paper_id, p.title, p.doi,
                   p.publication_date, p.detected_date, p.keyword,
                   p.source_display_name, p.cited_by_count,
                   a.reddit_hits, a.wiki_hits, a.citation_count,
                   a.cit_velocity, a.age_days,
                   r.result_json AS viral_result_json
            FROM viral_events v
            JOIN papers p ON p.id = v.paper_id_fk
            LEFT JOIN attention_scores a ON a.paper_id_fk = p.id
            LEFT JOIN viral_event_results r ON r.sector = v.sector
            WHERE v.event_date BETWEEN ? AND ?
              {"AND v.sector = ?" if sector else ""}
            ORDER BY v.event_date DESC, v.cas DESC, p.title
            """,
            [start_date, latest_event_date] + ([sector] if sector else []),
        ).fetchall()

        event_ids = [row["viral_event_id"] for row in rows]
        windows = _event_windows(conn, event_ids)

    events = []
    for row in rows:
        item = row_to_dict(row)
        result = parse_json(item.pop("viral_result_json"), {})
        car_5d = result.get("car_5d") or {}
        item["doi_url"] = doi_url(item.get("doi"))
        item["historical_context"] = {
            "mean_car_5d": car_5d.get("mean_car"),
            "n_events": car_5d.get("n_events"),
            "p_value": car_5d.get("p_value"),
            "note": car_5d.get("note"),
        }
        item["event_window"] = windows.get(item["viral_event_id"], [])
        events.append(item)

    return {
        "sector": sector,
        "window": {
            "days": days,
            "start_date": start_date,
            "end_date": latest_event_date,
        },
        "events": events,
    }


def _event_windows(conn: Any, event_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    if not event_ids:
        return {}
    placeholders = ",".join("?" for _ in event_ids)
    rows = conn.execute(
        f"""
        SELECT viral_event_id, date, day_relative, log_return, spy_return,
               ar, car, method
        FROM event_windows
        WHERE viral_event_id IN ({placeholders})
        ORDER BY viral_event_id, day_relative
        """,
        event_ids,
    ).fetchall()
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        item = row_to_dict(row)
        grouped[item.pop("viral_event_id")].append(item)
    return grouped


def _distribution_stats(values: list[float | None]) -> dict[str, Any]:
    clean = [value for value in values if value is not None]
    if not clean:
        return {"n": 0, "mean": None, "median": None, "min": None, "max": None}
    return {
        "n": len(clean),
        "mean": sum(clean) / len(clean),
        "median": statistics.median(clean),
        "min": min(clean),
        "max": max(clean),
    }


def _volatility_summary(rows: list[Any]) -> dict[str, Any]:
    events = [row_to_dict(row) for row in rows]
    diffs = [
        item["post_abs_ar"] - item["pre_abs_ar"]
        for item in events
        if item.get("pre_abs_ar") is not None and item.get("post_abs_ar") is not None
    ]
    return {
        "events": events,
        "stats": {
            "n": len(events),
            "mean_pre_abs_ar": _mean(item.get("pre_abs_ar") for item in events),
            "mean_post_abs_ar": _mean(item.get("post_abs_ar") for item in events),
            "mean_change": (sum(diffs) / len(diffs)) if diffs else None,
            "paired_t_test": None,
            "note": "Formal paired t-test is not recomputed by the API.",
        },
    }


def _mean(values: Any) -> float | None:
    clean = [value for value in values if value is not None]
    return (sum(clean) / len(clean)) if clean else None
