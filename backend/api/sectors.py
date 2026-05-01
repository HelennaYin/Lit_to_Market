"""Sector metadata, overview, and weekly analysis endpoints."""

from __future__ import annotations

from typing import Any

from flask import Blueprint, abort, jsonify, request

from backend.api.helpers import (
    VALID_SIGNALS,
    get_sector_or_404,
    parse_json,
    row_to_dict,
)
from backend.api.viral import get_recent_viral_events
from backend.database import get_connection


bp = Blueprint("sectors", __name__, url_prefix="/api/sectors")


@bp.get("")
def list_sectors():
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT sector, label, weekly_ticker, viral_ticker,
                   keywords_json, updated_at
            FROM sectors
            ORDER BY label
            """
        ).fetchall()
    sectors = []
    for row in rows:
        item = row_to_dict(row)
        item["keywords"] = parse_json(item.pop("keywords_json"), [])
        sectors.append(item)
    return jsonify({"sectors": sectors})


@bp.get("/<sector>/overview")
def sector_overview(sector: str):
    sector_row = get_sector_or_404(sector)
    signal = request.args.get("signal", "pub_zscore")
    if signal not in VALID_SIGNALS:
        abort(400, description=f"signal must be one of {sorted(VALID_SIGNALS)}")

    with get_connection() as conn:
        latest_publication = conn.execute(
            """
            SELECT week_start, pub_count, pub_deviation, pub_zscore, pub_4w_dev
            FROM publications_weekly
            WHERE sector = ?
            ORDER BY week_start DESC
            LIMIT 1
            """,
            (sector,),
        ).fetchone()
        latest_return = conn.execute(
            """
            SELECT week_start, ticker, log_return, spy_return, abnormal_return
            FROM abnormal_returns_weekly
            WHERE sector = ?
            ORDER BY week_start DESC
            LIMIT 1
            """,
            (sector,),
        ).fetchone()
        sparkline = conn.execute(
            f"""
            SELECT week_start, {signal} AS value, pub_count
            FROM publications_weekly
            WHERE sector = ?
            ORDER BY week_start DESC
            LIMIT 12
            """,
            (sector,),
        ).fetchall()
        analysis_row = conn.execute(
            """
            SELECT signal_col, result_json, computed_at
            FROM analysis_results
            WHERE sector = ? AND signal_col = ?
            """,
            (sector, signal),
        ).fetchone()
        threshold = conn.execute(
            """
            SELECT threshold_value, source_quantile, source_event_set,
                   n_source_events, computed_at
            FROM radar_thresholds
            WHERE sector = ?
            """,
            (sector,),
        ).fetchone()

    latest_pub = row_to_dict(latest_publication)
    latest_ret = row_to_dict(latest_return)
    analysis = _analysis_summary(analysis_row, latest_pub, signal)
    recent_viral = get_recent_viral_events(sector=sector, days=5)

    return jsonify(
        {
            "sector": sector_row,
            "latest_publication": latest_pub,
            "latest_return": latest_ret,
            "sparkline": list(reversed([row_to_dict(row) for row in sparkline])),
            "weekly_evidence": analysis,
            "viral_radar": {
                "status": "Viral paper detected"
                if recent_viral["events"]
                else "No recent viral papers",
                "threshold": row_to_dict(threshold),
                "recent_window": recent_viral["window"],
                "events": recent_viral["events"],
            },
            "last_updated": _last_updated(sector),
        }
    )


@bp.get("/<sector>/analysis")
def sector_analysis(sector: str):
    get_sector_or_404(sector)
    signal = request.args.get("signal", "pub_zscore")
    if signal not in VALID_SIGNALS:
        abort(400, description=f"signal must be one of {sorted(VALID_SIGNALS)}")

    with get_connection() as conn:
        result_row = conn.execute(
            """
            SELECT result_json, computed_at
            FROM analysis_results
            WHERE sector = ? AND signal_col = ?
            """,
            (sector, signal),
        ).fetchone()
        if result_row is None:
            abort(404, description=f"No analysis found for {sector}/{signal}")

        series_rows = conn.execute(
            f"""
            SELECT p.week_start, p.pub_count, p.pub_deviation, p.pub_zscore,
                   p.pub_4w_dev, p.{signal} AS selected_signal,
                   r.ticker, r.log_return, r.spy_return, r.abnormal_return
            FROM publications_weekly p
            LEFT JOIN abnormal_returns_weekly r
              ON r.sector = p.sector AND r.week_start = p.week_start
            WHERE p.sector = ?
            ORDER BY p.week_start
            """,
            (sector,),
        ).fetchall()

    result = parse_json(result_row["result_json"], {})
    return jsonify(
        {
            "sector": sector,
            "signal": signal,
            "computed_at": result_row["computed_at"],
            "result": result,
            "series": [row_to_dict(row) for row in series_rows],
        }
    )


def _analysis_summary(row: Any, latest_publication: dict[str, Any], signal: str) -> dict[str, Any]:
    if row is None:
        return {"status": "Insufficient data", "computed_at": None, "result": None}

    result = parse_json(row["result_json"], {})
    best = result.get("best_lag_corr") or {}
    granger = result.get("granger") or []
    any_significant_granger = any(item.get("sig_05") for item in granger)
    any_significant_lag = bool(best.get("sig_05") or best.get("sig_bonf"))
    threshold = result.get("car_threshold")
    latest_value = latest_publication.get(signal)
    signal_is_elevated = (
        latest_value is not None
        and threshold is not None
        and latest_value >= threshold
    )
    supports_context = any_significant_lag or any_significant_granger
    status = "Weekly signal elevated" if signal_is_elevated and supports_context else "No weekly evidence"

    return {
        "status": status,
        "signal": row["signal_col"],
        "computed_at": row["computed_at"],
        "n_obs": result.get("n_obs"),
        "date_range": result.get("date_range"),
        "latest_value": latest_value,
        "surge_threshold": threshold,
        "signal_is_elevated": signal_is_elevated,
        "supports_historical_context": supports_context,
        "best_lag_corr": best,
        "adf": result.get("adf"),
    }


def _last_updated(sector: str) -> dict[str, Any]:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
              (SELECT MAX(created_at) FROM publications_weekly WHERE sector = ?) AS publications,
              (SELECT MAX(created_at) FROM abnormal_returns_weekly WHERE sector = ?) AS returns,
              (SELECT MAX(computed_at) FROM analysis_results WHERE sector = ?) AS analysis,
              (SELECT MAX(created_at) FROM viral_events WHERE sector = ?) AS viral_events,
              (SELECT MAX(computed_at) FROM viral_event_results WHERE sector = ?) AS viral_analysis
            """,
            (sector, sector, sector, sector, sector),
        ).fetchone()
    return row_to_dict(row)
