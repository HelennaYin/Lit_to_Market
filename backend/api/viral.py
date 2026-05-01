"""Viral-paper radar and event-study endpoints."""

from __future__ import annotations

import statistics
import math
import random
from collections import defaultdict
from datetime import date, datetime
from typing import Any

from flask import Blueprint, jsonify, request

from backend.api.helpers import (
    clamp_days,
    doi_url,
    get_sector_or_404,
    parse_json,
    row_to_dict,
)
from backend.database import get_connection

try:
    from scipy import stats as scipy_stats
except ImportError:  # pragma: no cover - optional precision dependency
    scipy_stats = None


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
        event_rows = conn.execute(
            """
            SELECT v.id AS viral_event_id, v.event_date
            FROM viral_events v
            WHERE v.sector = ?
              AND EXISTS (
                SELECT 1
                FROM event_windows w
                WHERE w.viral_event_id = v.id
                  AND w.day_relative = 5
              )
            ORDER BY v.event_date
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
        market_rows = conn.execute(
            """
            SELECT date, log_return
            FROM market_daily
            WHERE sector = ?
            ORDER BY date
            """,
            (sector,),
        ).fetchall()
        spy_rows = conn.execute(
            """
            SELECT date, log_return
            FROM spy_daily
            ORDER BY date
            """
        ).fetchall()

    car5 = [row_to_dict(row) for row in car5_rows]
    for item in car5:
        item["doi_url"] = doi_url(item.get("doi"))
    control_test = _control_test(
        sector=sector,
        event_rows=[row_to_dict(row) for row in event_rows],
        market_rows=[row_to_dict(row) for row in market_rows],
        spy_rows=[row_to_dict(row) for row in spy_rows],
    )

    return jsonify(
        {
            "sector": sector,
            "computed_at": summary_row["computed_at"] if summary_row else None,
            "summary": parse_json(summary_row["result_json"], {}) if summary_row else {},
            "car_curve": _car_curve_stats(curve_rows),
            "car_5d_distribution": {
                "events": car5,
                "stats": _distribution_stats([row.get("car_5d") for row in car5]),
            },
            "attention_vs_car_5d": car5,
            "volatility_event_study": _volatility_summary(volatility_rows),
            "control_test": control_test,
            "conclusion": _viral_conclusion(control_test),
        }
    )


def get_recent_viral_events(sector: str | None = None, days: int = 5) -> dict[str, Any]:
    if sector:
        get_sector_or_404(sector)

    today = date.today().isoformat()
    start_date = (date.today().toordinal() - max(days, 1) + 1)
    start_date = date.fromordinal(start_date).isoformat()
    with get_connection() as conn:
        signal_count = conn.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM radar_signals
            WHERE signal_date BETWEEN ? AND ?
              {"AND sector = ?" if sector else ""}
            """,
            [start_date, today] + ([sector] if sector else []),
        ).fetchone()["n"]

        if signal_count:
            rows = conn.execute(
                f"""
                SELECT COALESCE(v.id, r.id) AS viral_event_id,
                       r.sector,
                       COALESCE(v.event_date, r.publication_date) AS event_date,
                       r.cas, r.threshold_value,
                       p.id AS paper_id, p.title, p.doi, p.publication_date,
                       p.detected_date, p.keyword, p.source_display_name,
                       p.cited_by_count,
                       a.reddit_hits, a.wiki_hits, a.citation_count,
                       a.cit_velocity, a.age_days,
                       r.historical_car_5d, r.historical_n,
                       r.historical_pval, r.days_remaining,
                       NULL AS viral_result_json
                FROM radar_signals r
                JOIN papers p ON p.id = r.paper_id_fk
                LEFT JOIN attention_scores a ON a.paper_id_fk = p.id
                LEFT JOIN viral_events v
                  ON v.paper_id_fk = p.id
                 AND v.sector = r.sector
                 AND v.is_historical = 0
                WHERE r.signal_date BETWEEN ? AND ?
                  {"AND r.sector = ?" if sector else ""}
                ORDER BY r.signal_date DESC, r.cas DESC, p.title
                """,
                [start_date, today] + ([sector] if sector else []),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT v.id AS viral_event_id, v.sector, v.event_date, v.cas,
                       v.threshold_value, p.id AS paper_id, p.title, p.doi,
                       p.publication_date, p.detected_date, p.keyword,
                       p.source_display_name, p.cited_by_count,
                       a.reddit_hits, a.wiki_hits, a.citation_count,
                       a.cit_velocity, a.age_days,
                       NULL AS historical_car_5d, NULL AS historical_n,
                       NULL AS historical_pval, NULL AS days_remaining,
                       r.result_json AS viral_result_json
                FROM viral_events v
                JOIN papers p ON p.id = v.paper_id_fk
                LEFT JOIN attention_scores a ON a.paper_id_fk = p.id
                LEFT JOIN viral_event_results r ON r.sector = v.sector
                WHERE p.publication_date BETWEEN ? AND ?
                  {"AND v.sector = ?" if sector else ""}
                ORDER BY p.publication_date DESC, v.cas DESC, p.title
                """,
                [start_date, today] + ([sector] if sector else []),
            ).fetchall()

        event_ids = [row["viral_event_id"] for row in rows if row["viral_event_id"]]
        windows = _event_windows(conn, event_ids) if not signal_count else {}

    events = []
    for row in rows:
        item = row_to_dict(row)
        result = parse_json(item.pop("viral_result_json"), {})
        car_5d = result.get("car_5d") or {}
        item["doi_url"] = doi_url(item.get("doi"))
        item["historical_context"] = {
            "mean_car_5d": item.pop("historical_car_5d", None) or car_5d.get("mean_car"),
            "n_events": item.pop("historical_n", None) or car_5d.get("n_events"),
            "p_value": item.pop("historical_pval", None) or car_5d.get("p_value"),
            "note": car_5d.get("note"),
        }
        item["event_window"] = windows.get(item["viral_event_id"], [])
        events.append(item)

    return {
        "sector": sector,
        "window": {
            "days": days,
            "start_date": start_date,
            "end_date": today,
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
        return {
            "n": 0,
            "mean": None,
            "median": None,
            "min": None,
            "max": None,
            "t_stat": None,
            "p_value": None,
            "test": "one-sample t-test vs zero",
        }
    t_stat, p_value = _one_sample_ttest(clean, 0.0)
    return {
        "n": len(clean),
        "mean": sum(clean) / len(clean),
        "median": statistics.median(clean),
        "min": min(clean),
        "max": max(clean),
        "t_stat": t_stat,
        "p_value": p_value,
        "significance": _significance(p_value),
        "test": "one-sample t-test vs zero",
    }


def _volatility_summary(rows: list[Any]) -> dict[str, Any]:
    events = [row_to_dict(row) for row in rows]
    diffs = [
        item["post_abs_ar"] - item["pre_abs_ar"]
        for item in events
        if item.get("pre_abs_ar") is not None and item.get("post_abs_ar") is not None
    ]
    t_stat, p_value = _paired_ttest(
        [item.get("post_abs_ar") for item in events],
        [item.get("pre_abs_ar") for item in events],
    )
    return {
        "events": events,
        "stats": {
            "n": len(events),
            "mean_pre_abs_ar": _mean(item.get("pre_abs_ar") for item in events),
            "mean_post_abs_ar": _mean(item.get("post_abs_ar") for item in events),
            "mean_change": (sum(diffs) / len(diffs)) if diffs else None,
            "paired_t_test": {
                "t_stat": t_stat,
                "p_value": p_value,
                "significance": _significance(p_value),
                "test": "paired t-test of post-event vs pre-event absolute abnormal return",
            },
        },
    }


def _mean(values: Any) -> float | None:
    clean = [value for value in values if value is not None]
    return (sum(clean) / len(clean)) if clean else None


def _car_curve_stats(rows: list[Any]) -> list[dict[str, Any]]:
    curve = []
    for row in rows:
        item = row_to_dict(row)
        n = item.get("n") or 0
        mean_car = item.get("mean_car")
        # SQLite aggregate rows do not include per-event variance, so p-values
        # for each day are computed from day-level samples in the control test.
        item["y_axis"] = "Mean cumulative abnormal return"
        item["x_axis"] = "Trading days relative to viral paper publication"
        item["n"] = n
        item["mean_car"] = mean_car
        curve.append(item)
    return curve


def _control_test(
    sector: str,
    event_rows: list[dict[str, Any]],
    market_rows: list[dict[str, Any]],
    spy_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    if not event_rows or len(market_rows) < 90:
        return {"available": False, "note": "Not enough events or market data for control test."}

    real_dates = [row["event_date"] for row in event_rows if row.get("event_date")]
    real_windows = _build_market_windows(real_dates, market_rows, spy_rows)
    control_dates = _sample_control_dates(sector, len(real_dates), market_rows)
    control_windows = _build_market_windows(control_dates, market_rows, spy_rows)

    if not real_windows or not control_windows:
        return {"available": False, "note": "No complete real/control windows could be built."}

    curve = []
    for day in sorted(set(real_windows) | set(control_windows)):
        real_vals = real_windows.get(day, [])
        control_vals = control_windows.get(day, [])
        curve.append(
            {
                "day_relative": day,
                "real_mean_car": _mean(real_vals),
                "control_mean_car": _mean(control_vals),
                "real_n": len(real_vals),
                "control_n": len(control_vals),
            }
        )

    real_5 = real_windows.get(5, [])
    control_5 = control_windows.get(5, [])
    t_stat, p_value = _two_sample_ttest(real_5, control_5)
    real_mean = _mean(real_5)
    control_mean = _mean(control_5)
    diff = (
        real_mean - control_mean
        if real_mean is not None and control_mean is not None
        else None
    )

    return {
        "available": True,
        "method": "Real viral-paper event dates compared with deterministic randomized control dates from the same sector price history.",
        "window": {"pre_days": 3, "post_days": 5},
        "curve": curve,
        "day_5": {
            "real_mean_car": real_mean,
            "control_mean_car": control_mean,
            "difference": diff,
            "real_n": len(real_5),
            "control_n": len(control_5),
            "t_stat": t_stat,
            "p_value": p_value,
            "significance": _significance(p_value),
            "test": "two-sample t-test of real CAR+5 vs randomized control CAR+5",
        },
    }


def _sample_control_dates(sector: str, n_dates: int, market_rows: list[dict[str, Any]]) -> list[str]:
    dates = [row["date"] for row in market_rows if row.get("date")]
    if len(dates) < 80:
        return []
    valid = dates[70:-8] or dates
    rng = random.Random(f"litmarket-control-{sector}")
    return [rng.choice(valid) for _ in range(n_dates)]


def _build_market_windows(
    event_dates: list[str],
    market_rows: list[dict[str, Any]],
    spy_rows: list[dict[str, Any]],
) -> dict[int, list[float]]:
    dates = [row["date"] for row in market_rows]
    returns = [row.get("log_return") for row in market_rows]
    spy_by_date = {row["date"]: row.get("log_return") for row in spy_rows}
    by_day: dict[int, list[float]] = defaultdict(list)

    for event_date in event_dates:
        event_idx = _nearest_date_idx(dates, event_date)
        if event_idx is None:
            continue
        alpha, beta = _market_model_params(dates, returns, spy_by_date, event_idx)
        start = max(0, event_idx - 3)
        end = min(len(dates) - 1, event_idx + 5)
        n_pre = event_idx - start
        car = 0.0
        for idx in range(start, end + 1):
            sector_return = returns[idx]
            if sector_return is None:
                continue
            spy_return = spy_by_date.get(dates[idx]) or 0.0
            abnormal_return = sector_return - alpha - beta * spy_return
            car += abnormal_return
            by_day[idx - start - n_pre].append(car)
    return by_day


def _nearest_date_idx(dates: list[str], event_date: str) -> int | None:
    target = _date_ordinal(event_date)
    if target is None:
        return None
    best_idx = None
    best_diff = None
    for idx, value in enumerate(dates):
        current = _date_ordinal(value)
        if current is None:
            continue
        diff = abs(current - target)
        if best_diff is None or diff < best_diff:
            best_idx = idx
            best_diff = diff
    return best_idx if best_diff is not None and best_diff <= 5 else None


def _date_ordinal(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").toordinal()
    except ValueError:
        return None


def _market_model_params(
    dates: list[str],
    returns: list[float | None],
    spy_by_date: dict[str, float | None],
    event_idx: int,
) -> tuple[float, float]:
    start = max(0, event_idx - 200)
    end = max(0, event_idx - 20)
    pairs = [
        (spy_by_date.get(dates[idx]), returns[idx])
        for idx in range(start, end)
        if returns[idx] is not None and spy_by_date.get(dates[idx]) is not None
    ]
    if len(pairs) < 60:
        sample = [value for value in returns[max(0, event_idx - 60):event_idx] if value is not None]
        return ((_mean(sample) or 0.0), 0.0)
    xs = [x for x, _ in pairs]
    ys = [y for _, y in pairs]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    var_x = sum((x - mean_x) ** 2 for x in xs)
    if var_x == 0:
        return mean_y, 0.0
    cov = sum((x - mean_x) * (y - mean_y) for x, y in pairs)
    beta = cov / var_x
    alpha = mean_y - beta * mean_x
    return alpha, beta


def _one_sample_ttest(values: list[float], mean: float = 0.0) -> tuple[float | None, float | None]:
    clean = [value for value in values if value is not None]
    if len(clean) < 2:
        return None, None
    if scipy_stats is not None:
        stat, p_value = scipy_stats.ttest_1samp(clean, mean)
        return float(stat), float(p_value)
    sample_mean = sum(clean) / len(clean)
    sample_std = statistics.stdev(clean)
    if sample_std == 0:
        return None, None
    t_stat = (sample_mean - mean) / (sample_std / math.sqrt(len(clean)))
    return t_stat, _normal_two_sided_p(t_stat)


def _paired_ttest(after: list[float | None], before: list[float | None]) -> tuple[float | None, float | None]:
    pairs = [(a, b) for a, b in zip(after, before) if a is not None and b is not None]
    if len(pairs) < 2:
        return None, None
    diffs = [a - b for a, b in pairs]
    return _one_sample_ttest(diffs, 0.0)


def _two_sample_ttest(a_values: list[float], b_values: list[float]) -> tuple[float | None, float | None]:
    a = [value for value in a_values if value is not None]
    b = [value for value in b_values if value is not None]
    if len(a) < 2 or len(b) < 2:
        return None, None
    if scipy_stats is not None:
        stat, p_value = scipy_stats.ttest_ind(a, b, equal_var=False)
        return float(stat), float(p_value)
    mean_a = sum(a) / len(a)
    mean_b = sum(b) / len(b)
    var_a = statistics.variance(a)
    var_b = statistics.variance(b)
    denom = math.sqrt(var_a / len(a) + var_b / len(b))
    if denom == 0:
        return None, None
    t_stat = (mean_a - mean_b) / denom
    return t_stat, _normal_two_sided_p(t_stat)


def _normal_two_sided_p(stat: float) -> float:
    return math.erfc(abs(stat) / math.sqrt(2))


def _significance(p_value: float | None) -> str:
    if p_value is None:
        return "not computed"
    if p_value < 0.01:
        return "p < 0.01"
    if p_value < 0.05:
        return "p < 0.05"
    if p_value < 0.10:
        return "p < 0.10"
    return "not significant"


def _viral_conclusion(control_test: dict[str, Any]) -> dict[str, Any]:
    day_5 = control_test.get("day_5") or {}
    diff = day_5.get("difference")
    p_value = day_5.get("p_value")
    if diff is None or p_value is None:
        text = "No control-test inference is available for this sector."
        direction = "unknown"
    elif p_value < 0.05 and diff > 0:
        text = (
            "Historically, viral-paper dates had higher CAR+5 than randomized "
            "control dates. This supports a positive short-term market-reaction "
            "signal, not a guaranteed forecast."
        )
        direction = "increase"
    elif p_value < 0.05 and diff < 0:
        text = (
            "Historically, viral-paper dates had lower CAR+5 than randomized "
            "control dates. This supports a negative short-term market-reaction "
            "signal, not a guaranteed forecast."
        )
        direction = "decrease"
    else:
        text = "Real viral-paper CAR+5 is not statistically distinguishable from randomized control dates."
        direction = "no reliable difference"
    return {
        "direction": direction,
        "text": text,
        "p_value": p_value,
        "difference": diff,
        "basis": "real-vs-control CAR+5 comparison",
    }
