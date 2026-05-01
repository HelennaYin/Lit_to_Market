"""Custom research-run API endpoints."""

from __future__ import annotations

import json
import os
import re
import threading
import uuid
from datetime import date, datetime, timezone
from typing import Any

from flask import Blueprint, abort, jsonify, request

from analysis.research_runner import ResearchInput, run_research
from backend.api.helpers import parse_iso_date, parse_json, row_to_dict
from backend.database import get_connection


bp = Blueprint("research", __name__, url_prefix="/api/research")

MIN_START_DATE = date(2015, 1, 1)
MIN_RANGE_DAYS = 365 * 2
TICKER_RE = re.compile(r"^[A-Z0-9.\-]{1,12}$")
RUNNING_THREADS: dict[str, threading.Thread] = {}


@bp.post("/runs")
def create_research_run():
    payload = request.get_json(silent=True) or {}
    keywords = normalize_keywords(payload.get("keywords"))
    ticker = normalize_ticker(payload.get("ticker"))
    date_start = safe_parse_date(payload.get("date_start") or payload.get("start"), "start date")
    date_end = safe_parse_date(payload.get("date_end") or payload.get("end"), "end date")
    validate_inputs(keywords, ticker, date_start, date_end)

    run_id = str(uuid.uuid4())
    submitted_at = now_iso()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO research_runs (
                id, keywords_json, ticker, date_start, date_end, status,
                progress_json, result_json, error_message, submitted_at,
                completed_at
            )
            VALUES (?, ?, ?, ?, ?, 'queued', ?, NULL, NULL, ?, NULL)
            """,
            (
                run_id,
                json.dumps(keywords),
                ticker,
                date_start.isoformat(),
                date_end.isoformat(),
                json.dumps({"stage": "queued", "pct": 0, "message": "Queued"}),
                submitted_at,
            ),
        )

    thread = threading.Thread(
        target=run_research_thread,
        args=(run_id, keywords, ticker, date_start, date_end),
        daemon=True,
    )
    RUNNING_THREADS[run_id] = thread
    thread.start()

    return jsonify({"run": get_run_or_404(run_id)}), 202


@bp.get("/runs/<run_id>")
def get_research_run(run_id: str):
    return jsonify({"run": get_run_or_404(run_id)})


@bp.get("/runs")
def list_research_runs():
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, keywords_json, ticker, date_start, date_end, status,
                   progress_json, error_message, submitted_at, completed_at
            FROM research_runs
            ORDER BY submitted_at DESC
            LIMIT 20
            """
        ).fetchall()
    return jsonify({"runs": [format_run(row_to_dict(row), include_result=False) for row in rows]})


def run_research_thread(
    run_id: str,
    keywords: list[str],
    ticker: str,
    date_start: date,
    date_end: date,
) -> None:
    update_run(run_id, status="running", progress={"stage": "start", "pct": 2, "message": "Starting"})

    def progress(stage: str, pct: int, message: str) -> None:
        update_run(run_id, status="running", progress={"stage": stage, "pct": pct, "message": message})

    try:
        result = run_research(
            ResearchInput(
                run_id=run_id,
                keywords=keywords,
                ticker=ticker,
                date_start=date_start,
                date_end=date_end,
                mailto=os.getenv("OPENALEX_EMAIL", "research@litmarket.io"),
            ),
            progress=progress,
        )
    except Exception as exc:
        update_run(
            run_id,
            status="failed",
            progress={"stage": "failed", "pct": 100, "message": "Run failed"},
            error_message=str(exc),
            completed_at=now_iso(),
        )
    else:
        update_run(
            run_id,
            status="completed",
            progress={"stage": "complete", "pct": 100, "message": "Run complete"},
            result=result,
            completed_at=now_iso(),
        )
    finally:
        RUNNING_THREADS.pop(run_id, None)


def update_run(
    run_id: str,
    *,
    status: str,
    progress: dict[str, Any] | None = None,
    result: dict[str, Any] | None = None,
    error_message: str | None = None,
    completed_at: str | None = None,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE research_runs
            SET status = ?,
                progress_json = COALESCE(?, progress_json),
                result_json = COALESCE(?, result_json),
                error_message = ?,
                completed_at = COALESCE(?, completed_at)
            WHERE id = ?
            """,
            (
                status,
                json.dumps(progress) if progress is not None else None,
                json.dumps(result) if result is not None else None,
                error_message,
                completed_at,
                run_id,
            ),
        )


def get_run_or_404(run_id: str) -> dict[str, Any]:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, keywords_json, ticker, date_start, date_end, status,
                   progress_json, result_json, error_message, submitted_at,
                   completed_at
            FROM research_runs
            WHERE id = ?
            """,
            (run_id,),
        ).fetchone()
    if row is None:
        abort(404, description=f"Unknown research run: {run_id}")
    return format_run(row_to_dict(row), include_result=True)


def format_run(row: dict[str, Any], include_result: bool) -> dict[str, Any]:
    run = {
        "id": row.get("id"),
        "keywords": parse_json(row.get("keywords_json"), []),
        "ticker": row.get("ticker"),
        "date_start": row.get("date_start"),
        "date_end": row.get("date_end"),
        "status": row.get("status"),
        "progress": parse_json(row.get("progress_json"), {}),
        "error_message": row.get("error_message"),
        "submitted_at": row.get("submitted_at"),
        "completed_at": row.get("completed_at"),
    }
    if include_result:
        run["result"] = parse_json(row.get("result_json"), None)
    return run


def normalize_keywords(value: Any) -> list[str]:
    if isinstance(value, str):
        raw = value.splitlines()
    elif isinstance(value, list):
        raw = value
    else:
        raw = []
    keywords = []
    seen = set()
    for item in raw:
        keyword = " ".join(str(item or "").strip().split())
        if len(keyword) < 2:
            continue
        key = keyword.lower()
        if key not in seen:
            keywords.append(keyword)
            seen.add(key)
    return keywords[:12]


def normalize_ticker(value: Any) -> str:
    return str(value or "").strip().upper()


def safe_parse_date(value: Any, label: str) -> date | None:
    try:
        return parse_iso_date(str(value) if value else None)
    except ValueError:
        abort(400, description=f"Invalid {label}; use YYYY-MM-DD")


def validate_inputs(
    keywords: list[str],
    ticker: str,
    date_start: date | None,
    date_end: date | None,
) -> None:
    if not keywords:
        abort(400, description="Enter at least one keyword or title phrase")
    if not ticker or not TICKER_RE.match(ticker):
        abort(400, description="Ticker must be 1-12 letters, numbers, dots, or dashes")
    if date_start is None or date_end is None:
        abort(400, description="Start and end dates are required")
    today = date.today()
    if date_start < MIN_START_DATE:
        abort(400, description="Start date cannot be earlier than 2015-01-01")
    if date_end > today:
        abort(400, description=f"End date cannot be later than {today.isoformat()}")
    if date_end <= date_start:
        abort(400, description="End date must be after start date")
    if (date_end - date_start).days < MIN_RANGE_DAYS:
        abort(400, description="Date range must be at least two years")


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
