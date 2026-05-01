"""Shared helpers for read-only LitMarket API endpoints."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import Any

from flask import abort

from backend.database import get_connection


VALID_SIGNALS = {"pub_deviation", "pub_zscore", "pub_4w_dev"}


def row_to_dict(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def parse_json(value: str | None, fallback: Any = None) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def get_sector_or_404(sector: str) -> dict[str, Any]:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT sector, label, weekly_ticker, viral_ticker,
                   keywords_json, created_at, updated_at
            FROM sectors
            WHERE sector = ?
            """,
            (sector,),
        ).fetchone()
    if row is None:
        abort(404, description=f"Unknown sector: {sector}")
    data = row_to_dict(row)
    data["keywords"] = parse_json(data.pop("keywords_json"), [])
    return data


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value[:10], "%Y-%m-%d").date()


def recent_start_date(anchor: str | None, days: int) -> str | None:
    parsed = parse_iso_date(anchor)
    if parsed is None:
        return None
    return (parsed - timedelta(days=max(days, 1) - 1)).isoformat()


def doi_url(doi: str | None) -> str | None:
    if not doi:
        return None
    return f"https://doi.org/{doi}"


def clamp_days(value: str | None, default: int = 5, max_days: int = 365) -> int:
    if value is None:
        return default
    try:
        days = int(value)
    except ValueError:
        abort(400, description="days must be an integer")
    if days < 1:
        abort(400, description="days must be at least 1")
    return min(days, max_days)
