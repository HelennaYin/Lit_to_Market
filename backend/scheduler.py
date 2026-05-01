"""Background scheduler for LitMarket refresh jobs."""

from __future__ import annotations

import logging
import os
from zoneinfo import ZoneInfo

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
except ImportError:  # pragma: no cover - dependency is installed in Docker
    BackgroundScheduler = None
    CronTrigger = None

from backend.pipelines.refresh_database import refresh_database


log = logging.getLogger(__name__)
_scheduler: BackgroundScheduler | None = None


def start_scheduler() -> BackgroundScheduler | None:
    """Start the 2am refresh scheduler when explicitly enabled."""
    global _scheduler
    if os.getenv("LITMARKET_ENABLE_SCHEDULER", "").lower() not in {"1", "true", "yes"}:
        return None
    if BackgroundScheduler is None or CronTrigger is None:
        log.warning("LITMARKET_ENABLE_SCHEDULER is set, but APScheduler is not installed.")
        return None
    if _scheduler and _scheduler.running:
        return _scheduler

    timezone_name = os.getenv("LITMARKET_SCHEDULER_TIMEZONE", "America/New_York")
    hour = int(os.getenv("LITMARKET_REFRESH_HOUR", "2"))
    minute = int(os.getenv("LITMARKET_REFRESH_MINUTE", "0"))

    scheduler = BackgroundScheduler(timezone=ZoneInfo(timezone_name))
    scheduler.add_job(
        scheduled_refresh,
        trigger=CronTrigger(hour=hour, minute=minute, timezone=ZoneInfo(timezone_name)),
        id="litmarket_refresh_database",
        name="LitMarket database refresh",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=60 * 60,
    )
    scheduler.start()
    _scheduler = scheduler
    log.info("LitMarket scheduler enabled: refresh_database daily at %02d:%02d %s", hour, minute, timezone_name)
    return scheduler


def scheduled_refresh() -> None:
    """Run the normal incremental refresh with no attention-score cap."""
    log.info("Scheduled LitMarket refresh started")
    try:
        summary = refresh_database(
            dry_run=False,
            skip_sources=False,
            skip_weekly=False,
            skip_market=False,
            skip_viral_seed=True,
            skip_nightly_radar=False,
            nightly_days=1,
            nightly_max_pages=int(os.getenv("LITMARKET_NIGHTLY_MAX_PAGES", "1")),
            nightly_max_attention_scores=None,
            nightly_skip_attention=False,
            max_weekly_weeks=int(os.getenv("LITMARKET_MAX_WEEKLY_WEEKS", "26")),
        )
    except Exception:
        log.exception("Scheduled LitMarket refresh failed")
        return
    log.info("Scheduled LitMarket refresh completed: %s", summary)
