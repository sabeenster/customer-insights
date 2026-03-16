"""
Scheduler
=========
APScheduler-based weekly report scheduling.
Runs inside the FastAPI process for simplicity.
"""

import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import REPORT_SCHEDULE_DAY, REPORT_SCHEDULE_HOUR, REPORT_SCHEDULE_MINUTE

log = logging.getLogger("insights.scheduler")

scheduler = AsyncIOScheduler()


def start_scheduler(report_callback):
    """
    Start the weekly report scheduler.

    Args:
        report_callback: async function to call when the schedule fires.
                         Should be the pipeline's generate_and_send_report().
    """
    trigger = CronTrigger(
        day_of_week=REPORT_SCHEDULE_DAY,
        hour=REPORT_SCHEDULE_HOUR,
        minute=REPORT_SCHEDULE_MINUTE,
    )

    scheduler.add_job(
        report_callback,
        trigger=trigger,
        id="weekly_report",
        replace_existing=True,
        name="Weekly Customer Insights Report",
    )

    scheduler.start()
    log.info(
        f"Scheduler started: reports every {REPORT_SCHEDULE_DAY} "
        f"at {REPORT_SCHEDULE_HOUR:02d}:{REPORT_SCHEDULE_MINUTE:02d}"
    )


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        log.info("Scheduler stopped")
