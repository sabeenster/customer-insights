"""
Report Builder
==============
Renders the insight sections into an HTML email using Jinja2.
"""

import os
import logging
from datetime import datetime, timezone

from jinja2 import Environment, FileSystemLoader

log = logging.getLogger("insights.report")

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=False)


def build_report(
    insights: dict,
    run_id: str,
    data_sources: dict,
    period_start: str,
    period_end: str,
) -> str:
    """Render insights into an HTML email report."""

    sections = insights.get("sections", [])

    template = env.get_template("report_email.html")
    html = template.render(
        sections=sections,
        run_id=run_id,
        data_sources=data_sources,
        period_start=period_start,
        period_end=period_end,
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    )

    log.info(f"Report built: {len(sections)} sections, {len(html)} chars")
    return html
