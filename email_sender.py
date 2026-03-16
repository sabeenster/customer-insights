from __future__ import annotations

"""
Email Sender
============
Sends HTML reports via Resend API. Falls back to saving locally if not configured.
"""

import os
import logging

import httpx

from config import RESEND_API_KEY, REPORT_FROM_EMAIL, REPORT_RECIPIENTS

log = logging.getLogger("insights.email")

RESEND_API_URL = "https://api.resend.com/emails"


async def send_report(html: str, subject: str, recipients: list[str] = None) -> dict:
    """
    Send an HTML email report via Resend.

    Returns {"success": True/False, "error": str|None}
    """
    to_list = recipients or REPORT_RECIPIENTS

    if not RESEND_API_KEY:
        log.warning("RESEND_API_KEY not set — saving report locally instead")
        return _save_locally(html, subject)

    if not to_list:
        log.warning("No recipients configured — saving report locally instead")
        return _save_locally(html, subject)

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                RESEND_API_URL,
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "from": REPORT_FROM_EMAIL,
                    "to": to_list,
                    "subject": subject,
                    "html": html,
                },
            )
            response.raise_for_status()
            data = response.json()
            log.info(f"Email sent successfully to {to_list}: {data.get('id', 'ok')}")
            return {"success": True, "error": None, "resend_id": data.get("id")}
    except httpx.HTTPStatusError as e:
        error_msg = f"Resend API error {e.response.status_code}: {e.response.text[:200]}"
        log.error(error_msg)
        _save_locally(html, subject)
        return {"success": False, "error": error_msg}
    except Exception as e:
        error_msg = f"Email send failed: {str(e)}"
        log.error(error_msg)
        _save_locally(html, subject)
        return {"success": False, "error": error_msg}


def _save_locally(html: str, subject: str) -> dict:
    """Save report HTML to a local file as fallback."""
    out_dir = os.path.join(os.path.dirname(__file__), "audit_logs")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "latest_report.html")
    with open(path, "w") as f:
        f.write(html)
    log.info(f"Report saved locally: {path}")
    return {"success": False, "error": "Email not configured — saved locally", "local_path": path}
