from __future__ import annotations

"""
Audit Logger
=============
Tracks every report run with full data snapshots, prompts, responses, and email status.
Produces JSON audit logs in audit_logs/ for traceability and debugging.
"""

import os
import json
import logging
from datetime import datetime, timezone

log = logging.getLogger("insights.audit")

AUDIT_DIR = os.path.join(os.path.dirname(__file__), "audit_logs")
os.makedirs(AUDIT_DIR, exist_ok=True)


class AuditRun:
    """Tracks a single report generation run."""

    def __init__(self):
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.completed_at = None
        self.status = "running"
        self.data_sources = {}
        self.metrics_snapshot = None
        self.claude_prompt = None
        self.claude_response = None
        self.email_status = None
        self.errors = []
        log.info(f"Audit run started: {self.run_id}")

    def log_data_source(self, name: str, record_count: int, date_range: str = None, error: str = None):
        self.data_sources[name] = {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_count": record_count,
            "date_range": date_range,
            "status": "error" if error else "ok",
            "error": error,
        }
        if error:
            self.errors.append(f"Data source '{name}': {error}")
            log.warning(f"Data source '{name}' error: {error}")
        else:
            log.info(f"Data source '{name}': {record_count} records")

    def log_metrics(self, metrics: dict):
        self.metrics_snapshot = metrics

    def log_claude_prompt(self, system_prompt: str, user_prompt: str):
        self.claude_prompt = {"system": system_prompt, "user": user_prompt}

    def log_claude_response(self, response: dict):
        self.claude_response = response

    def log_email(self, recipients: list, success: bool, error: str = None):
        self.email_status = {
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "recipients": recipients,
            "success": success,
            "error": error,
        }
        if error:
            self.errors.append(f"Email: {error}")

    def log_error(self, error: str):
        self.errors.append(error)
        log.error(f"Run {self.run_id}: {error}")

    def complete(self, status: str = None):
        self.completed_at = datetime.now(timezone.utc).isoformat()
        if status:
            self.status = status
        elif self.errors:
            self.status = "partial" if self.metrics_snapshot else "failed"
        else:
            self.status = "success"
        self._save()
        log.info(f"Audit run {self.run_id} completed: {self.status}")

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "status": self.status,
            "data_sources": self.data_sources,
            "metrics_snapshot": self.metrics_snapshot,
            "claude_prompt": self.claude_prompt,
            "claude_response": self.claude_response,
            "email_status": self.email_status,
            "errors": self.errors,
        }

    def _save(self):
        path = os.path.join(AUDIT_DIR, f"{self.run_id}.json")
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2, default=str)
        log.info(f"Audit log saved: {path}")


def list_runs() -> list[dict]:
    """Return summary of all past runs, most recent first."""
    runs = []
    for fname in sorted(os.listdir(AUDIT_DIR), reverse=True):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(AUDIT_DIR, fname)
        try:
            with open(path) as f:
                data = json.load(f)
            runs.append({
                "run_id": data["run_id"],
                "started_at": data["started_at"],
                "completed_at": data.get("completed_at"),
                "status": data["status"],
                "data_sources": list(data.get("data_sources", {}).keys()),
                "error_count": len(data.get("errors", [])),
            })
        except Exception:
            continue
    return runs


def get_run(run_id: str) -> dict | None:
    """Load full audit log for a specific run."""
    path = os.path.join(AUDIT_DIR, f"{run_id}.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)
