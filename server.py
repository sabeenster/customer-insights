"""
Customer Insights Server
========================
FastAPI application that orchestrates the full insights pipeline:
  fetch data → compute metrics → AI analysis → build report → send email

Endpoints:
  GET  /health              — connection status for all services
  POST /generate-report     — manually trigger a report generation
  GET  /reports/latest       — view the most recently generated report HTML
  GET  /audit/runs           — list all past pipeline runs
  GET  /audit/{run_id}       — full audit log for a specific run
  POST /upload/agentway-csv  — upload Agentway CSV export for next report
"""

import os
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse

from config import PORT, DEFAULT_LOOKBACK_DAYS
from shopify_client import ShopifyClient
from agentway_client import parse_csv, merge_datasets, save_data, get_agentway_metrics, compute_support_metrics
from richpanel_client import parse_richpanel_csv, compute_richpanel_metrics
from analysis_engine import AnalysisEngine
from report_builder import build_report
from email_sender import send_report
from audit_logger import AuditRun, list_runs, get_run
from scheduler import start_scheduler, stop_scheduler

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("insights.log"),
    ],
)
log = logging.getLogger("insights.server")

# Clients (initialized once)
shopify = ShopifyClient()
analysis = AnalysisEngine()

# In-memory store for latest report
latest_report_html = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start scheduler on boot, stop on shutdown."""
    start_scheduler(generate_and_send_report)
    log.info("Customer Insights server started")
    yield
    stop_scheduler()
    log.info("Customer Insights server stopped")


app = FastAPI(
    title="Customer Insights",
    description="AI-powered customer insights for brand executives",
    lifespan=lifespan,
)


# ── Pipeline ──────────────────────────────────────────────────────────────────

async def generate_and_send_report():
    """Full pipeline: fetch → compute → analyze → build → send."""
    global latest_report_html

    audit = AuditRun()
    log.info(f"=== Report generation started (run: {audit.run_id}) ===")

    try:
        # 1. Load support ticket data from CSV (Agentway or Rich Panel format)
        agentway_data = None
        try:
            from agentway_client import load_data
            raw_tickets = load_data()
            if raw_tickets and len(raw_tickets) > 0:
                # Detect source: Rich Panel tickets have "_source": "richpanel"
                is_richpanel = any(t.get("_source") == "richpanel" for t in raw_tickets)
                if is_richpanel:
                    agentway_data = compute_richpanel_metrics(raw_tickets)
                    audit.log_data_source("Rich Panel", agentway_data["total_tickets"])
                    log.info(f"Rich Panel data loaded: {agentway_data['total_tickets']} tickets")
                else:
                    agentway_data = compute_support_metrics(raw_tickets)
                    audit.log_data_source("Agentway", agentway_data["total_tickets"])
            else:
                audit.log_data_source("Support Data", 0, error="No CSV data uploaded yet")
        except Exception as e:
            audit.log_data_source("Support Data", 0, error=str(e))
            log.warning(f"Data load failed: {e}")

        if not agentway_data:
            audit.log_error("No Agentway data available — cannot generate report")
            audit.complete("failed")
            log.error("Report generation aborted: no support ticket data")
            return

        # 1b. Optionally fetch lightweight Shopify summary (if configured)
        shopify_summary = None
        if shopify.is_configured():
            try:
                shopify_summary = await shopify.get_summary_metrics(days_back=DEFAULT_LOOKBACK_DAYS)
                if shopify_summary:
                    audit.log_data_source("Shopify", shopify_summary["current_period"]["total_orders"])
                    log.info(f"Shopify summary loaded: {shopify_summary['current_period']['total_orders']} orders")
            except Exception as e:
                log.warning(f"Shopify summary skipped: {e}")
        else:
            log.info("Shopify not configured — generating report from support data only")

        # 2. Generate AI insights from support ticket data + optional Shopify context
        result = await analysis.generate_insights(agentway_data, shopify_summary=shopify_summary)
        audit.log_claude_prompt(result["system_prompt"], result["user_prompt"])
        audit.log_claude_response(result["raw_response"])
        insights = result["insights"]

        # 3. Build HTML report
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        period_start = (now - timedelta(days=DEFAULT_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        period_end = now.strftime("%Y-%m-%d")
        html = build_report(
            insights=insights,
            run_id=audit.run_id,
            data_sources=audit.data_sources,
            period_start=period_start,
            period_end=period_end,
        )
        latest_report_html = html

        # Also save locally always
        report_path = os.path.join(os.path.dirname(__file__), "audit_logs", "latest_report.html")
        with open(report_path, "w") as f:
            f.write(html)

        # 4. Send email
        subject = f"Customer Insights Report — {period_start} to {period_end}"
        email_result = await send_report(html, subject)
        audit.log_email(
            recipients=email_result.get("recipients", []),
            success=email_result.get("success", False),
            error=email_result.get("error"),
        )

        audit.complete()
        log.info(f"=== Report generation complete (run: {audit.run_id}, status: {audit.status}) ===")

    except Exception as e:
        audit.log_error(f"Pipeline failed: {str(e)}")
        audit.complete("failed")
        log.error(f"=== Report generation FAILED (run: {audit.run_id}): {e} ===")
        raise


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """Health check with connection status for all services."""
    shopify_status = await shopify.health_check()

    # Check if Agentway CSV data is available
    agentway_metrics = get_agentway_metrics()
    agentway_status = (
        {"status": "data_loaded", "tickets": agentway_metrics["total_tickets"]}
        if agentway_metrics and agentway_metrics.get("total_tickets", 0) > 0
        else {"status": "no_data", "hint": "Upload CSV via POST /upload/agentway-csv"}
    )

    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "services": {
            "shopify": shopify_status,
            "agentway": agentway_status,
            "email": {"status": "configured" if os.getenv("RESEND_API_KEY") else "not_configured"},
        },
    }


@app.post("/generate-report")
async def trigger_report(background_tasks: BackgroundTasks):
    """Manually trigger a report generation. Runs in the background."""
    background_tasks.add_task(generate_and_send_report)
    return {
        "status": "generating",
        "message": "Report generation started. Check /reports/latest or /audit/runs for results.",
    }


@app.get("/reports/latest", response_class=HTMLResponse)
async def get_latest_report():
    """View the most recently generated report."""
    if latest_report_html:
        return HTMLResponse(content=latest_report_html)

    # Try loading from disk
    report_path = os.path.join(os.path.dirname(__file__), "audit_logs", "latest_report.html")
    if os.path.exists(report_path):
        with open(report_path) as f:
            return HTMLResponse(content=f.read())

    return HTMLResponse(
        content="<h1>No report generated yet</h1><p>POST to /generate-report to create one.</p>",
        status_code=404,
    )


@app.get("/audit/runs")
async def audit_runs():
    """List all past pipeline runs."""
    return {"runs": list_runs()}


@app.get("/audit/{run_id}")
async def audit_detail(run_id: str):
    """Full audit log for a specific run."""
    data = get_run(run_id)
    if not data:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    return data


@app.post("/upload/agentway-csv")
async def upload_agentway_csv(file: UploadFile = File(...), project: str = None):
    """
    Upload a single CSV (either insights or topics format).
    Auto-detects the format. For best results, use POST /upload/insights instead.
    """
    content = await file.read()
    csv_text = content.decode("utf-8-sig")

    tickets = parse_csv(csv_text)
    if not tickets:
        return JSONResponse(
            {"error": "No tickets found in CSV. Check column headers match expected format."},
            status_code=400,
        )

    if project:
        tickets = [t for t in tickets if t.get("project_name", "").lower() == project.lower()]
        if not tickets:
            return JSONResponse(
                {"error": f"No tickets found for project '{project}'"},
                status_code=400,
            )

    save_data(tickets)
    metrics = compute_support_metrics(tickets)

    log.info(f"Single CSV uploaded: {len(tickets)} tickets")
    return {
        "status": "accepted",
        "tickets_parsed": len(tickets),
        "top_topics": metrics.get("top_topics", [])[:5],
        "message": "Data saved. It will be included in the next report generation.",
    }


@app.post("/upload/insights")
async def upload_insights(
    insights_file: UploadFile = File(..., description="Agentway dashboard CSV with Topic Summary"),
    topics_file: UploadFile = File(..., description="Beekeeper SQL CSV with topic_name, resolution_hours, etc."),
):
    """
    Upload TWO CSV files for the most comprehensive analysis:
      - insights_file: Agentway dashboard export (has rich Topic Summary per ticket)
      - topics_file: Beekeeper SQL export (has topic_name, resolution_hours, message_count)

    The files are merged on friendly_id to combine rich summaries with structured metrics.
    Then automatically generates a report and emails it.
    """
    insights_content = await insights_file.read()
    topics_content = await topics_file.read()

    insights_csv = insights_content.decode("utf-8-sig")
    topics_csv = topics_content.decode("utf-8-sig")

    tickets = merge_datasets(insights_csv, topics_csv)
    if not tickets:
        return JSONResponse(
            {"error": "No tickets found after merging. Check CSV column headers."},
            status_code=400,
        )

    # Filter out spam
    tickets = [t for t in tickets if t.get("spam_verdict") != "spam"]

    save_data(tickets)
    metrics = compute_support_metrics(tickets)

    with_summary = sum(1 for t in tickets if t.get("topic_summary"))
    with_topics = sum(1 for t in tickets if t.get("topics"))

    log.info(f"Merged upload: {len(tickets)} tickets ({with_summary} with summaries, {with_topics} with topics)")
    return {
        "status": "accepted",
        "tickets_merged": len(tickets),
        "with_topic_summaries": with_summary,
        "with_structured_topics": with_topics,
        "top_topics": metrics.get("top_topics", [])[:5],
        "message": "Data merged and saved. Use POST /generate-report to create the report.",
    }


@app.post("/upload/richpanel-csv")
async def upload_richpanel_csv(file: UploadFile = File(...)):
    """
    Upload a Rich Panel CSV export.
    Parses conversations, computes aggregate metrics (no API calls).
    Then use POST /generate-report to create the insights report.
    """
    content = await file.read()
    csv_text = content.decode("utf-8-sig")

    tickets = parse_richpanel_csv(csv_text)
    if not tickets:
        return JSONResponse(
            {"error": "No tickets found in CSV. Check this is a Rich Panel export."},
            status_code=400,
        )

    save_data(tickets)
    metrics = compute_richpanel_metrics(tickets)

    log.info(f"Rich Panel CSV uploaded: {len(tickets)} tickets")
    return {
        "status": "accepted",
        "source": "richpanel",
        "tickets_parsed": len(tickets),
        "channel_breakdown": metrics.get("channel_breakdown", []),
        "assignee_breakdown": metrics.get("assignee_breakdown", []),
        "status_breakdown": metrics.get("status_breakdown", {}),
        "message": "Data saved. Use POST /generate-report to create the report.",
    }


# ── Inbound Email Webhook (Resend) ────────────────────────────────────────────

# Brand name → project slug mapping
BRAND_MAP = {
    "future kind": "future",
    "futurekind": "future",
    "emme": "emme",
    "emme mama": "emme",
    "emmemama": "emme",
    "dippin daisy": "dippindaisys",
    "dippindaisy": "dippindaisys",
    "big moods": "bigmoods",
    "bigmoods": "bigmoods",
    "knkg": "knkg",
}


def _detect_brand_from_subject(subject: str) -> str | None:
    """Extract brand slug from email subject line. Case-insensitive."""
    subject_lower = subject.lower().strip()
    for brand_name, slug in BRAND_MAP.items():
        if brand_name in subject_lower:
            return slug
    return None


@app.post("/webhook/inbound-email")
async def inbound_email(request: Request, background_tasks: BackgroundTasks):
    """
    Receive inbound emails from Resend webhook.
    Expects email to otto@agentway.com with a CSV attachment.
    Subject line must contain a brand name (e.g., "Future Kind Topics").

    Resend inbound webhook payload:
    {
      "from": "sender@example.com",
      "to": "otto@agentway.com",
      "subject": "Future Kind Topics",
      "attachments": [{"filename": "...", "content": "base64..."}]
    }
    """
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON payload"}, status_code=400)

    sender = payload.get("from", "")
    subject = payload.get("subject", "")
    attachments = payload.get("attachments", [])

    log.info(f"Inbound email from {sender}, subject: '{subject}', attachments: {len(attachments)}")

    # Detect brand from subject
    brand_slug = _detect_brand_from_subject(subject)
    if not brand_slug:
        log.warning(f"Could not detect brand from subject: '{subject}'")
        return JSONResponse(
            {"error": f"Could not detect brand from subject '{subject}'. Include one of: Future Kind, Emme, Dippin Daisy, Big Moods, KNKG"},
            status_code=400,
        )

    # Find CSV attachment
    csv_attachment = None
    for att in attachments:
        filename = (att.get("filename") or "").lower()
        if filename.endswith(".csv"):
            csv_attachment = att
            break

    if not csv_attachment:
        return JSONResponse(
            {"error": "No CSV attachment found. Attach a .csv file with ticket data."},
            status_code=400,
        )

    # Decode attachment (Resend sends base64-encoded content)
    import base64
    try:
        csv_bytes = base64.b64decode(csv_attachment["content"])
        csv_text = csv_bytes.decode("utf-8-sig")
    except Exception as e:
        return JSONResponse({"error": f"Could not decode CSV: {e}"}, status_code=400)

    # Parse CSV (auto-detect format)
    tickets = parse_csv(csv_text)
    if not tickets:
        return JSONResponse({"error": "No tickets found in CSV."}, status_code=400)

    # Filter to brand if project data exists
    if any(t.get("project_slug") for t in tickets):
        filtered = [t for t in tickets if (t.get("project_slug") or "").lower() == brand_slug]
        if filtered:
            tickets = filtered

    save_data(tickets)
    log.info(f"Inbound email: {len(tickets)} tickets saved for brand '{brand_slug}', triggering report")

    # Auto-generate report
    background_tasks.add_task(generate_and_send_report)

    return {
        "status": "accepted",
        "brand": brand_slug,
        "tickets_parsed": len(tickets),
        "message": f"CSV processed for {brand_slug}. Report generating and will be emailed.",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
