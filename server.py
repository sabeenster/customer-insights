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
from agentway_client import parse_csv, save_data, get_agentway_metrics, compute_support_metrics
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
        # 1. Fetch Shopify orders only (customer metrics derived from orders)
        orders = []
        try:
            orders = await shopify.fetch_orders(DEFAULT_LOOKBACK_DAYS)
            audit.log_data_source(
                "Shopify Orders", len(orders),
                date_range=f"last {DEFAULT_LOOKBACK_DAYS} days",
            )
        except Exception as e:
            audit.log_data_source("Shopify Orders", 0, error=str(e))
            log.error(f"Shopify orders fetch failed: {e}")

        # 2. Compute metrics (customer metrics derived from orders — no separate fetch)
        metrics = shopify.compute_metrics(orders, DEFAULT_LOOKBACK_DAYS)
        audit.log_metrics(metrics)

        # 3. Load Agentway data from CSV (graceful degradation)
        agentway_data = None
        try:
            agentway_metrics = get_agentway_metrics()
            if agentway_metrics and agentway_metrics.get("total_tickets", 0) > 0:
                agentway_data = agentway_metrics
                audit.log_data_source("Agentway", agentway_metrics["total_tickets"])
            else:
                audit.log_data_source("Agentway", 0, error="No CSV data uploaded yet — upload via POST /upload/agentway-csv")
        except Exception as e:
            audit.log_data_source("Agentway", 0, error=str(e))
            log.warning(f"Agentway data load failed (non-blocking): {e}")

        # 4. Generate AI insights
        result = await analysis.generate_insights(metrics, agentway_data)
        audit.log_claude_prompt(result["system_prompt"], result["user_prompt"])
        audit.log_claude_response(result["raw_response"])
        insights = result["insights"]

        # 5. Build HTML report
        period = metrics.get("period", {})
        html = build_report(
            insights=insights,
            run_id=audit.run_id,
            data_sources=audit.data_sources,
            period_start=period.get("start", "?"),
            period_end=period.get("end", "?"),
        )
        latest_report_html = html

        # Also save locally always
        report_path = os.path.join(os.path.dirname(__file__), "audit_logs", "latest_report.html")
        with open(report_path, "w") as f:
            f.write(html)

        # 6. Send email
        subject = f"Customer Insights Report — {period.get('start', '')} to {period.get('end', '')}"
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
async def upload_agentway_csv(file: UploadFile = File(...)):
    """
    Upload an Agentway CSV export from the SQL query.
    The CSV is parsed, metrics are computed, and data is saved for the next report.
    """
    content = await file.read()
    csv_text = content.decode("utf-8-sig")  # Handle BOM from Excel/Beekeeper exports

    tickets = parse_csv(csv_text)
    if not tickets:
        return JSONResponse(
            {"error": "No tickets found in CSV. Check column headers match expected format."},
            status_code=400,
        )

    save_data(tickets)
    metrics = compute_support_metrics(tickets)

    log.info(f"Agentway CSV uploaded: {len(tickets)} tickets, {len(metrics.get('top_topics', []))} topics")
    return {
        "status": "accepted",
        "tickets_parsed": len(tickets),
        "top_topics": metrics.get("top_topics", [])[:5],
        "message": "Data saved. It will be included in the next report generation.",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
