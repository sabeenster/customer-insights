# Customer Insights

AI-powered support ticket analysis for brand executives. Analyzes Agentway support data and generates executive-quality insights reports delivered via email.

## How It Works

1. Export two CSVs from Agentway/Beekeeper (see SQL queries below)
2. Upload both files via one curl command
3. Claude analyzes the data and generates an executive report
4. Report is emailed to configured recipients

**All insights are data-grounded** — every claim cites specific numbers from the uploaded data. No Shopify or external data is pulled unless you explicitly add it later.

## Quick Start

### 1. Export CSVs

**Insights CSV** — Export from Agentway dashboard (Tickets > Export). Contains rich `Topic Summary` per ticket.

**Topics CSV** — Run this SQL in Beekeeper Studio against Agentway PostgreSQL:

```sql
SELECT
    t.friendly_id,
    t.created_at AS ticket_created_at,
    t.closed_at,
    t.status,
    t.summary,
    p.name AS project_name,
    p.slug AS project_slug,
    tt.name AS topic_name,
    tt.description AS topic_description,
    tts.version AS topic_set_version,
    EXTRACT(EPOCH FROM (t.closed_at - t.created_at)) / 3600 AS resolution_hours,
    (SELECT COUNT(*) FROM messages m WHERE m.ticket_id = t.id) AS message_count
FROM tickets t
JOIN projects p ON p.id = t.project_id
JOIN ticket_topic_assignments tta ON tta.ticket_id = t.id
JOIN ticket_topics tt ON tt.id = tta.topic_id
JOIN ticket_topic_sets tts ON tts.id = tt.topic_set_id
WHERE
    p.slug = 'future'              -- Change for different brand
    AND t.created_at >= '2026-01-01'  -- Change date range as needed
ORDER BY t.created_at DESC;
```

**Project slugs:** `future` (Future Kind), `dip` (Dippin Daisy), etc.

### 2. Upload + Generate Report

**Two-file upload (recommended — richest insights):**
```bash
curl -X POST https://customer-insights-production.up.railway.app/upload/insights \
  -F "insights_file=@/path/to/insights-export.csv" \
  -F "topics_file=@/path/to/beekeeper-topics.csv" \
&& curl -X POST https://customer-insights-production.up.railway.app/generate-report
```

**Single-file upload (works with either format):**
```bash
curl -X POST https://customer-insights-production.up.railway.app/upload/agentway-csv \
  -F "file=@/path/to/any-csv.csv" \
&& curl -X POST https://customer-insights-production.up.railway.app/generate-report
```

### 3. View Results

- **Email** — Report is sent to configured recipients automatically
- **Web** — `https://customer-insights-production.up.railway.app/reports/latest`
- **Audit** — `https://customer-insights-production.up.railway.app/audit/runs`

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Connection status for all services |
| POST | `/upload/insights` | Upload two CSVs (insights + topics) for merged analysis |
| POST | `/upload/agentway-csv` | Upload single CSV (auto-detects format) |
| POST | `/generate-report` | Trigger report generation (runs in background) |
| GET | `/reports/latest` | View most recent report HTML |
| GET | `/audit/runs` | List all past pipeline runs |
| GET | `/audit/{run_id}` | Full audit log for a specific run |

## Anti-Hallucination Guarantees

The system is designed to prevent fabricated insights:

1. **Data-grounded prompts** — Claude receives only pre-computed metrics (counts, percentages, trends) calculated deterministically from your CSV data. All math happens in Python, not in the LLM.

2. **Citation requirement** — The system prompt requires every insight to cite specific metric names and values. Claims without data backing are flagged as low confidence.

3. **Confidence scoring** — Each report section has a confidence level (high/medium/low) based on sample size and data completeness.

4. **Full audit trail** — Every run logs: the exact data sent to Claude, the full prompt, the raw response, and the parsed output. Available at `/audit/{run_id}`.

5. **Deterministic metrics** — Topic counts, percentages, resolution times, weekly volumes, and trend comparisons are all computed in `agentway_client.py` before Claude sees them. Claude interprets; it does not calculate.

6. **Representative sampling** — When topic summaries are included, the system samples up to 2 per top topic category to ensure balanced coverage rather than biased selection.

## Architecture

```
CSV Upload → Parse & Merge → Compute Metrics → Claude Analysis → HTML Report → Email
                                    ↓
                              Audit Log (full prompt + response)
```

**Files:**
- `server.py` — FastAPI app, endpoints, pipeline orchestration
- `agentway_client.py` — CSV parsing, merging, metric computation
- `analysis_engine.py` — Claude API integration, prompt construction
- `report_builder.py` — HTML report rendering (Jinja2)
- `email_sender.py` — Resend API email delivery
- `audit_logger.py` — Run logging and audit trail
- `config.py` — Environment variable loading
- `scheduler.py` — Optional scheduled report generation

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ANTHROPIC_API_KEY` | Yes | Claude API key |
| `RESEND_API_KEY` | No | Resend email API key (falls back to local file) |
| `REPORT_RECIPIENTS` | No | Comma-separated email addresses |
| `REPORT_FROM_EMAIL` | No | Sender email (default: insights@yourdomain.com) |
| `SHOPIFY_STORE_URL` | No | Reserved for future Shopify integration |
| `SHOPIFY_ACCESS_TOKEN` | No | Reserved for future Shopify integration |

## Deployment

Deployed on **Railway** with auto-deploy from GitHub (`sabeenster/customer-insights`, main branch).

- Runtime: Python 3.13
- Start command: `uvicorn server:app --host 0.0.0.0 --port $PORT` (via Procfile)
- Region: europe-west4
