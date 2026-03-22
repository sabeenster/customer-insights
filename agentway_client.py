from __future__ import annotations

"""
Agentway Client
===============
Processes support ticket data from two CSV sources:

1. **Insights CSV** (from Agentway dashboard export):
   Friendly ID, Status, Customer Name, Customer Primary Identity,
   Latest Activity At, Topic Summary, Spam Verdict, Closed Reason, Topic IDs

2. **Topics CSV** (from Beekeeper SQL query):
   friendly_id, ticket_created_at, closed_at, status, summary,
   project_name, project_slug, topic_name, topic_description,
   topic_set_version, resolution_hours, message_count

The two files are merged on friendly_id to produce the richest possible dataset.
"""

import csv
import io
import os
import json
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict

log = logging.getLogger("insights.agentway")

# Path where uploaded CSV or latest data is stored
DATA_DIR = os.path.join(os.path.dirname(__file__), "audit_logs")
AGENTWAY_DATA_PATH = os.path.join(DATA_DIR, "agentway_latest.json")


def _detect_csv_format(headers: list[str]) -> str:
    """Detect whether a CSV is the 'insights' format or 'topics' format."""
    normalized = [h.strip().lower().replace(" ", "_") for h in headers]
    if "topic_summary" in normalized:
        return "insights"
    if "topic_name" in normalized:
        return "topics"
    # Fallback: check for Friendly ID (insights) vs friendly_id (topics)
    if "friendly_id" in normalized and "topic_set_version" in normalized:
        return "topics"
    return "insights"


def parse_insights_csv(csv_content: str) -> dict[str, dict]:
    """
    Parse the Agentway dashboard 'Insights' CSV export.
    Returns a dict keyed by friendly_id (numeric, no prefix).
    """
    reader = csv.DictReader(io.StringIO(csv_content))
    result = {}
    for row in reader:
        cleaned = {k.strip().lower().replace(" ", "_"): v.strip() if v else None for k, v in row.items()}
        fid = cleaned.get("friendly_id", "")
        # Strip project prefix like "FUTURE-"
        if "-" in fid:
            fid = fid.split("-", 1)[1]
        if not fid:
            continue
        result[fid] = {
            "topic_summary": cleaned.get("topic_summary"),
            "customer_name": cleaned.get("customer_name"),
            "customer_email": cleaned.get("customer_primary_identity"),
            "spam_verdict": cleaned.get("spam_verdict"),
            "closed_reason": cleaned.get("closed_reason"),
            "status": cleaned.get("status"),
            "latest_activity": cleaned.get("latest_activity_at"),
        }
    log.info(f"Insights CSV: {len(result)} tickets with topic summaries")
    return result


def parse_topics_csv(csv_content: str) -> list[dict]:
    """
    Parse the Beekeeper SQL 'Topics' CSV export into a list of ticket dicts.

    The CSV contains one row per ticket × topic × topic_set_version.
    We deduplicate by keeping only the LATEST topic_set_version per ticket,
    then aggregate multiple topics for the same ticket into a list.
    """
    reader = csv.DictReader(io.StringIO(csv_content))

    all_rows = []
    project_max_version = defaultdict(int)
    for row in reader:
        cleaned = {k.strip().lower().replace(" ", "_"): v.strip() if v else None for k, v in row.items()}
        all_rows.append(cleaned)
        project = cleaned.get("project_name") or cleaned.get("project_slug") or "unknown"
        v = _safe_int(cleaned.get("topic_set_version"))
        if v > project_max_version[project]:
            project_max_version[project] = v

    log.info(f"Topics CSV raw rows: {len(all_rows)}, projects: {dict(project_max_version)}")

    latest_rows = []
    for r in all_rows:
        project = r.get("project_name") or r.get("project_slug") or "unknown"
        v = _safe_int(r.get("topic_set_version"))
        if v == project_max_version[project]:
            latest_rows.append(r)
    log.info(f"Rows after filtering to latest version per project: {len(latest_rows)}")

    ticket_map = {}
    for row in latest_rows:
        tid = row.get("friendly_id")
        if not tid:
            continue

        if tid not in ticket_map:
            ticket_map[tid] = {
                "friendly_id": tid,
                "ticket_created_at": row.get("ticket_created_at"),
                "closed_at": row.get("closed_at"),
                "status": row.get("status"),
                "summary": row.get("summary"),
                "project_name": row.get("project_name"),
                "project_slug": row.get("project_slug"),
                "resolution_hours": row.get("resolution_hours"),
                "message_count": row.get("message_count"),
                "topics": [],
            }

        topic_name = row.get("topic_name")
        if topic_name and topic_name != "UNCATEGORIZED":
            ticket_map[tid]["topics"].append({
                "name": topic_name,
                "description": row.get("topic_description"),
            })

    tickets = list(ticket_map.values())
    log.info(f"Parsed {len(tickets)} unique tickets from Topics CSV")
    return tickets


def parse_csv(csv_content: str) -> list[dict]:
    """
    Auto-detect CSV format and parse accordingly.
    Handles both insights and topics formats as a single file.
    """
    reader = csv.DictReader(io.StringIO(csv_content))
    headers = reader.fieldnames or []
    fmt = _detect_csv_format(headers)
    log.info(f"Auto-detected CSV format: {fmt}")

    if fmt == "topics":
        return parse_topics_csv(csv_content)
    else:
        # Insights-only upload: convert to ticket list format
        insights = parse_insights_csv(csv_content)
        tickets = []
        for fid, data in insights.items():
            tickets.append({
                "friendly_id": fid,
                "status": data.get("status"),
                "topic_summary": data.get("topic_summary"),
                "topics": [],
            })
        return tickets


def merge_datasets(insights_csv: str, topics_csv: str) -> list[dict]:
    """
    Merge two CSV files into one comprehensive dataset.

    - insights_csv: rich topic summaries from Agentway dashboard
    - topics_csv: structured topic names + metrics from Beekeeper SQL

    Joins on friendly_id. Tickets in either file are included (full outer join).
    """
    insights_map = parse_insights_csv(insights_csv)
    topics_tickets = parse_topics_csv(topics_csv)

    # Build merged set from topics data (has structured metrics)
    merged = {}
    for ticket in topics_tickets:
        fid = ticket["friendly_id"]
        enrichment = insights_map.pop(fid, {})
        ticket["topic_summary"] = enrichment.get("topic_summary")
        ticket["customer_name"] = enrichment.get("customer_name")
        ticket["spam_verdict"] = enrichment.get("spam_verdict")
        ticket["closed_reason"] = enrichment.get("closed_reason")
        merged[fid] = ticket

    # Add tickets only in insights CSV (no structured topic data)
    for fid, data in insights_map.items():
        merged[fid] = {
            "friendly_id": fid,
            "status": data.get("status"),
            "topic_summary": data.get("topic_summary"),
            "customer_name": data.get("customer_name"),
            "spam_verdict": data.get("spam_verdict"),
            "closed_reason": data.get("closed_reason"),
            "latest_activity": data.get("latest_activity"),
            "topics": [],
        }

    tickets = list(merged.values())
    with_summary = sum(1 for t in tickets if t.get("topic_summary"))
    with_topics = sum(1 for t in tickets if t.get("topics"))
    log.info(f"Merged: {len(tickets)} tickets ({with_summary} with summaries, {with_topics} with structured topics)")
    return tickets


def _safe_int(val) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def compute_support_metrics(tickets: list[dict]) -> dict:
    """Compute support metrics from parsed Agentway ticket data."""
    if not tickets:
        return {"total_tickets": 0, "note": "No Agentway ticket data available"}

    total = len(tickets)

    # Topic frequency (tickets now have a "topics" list from deduplication)
    topic_counts = defaultdict(int)
    for t in tickets:
        topics = t.get("topics", [])
        if isinstance(topics, list):
            for topic_obj in topics:
                name = topic_obj.get("name") if isinstance(topic_obj, dict) else topic_obj
                if name:
                    topic_counts[name] += 1
        else:
            # Fallback for flat format
            topic = t.get("topic_name")
            if topic:
                topic_counts[topic] += 1

    top_topics = sorted(topic_counts.items(), key=lambda x: x[1], reverse=True)[:15]

    # Project breakdown
    project_counts = defaultdict(int)
    for t in tickets:
        project = t.get("project_name")
        if project:
            project_counts[project] += 1

    # Resolution time stats
    resolution_times = []
    for t in tickets:
        try:
            hours = float(t.get("resolution_hours", 0))
            if hours > 0:
                resolution_times.append(hours)
        except (ValueError, TypeError):
            pass

    avg_resolution = sum(resolution_times) / len(resolution_times) if resolution_times else None
    median_resolution = sorted(resolution_times)[len(resolution_times) // 2] if resolution_times else None

    # Message count stats (conversation complexity)
    msg_counts = []
    for t in tickets:
        try:
            mc = int(t.get("message_count", 0))
            if mc > 0:
                msg_counts.append(mc)
        except (ValueError, TypeError):
            pass

    avg_messages = sum(msg_counts) / len(msg_counts) if msg_counts else None

    # Weekly ticket volume trend
    weekly_volume = defaultdict(int)
    for t in tickets:
        closed = t.get("closed_at") or t.get("ticket_created_at")
        if closed:
            try:
                # Handle various date formats
                dt_str = closed[:10]
                dt = datetime.strptime(dt_str, "%Y-%m-%d")
                week_key = dt.strftime("%Y-W%W")
                weekly_volume[week_key] += 1
            except (ValueError, IndexError):
                pass

    sorted_weeks = sorted(weekly_volume.items())[-8:]

    # Topic trend: which topics are increasing
    recent_cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    recent_topics = defaultdict(int)
    older_topics = defaultdict(int)
    for t in tickets:
        closed = t.get("closed_at") or t.get("ticket_created_at")
        if not closed:
            continue

        # Extract topic names from the topics list
        topic_names = []
        topics = t.get("topics", [])
        if isinstance(topics, list):
            for topic_obj in topics:
                name = topic_obj.get("name") if isinstance(topic_obj, dict) else topic_obj
                if name:
                    topic_names.append(name)
        elif t.get("topic_name"):
            topic_names.append(t["topic_name"])

        if not topic_names:
            continue

        try:
            dt = datetime.strptime(closed[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            for topic in topic_names:
                if dt >= recent_cutoff:
                    recent_topics[topic] += 1
                else:
                    older_topics[topic] += 1
        except (ValueError, IndexError):
            pass

    # Find trending topics (appear more in recent 30 days vs prior period)
    trending = []
    for topic in set(list(recent_topics.keys()) + list(older_topics.keys())):
        recent = recent_topics.get(topic, 0)
        older = older_topics.get(topic, 0)
        if recent > 0:
            trending.append({
                "topic": topic,
                "recent_30d": recent,
                "prior_period": older,
                "direction": "increasing" if recent > older else "stable" if recent == older else "decreasing",
            })
    trending.sort(key=lambda x: x["recent_30d"], reverse=True)

    # Sample topic summaries for AI context — representative across top topics
    # Pick up to 2 examples per top topic, then fill remaining slots from other tickets
    topic_summaries_sample = []
    seen_ids = set()
    summaries_per_topic = defaultdict(int)
    MAX_SAMPLES = 30
    MAX_PER_TOPIC = 2

    # First pass: get examples for each top topic
    top_topic_names = {name for name, _ in top_topics}
    for t in tickets:
        if len(topic_summaries_sample) >= MAX_SAMPLES:
            break
        summary = t.get("topic_summary")
        if not summary or len(summary) < 20:
            continue
        fid = t.get("friendly_id")
        if fid in seen_ids:
            continue
        ticket_topics = [tp.get("name") for tp in t.get("topics", []) if isinstance(tp, dict)]
        matched_top = [tp for tp in ticket_topics if tp in top_topic_names]
        if matched_top and all(summaries_per_topic[tp] < MAX_PER_TOPIC for tp in matched_top):
            topic_summaries_sample.append({
                "friendly_id": fid,
                "status": t.get("status"),
                "topic_summary": summary[:500],
                "topics": ticket_topics,
            })
            seen_ids.add(fid)
            for tp in matched_top:
                summaries_per_topic[tp] += 1

    # Second pass: fill remaining slots with any other tickets that have summaries
    for t in tickets:
        if len(topic_summaries_sample) >= MAX_SAMPLES:
            break
        fid = t.get("friendly_id")
        if fid in seen_ids:
            continue
        summary = t.get("topic_summary")
        if not summary or len(summary) < 20:
            continue
        topic_summaries_sample.append({
            "friendly_id": fid,
            "status": t.get("status"),
            "topic_summary": summary[:500],
            "topics": [tp.get("name") for tp in t.get("topics", []) if isinstance(tp, dict)],
        })
        seen_ids.add(fid)

    return {
        "total_tickets": total,
        "top_topics": [{"topic": name, "count": count, "pct": round(count / total * 100, 1)} for name, count in top_topics],
        "project_breakdown": dict(project_counts),
        "resolution_time": {
            "avg_hours": round(avg_resolution, 1) if avg_resolution else None,
            "median_hours": round(median_resolution, 1) if median_resolution else None,
            "sample_size": len(resolution_times),
        },
        "conversation_complexity": {
            "avg_messages_per_ticket": round(avg_messages, 1) if avg_messages else None,
            "sample_size": len(msg_counts),
        },
        "weekly_volume": [{"week": w, "tickets": c} for w, c in sorted_weeks],
        "trending_topics": trending[:10],
        "topic_summaries_sample": topic_summaries_sample,
    }


def save_data(tickets: list[dict]):
    """Persist parsed ticket data for use by the report pipeline."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(AGENTWAY_DATA_PATH, "w") as f:
        json.dump(tickets, f, default=str)
    log.info(f"Agentway data saved: {len(tickets)} tickets")


def load_data() -> list[dict] | None:
    """Load previously saved ticket data."""
    if not os.path.exists(AGENTWAY_DATA_PATH):
        return None
    try:
        with open(AGENTWAY_DATA_PATH) as f:
            data = json.load(f)
        log.info(f"Agentway data loaded: {len(data)} tickets")
        return data
    except Exception as e:
        log.warning(f"Failed to load Agentway data: {e}")
        return None


def get_agentway_metrics() -> dict | None:
    """Load saved data and compute metrics. Returns None if no data available."""
    tickets = load_data()
    if not tickets:
        return None
    return compute_support_metrics(tickets)


# ── SQL Query Reference (Topics CSV) ─────────────────────────────────────────
# Run in Beekeeper Studio against Agentway PostgreSQL, export as CSV.
# Change p.slug for different brands. Change date filter as needed.
#
# SELECT
#     t.friendly_id,
#     t.created_at AS ticket_created_at,
#     t.closed_at,
#     t.status,
#     t.summary,
#     p.name AS project_name,
#     p.slug AS project_slug,
#     tt.name AS topic_name,
#     tt.description AS topic_description,
#     tts.version AS topic_set_version,
#     EXTRACT(EPOCH FROM (t.closed_at - t.created_at)) / 3600 AS resolution_hours,
#     (SELECT COUNT(*) FROM messages m WHERE m.ticket_id = t.id) AS message_count
# FROM tickets t
# JOIN projects p ON p.id = t.project_id
# JOIN ticket_topic_assignments tta ON tta.ticket_id = t.id
# JOIN ticket_topics tt ON tt.id = tta.topic_id
# JOIN ticket_topic_sets tts ON tts.id = tt.topic_set_id
# WHERE
#     p.slug = 'future'
#     AND t.created_at >= '2026-01-01'
# ORDER BY t.created_at DESC;
