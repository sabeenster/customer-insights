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
TOPIC_MAP_PATH = os.path.join(DATA_DIR, "topic_mapping.json")

# In-memory topic UUID → name mapping
_topic_map: dict[str, dict] = {}


def load_topic_mapping() -> dict[str, dict]:
    """Load topic UUID → name mapping from disk."""
    global _topic_map
    if _topic_map:
        return _topic_map
    if os.path.exists(TOPIC_MAP_PATH):
        with open(TOPIC_MAP_PATH) as f:
            _topic_map = json.load(f)
        log.info(f"Topic mapping loaded: {len(_topic_map)} topics")
    return _topic_map


def save_topic_mapping_csv(csv_content: str) -> dict:
    """
    Parse and save a topic mapping CSV (id, name, description).
    Returns summary stats.
    """
    global _topic_map
    reader = csv.DictReader(io.StringIO(csv_content))
    mapping = {}
    for row in reader:
        cleaned = {k.strip().lower(): (v.strip() if v else "") for k, v in row.items()}
        topic_id = cleaned.get("id", "").strip()
        name = cleaned.get("name", "").strip()
        if topic_id and name:
            mapping[topic_id] = {
                "name": name,
                "description": cleaned.get("description", ""),
            }
    if mapping:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(TOPIC_MAP_PATH, "w") as f:
            json.dump(mapping, f)
        _topic_map = mapping
        log.info(f"Topic mapping saved: {len(mapping)} topics")
    return {"topics_loaded": len(mapping)}


def resolve_topic_ids(topic_ids: list[str]) -> list[dict]:
    """Resolve topic UUIDs to human-readable names using the loaded mapping."""
    mapping = load_topic_mapping()
    if not mapping:
        return []
    resolved = []
    for tid in topic_ids:
        entry = mapping.get(tid)
        if entry:
            resolved.append({"name": entry["name"], "description": entry.get("description", "")})
    return resolved


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
        # Parse Topic IDs (comma-separated UUIDs from Agentway dashboard)
        topic_ids_raw = cleaned.get("topic_ids") or ""
        topic_ids = [tid.strip() for tid in topic_ids_raw.split(",") if tid.strip()]

        result[fid] = {
            "topic_summary": cleaned.get("topic_summary"),
            "customer_name": cleaned.get("customer_name"),
            "customer_email": cleaned.get("customer_primary_identity"),
            "spam_verdict": cleaned.get("spam_verdict"),
            "closed_reason": cleaned.get("closed_reason"),
            "status": cleaned.get("status"),
            "latest_activity": cleaned.get("latest_activity_at"),
            "first_message_at": cleaned.get("first_message_at"),
            "topic_ids": topic_ids,
        }
    log.info(f"Insights CSV: {len(result)} tickets with topic summaries")
    return result


def parse_topics_csv(csv_content: str) -> list[dict]:
    """
    Parse the Beekeeper SQL 'Topics' CSV export into a list of ticket dicts.

    The CSV contains one row per ticket × topic × topic_set_version.
    For each ticket, we use topics from its HIGHEST available version
    (not the global max — so older tickets classified under earlier
    versions are preserved rather than dropped).
    Multiple topics per ticket are aggregated into a list.
    """
    reader = csv.DictReader(io.StringIO(csv_content))

    all_rows = []
    # Track the highest version each ticket appears in
    ticket_max_version = defaultdict(int)
    for row in reader:
        cleaned = {k.strip().lower().replace(" ", "_"): v.strip() if v else None for k, v in row.items()}
        all_rows.append(cleaned)
        tid = cleaned.get("friendly_id")
        if tid:
            v = _safe_int(cleaned.get("topic_set_version"))
            if v > ticket_max_version[tid]:
                ticket_max_version[tid] = v

    log.info(f"Topics CSV raw rows: {len(all_rows)}, unique tickets: {len(ticket_max_version)}")

    # For each ticket, keep only rows from its highest available version
    best_rows = []
    for r in all_rows:
        tid = r.get("friendly_id")
        if not tid:
            continue
        v = _safe_int(r.get("topic_set_version"))
        if v == ticket_max_version[tid]:
            best_rows.append(r)
    log.info(f"Rows after per-ticket version selection: {len(best_rows)}")

    ticket_map = {}
    for row in best_rows:
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
    log.info(f"Parsed {len(tickets)} unique tickets from Topics CSV (0 dropped)")
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
        # Insights/dashboard upload: has Topic Summary (rich) + Topic IDs (UUIDs)
        insights = parse_insights_csv(csv_content)
        tickets = []
        for fid, data in insights.items():
            # Resolve Topic IDs to human-readable names using mapping
            topic_ids = data.get("topic_ids", [])
            topics = resolve_topic_ids(topic_ids) if topic_ids else []
            tickets.append({
                "friendly_id": fid,
                "status": data.get("status"),
                "topic_summary": data.get("topic_summary"),
                "first_message_at": data.get("first_message_at"),
                "latest_activity": data.get("latest_activity"),
                "topics": topics,  # Resolved names if mapping loaded, else empty
            })
        resolved_count = sum(1 for t in tickets if t["topics"])
        log.info(f"Dashboard CSV: {len(tickets)} tickets, {resolved_count} with resolved topics")
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

    # Filter out UUID-style topic names (not human-readable)
    import re
    uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
    filtered_counts = {k: v for k, v in topic_counts.items() if not uuid_pattern.match(k)}
    top_topics = sorted(filtered_counts.items(), key=lambda x: x[1], reverse=True)[:15]

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
                iso = dt.isocalendar()
                week_key = f"{iso[0]}-W{iso[1]:02d}"
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

    # Sample topic summaries for AI context
    # If no structured topics exist, increase sample size so Claude can identify themes
    has_structured_topics = len(top_topics) > 0 and not all(
        len(name) > 30 for name, _ in top_topics  # UUID-length names = not real topic names
    )
    topic_summaries_sample = []
    seen_ids = set()
    summaries_per_topic = defaultdict(int)
    MAX_SAMPLES = 30 if has_structured_topics else 50  # More samples when Claude must identify themes
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

    # Date range from actual data
    all_dates = []
    for t in tickets:
        for date_field in ["ticket_created_at", "first_message_at", "latest_activity"]:
            d = t.get(date_field, "")
            if d and len(d) >= 10:
                all_dates.append(d[:10])
    all_dates.sort()
    date_range_start = all_dates[0] if all_dates else None
    date_range_end = all_dates[-1] if all_dates else None

    return {
        "total_tickets": total,
        "date_range_start": date_range_start,
        "date_range_end": date_range_end,
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
