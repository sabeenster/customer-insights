from __future__ import annotations

"""
Rich Panel Client
=================
Parses Rich Panel CSV exports and computes aggregate metrics.
NO Claude API calls during parsing — all metrics are deterministic.

Expected CSV columns:
  ConversationId, ConversationNo, Topic, Email, FirstName, LastName,
  PhoneNumber, ConversationCreatedAt, ConversationSubject, Assignee,
  Tags, ConversationType, ConversationStatus, Rating, CustomerFeedback,
  ConversationFirstClosedAt, FirstResponseTime, ConversationUrl,
  conversation, privateNotes, postUrl, Reason for return,
  Reason for exchange, Which size do you want for the replacement?
"""

import csv
import io
import sys
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict

log = logging.getLogger("insights.richpanel")

# Rich Panel conversations can be very large
csv.field_size_limit(sys.maxsize)

# Date format used by Rich Panel: "06-Jun-2024 09:09:36"
RP_DATE_FORMAT = "%d-%b-%Y %H:%M:%S"


def parse_richpanel_csv(csv_content: str) -> list[dict]:
    """
    Parse a Rich Panel CSV export into normalized ticket dicts
    compatible with the existing insights pipeline.
    """
    reader = csv.DictReader(io.StringIO(csv_content))
    tickets = []

    for row in reader:
        conv_id = (row.get("ConversationId") or "").strip()
        if not conv_id:
            continue

        # Parse dates
        created_at = _parse_rp_date(row.get("ConversationCreatedAt", ""))
        closed_at = _parse_rp_date(row.get("ConversationFirstClosedAt", ""))

        # Extract conversation snippet (first 500 chars, no full transcript)
        conversation = (row.get("conversation") or "").strip()
        snippet = conversation[:500] if conversation else ""

        # Parse first response time (Rich Panel format varies)
        frt = (row.get("FirstResponseTime") or "").strip()

        tickets.append({
            "friendly_id": (row.get("ConversationNo") or conv_id).strip(),
            "conversation_id": conv_id,
            "ticket_created_at": created_at,
            "closed_at": closed_at,
            "status": (row.get("ConversationStatus") or "").strip(),
            "summary": (row.get("ConversationSubject") or "").strip()[:200],
            "topic_summary": snippet,
            "topics": [],  # No pre-classification
            "channel": (row.get("ConversationType") or "").strip(),
            "assignee": (row.get("Assignee") or "").strip(),
            "first_response_time": frt,
            "rating": (row.get("Rating") or "").strip(),
            "customer_feedback": (row.get("CustomerFeedback") or "").strip(),
            "tags": (row.get("Tags") or "").strip(),
            "return_reason": (row.get("Reason for return") or "").strip(),
            "exchange_reason": (row.get("Reason for exchange") or "").strip(),
            # Mark source for pipeline awareness
            "_source": "richpanel",
        })

    log.info(f"Rich Panel CSV: {len(tickets)} tickets parsed")
    return tickets


def compute_richpanel_metrics(tickets: list[dict]) -> dict:
    """
    Compute aggregate metrics from Rich Panel ticket data.
    All calculations are deterministic Python — no LLM involved.
    """
    if not tickets:
        return {"total_tickets": 0, "note": "No Rich Panel data"}

    total = len(tickets)

    # Status breakdown
    status_counts = defaultdict(int)
    for t in tickets:
        status_counts[t.get("status", "unknown")] += 1

    # Channel breakdown
    channel_counts = defaultdict(int)
    for t in tickets:
        ch = t.get("channel", "unknown")
        channel_counts[ch] += 1
    channel_breakdown = [
        {"channel": ch, "count": c, "pct": round(c / total * 100, 1)}
        for ch, c in sorted(channel_counts.items(), key=lambda x: -x[1])
    ]

    # Assignee breakdown
    assignee_counts = defaultdict(int)
    for t in tickets:
        a = t.get("assignee") or "Unassigned"
        assignee_counts[a] += 1
    assignee_breakdown = [
        {"assignee": a, "count": c, "pct": round(c / total * 100, 1)}
        for a, c in sorted(assignee_counts.items(), key=lambda x: -x[1])
    ]

    # Weekly volume trend
    weekly_volume = defaultdict(int)
    monthly_volume = defaultdict(int)
    for t in tickets:
        created = t.get("ticket_created_at")
        if not created:
            continue
        try:
            dt = datetime.strptime(created[:10], "%Y-%m-%d")
            weekly_volume[dt.strftime("%Y-W%W")] += 1
            monthly_volume[dt.strftime("%Y-%m")] += 1
        except (ValueError, IndexError):
            pass
    sorted_weeks = sorted(weekly_volume.items())[-12:]
    sorted_months = sorted(monthly_volume.items())[-6:]

    # First response time stats (where available)
    frt_values = []
    for t in tickets:
        frt = t.get("first_response_time", "")
        if frt:
            parsed = _parse_duration(frt)
            if parsed is not None:
                frt_values.append(parsed)

    frt_stats = None
    if frt_values:
        frt_values.sort()
        frt_stats = {
            "avg_minutes": round(sum(frt_values) / len(frt_values), 1),
            "median_minutes": round(frt_values[len(frt_values) // 2], 1),
            "sample_size": len(frt_values),
        }

    # Rating distribution
    rating_counts = defaultdict(int)
    for t in tickets:
        r = t.get("rating", "")
        if r:
            rating_counts[r] += 1
    rating_dist = dict(rating_counts) if rating_counts else None

    # Return/exchange reasons
    return_reasons = defaultdict(int)
    exchange_reasons = defaultdict(int)
    for t in tickets:
        rr = t.get("return_reason", "")
        if rr:
            return_reasons[rr] += 1
        er = t.get("exchange_reason", "")
        if er:
            exchange_reasons[er] += 1

    # Conversation samples — stratified by channel for representative coverage
    conversation_samples = _sample_conversations(tickets, channel_counts, max_samples=50)

    return {
        "total_tickets": total,
        "data_source": "richpanel",
        "status_breakdown": dict(status_counts),
        "channel_breakdown": channel_breakdown,
        "assignee_breakdown": assignee_breakdown,
        "weekly_volume": [{"week": w, "tickets": c} for w, c in sorted_weeks],
        "monthly_volume": [{"month": m, "tickets": c} for m, c in sorted_months],
        "first_response_time": frt_stats,
        "rating_distribution": rating_dist,
        "return_reasons": dict(return_reasons) if return_reasons else None,
        "exchange_reasons": dict(exchange_reasons) if exchange_reasons else None,
        "conversation_samples": conversation_samples,
    }


def _sample_conversations(
    tickets: list[dict], channel_counts: dict, max_samples: int = 50
) -> list[dict]:
    """
    Sample conversations stratified by channel for representative coverage.
    Returns subject + snippet for Claude to identify themes qualitatively.
    """
    samples = []
    seen = set()

    # Allocate samples proportional to channel volume
    total_allocated = 0
    channel_allocations = {}
    for ch, count in sorted(channel_counts.items(), key=lambda x: -x[1]):
        alloc = max(2, round(count / sum(channel_counts.values()) * max_samples))
        channel_allocations[ch] = alloc
        total_allocated += alloc

    # Collect samples per channel
    for t in tickets:
        if len(samples) >= max_samples:
            break
        ch = t.get("channel", "unknown")
        fid = t.get("friendly_id")
        if fid in seen:
            continue
        if channel_allocations.get(ch, 0) <= 0:
            continue

        summary = t.get("summary", "")
        snippet = t.get("topic_summary", "")
        if not summary and not snippet:
            continue

        samples.append({
            "friendly_id": fid,
            "channel": ch,
            "status": t.get("status"),
            "subject": summary[:200],
            "conversation_snippet": snippet[:300],
        })
        seen.add(fid)
        channel_allocations[ch] -= 1

    return samples


def _parse_rp_date(date_str: str) -> str | None:
    """Parse Rich Panel date format to ISO string."""
    date_str = (date_str or "").strip()
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, RP_DATE_FORMAT)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        # Try ISO format as fallback
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None


def _parse_duration(duration_str: str) -> float | None:
    """Parse Rich Panel duration strings (various formats) to minutes."""
    duration_str = duration_str.strip()
    if not duration_str:
        return None

    # Try parsing as minutes directly
    try:
        return float(duration_str)
    except ValueError:
        pass

    # Try "Xh Ym" or "X hours Y minutes" patterns
    import re
    hours = 0
    minutes = 0
    h_match = re.search(r"(\d+)\s*h", duration_str, re.IGNORECASE)
    m_match = re.search(r"(\d+)\s*m", duration_str, re.IGNORECASE)
    if h_match:
        hours = int(h_match.group(1))
    if m_match:
        minutes = int(m_match.group(1))
    if hours or minutes:
        return hours * 60 + minutes

    return None
