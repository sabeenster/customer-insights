from __future__ import annotations

"""
Analysis Engine
===============
Uses Claude API to generate executive-quality, data-grounded customer insights.
Every insight must cite specific numbers from the provided metrics.
"""

import json
import logging
import asyncio

import httpx

from config import ANTHROPIC_API_KEY, ANTHROPIC_API_URL, CLAUDE_MODEL

log = logging.getLogger("insights.analysis")

SYSTEM_PROMPT = """You are a senior customer experience consultant preparing a weekly executive briefing for a brand CEO.
Your job is to analyze customer support ticket data and surface actionable insights about what customers are experiencing.

CRITICAL RULES:
1. Every insight MUST reference specific numbers from the data provided. Never invent or estimate statistics.
2. If the data is insufficient to support a conclusion, explicitly say so rather than guessing.
3. Cite the metric name and value for every claim (e.g., "Order Issues represent 24.1% of all tickets").
4. Compare numbers where possible (e.g., trending topics vs prior period — only if the data supports it).
5. Be direct and specific. No filler, no generic business advice that could apply to any company.

STYLE:
- Write for a CEO who is busy but sharp. Lead with the most important finding.
- Use plain language, not jargon. If you must use a term, define it briefly.
- Be honest about what the data shows, even if it's unflattering.
- Prioritize actionable observations over descriptive summaries.
- Focus on WHAT customers are struggling with and WHY it matters to the business.

OUTPUT FORMAT:
Return a JSON object with this exact structure:
{
  "sections": [
    {
      "id": "section_id",
      "title": "Section Title",
      "content_html": "<p>HTML content with <strong>bold</strong> for key numbers...</p>",
      "severity": "info|positive|warning|critical",
      "confidence": "high|medium|low",
      "based_on": "Description of data source and key metrics used"
    }
  ]
}

REQUIRED SECTIONS (in order):
1. executive_summary — 3-5 bullet points of the most important findings about customer experience. Severity reflects overall CX health.
2. top_issues — The biggest customer pain points by volume. What are customers complaining about most? Break down the top topics with percentages.
3. trending_topics — What's getting worse or better? Highlight topics that are spiking or declining vs the prior period.
4. resolution_quality — Average resolution times, conversation complexity (messages per ticket). Are issues being resolved efficiently?
5. recommended_actions — 2-3 specific, prioritized actions with expected impact. Each must tie back to a data point from the support data.

SEVERITY GUIDE:
- "positive": metric is good or improving
- "info": neutral observation, for awareness
- "warning": metric declining or approaching a threshold
- "critical": requires immediate attention

CONFIDENCE GUIDE:
- "high": based on large sample size and clear trend
- "medium": based on limited data or short time window
- "low": based on very small sample, single data point, or missing context"""


class AnalysisEngine:
    def __init__(self):
        self.http = httpx.AsyncClient(timeout=120.0)

    async def _api_call_with_retry(self, payload: dict, max_retries: int = 5) -> dict:
        """Make an Anthropic API call with retry on rate limits. Adapted from enrichment_agent.py."""
        response = None
        for attempt in range(max_retries):
            try:
                response = await self.http.post(
                    ANTHROPIC_API_URL,
                    headers={
                        "x-api-key": ANTHROPIC_API_KEY,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json=payload,
                )
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout) as e:
                wait = 10 * (attempt + 1)
                log.warning(f"Connection error: {e}, waiting {wait}s (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(wait)
                continue

            if response.status_code in (429, 529):
                try:
                    retry_after = int(response.headers.get("retry-after", "60"))
                except (ValueError, TypeError):
                    retry_after = 60
                wait = max(retry_after, 30 * (attempt + 1))
                log.warning(f"Rate limited ({response.status_code}), waiting {wait}s (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(wait)
                continue

            if response.status_code >= 500:
                wait = 15 * (attempt + 1)
                log.warning(f"Server error ({response.status_code}), waiting {wait}s (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(wait)
                continue

            response.raise_for_status()
            return response.json()

        status = response.status_code if response else "no response"
        raise Exception(f"Failed after {max_retries} retries (last status: {status})")

    async def generate_insights(self, agentway_data: dict) -> dict:
        """
        Generate AI-powered insights from support ticket data.

        Returns the full prompt and response for audit logging, plus parsed sections.
        """
        user_prompt = self._build_user_prompt(agentway_data)

        payload = {
            "model": CLAUDE_MODEL,
            "max_tokens": 8192,
            "system": SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_prompt}],
        }

        log.info(f"Generating insights with {CLAUDE_MODEL}...")
        raw_response = await self._api_call_with_retry(payload)

        # Extract text content from Claude response
        text = ""
        for block in raw_response.get("content", []):
            if block.get("type") == "text":
                text += block["text"]

        # Parse JSON from response
        try:
            # Handle case where Claude wraps JSON in markdown code blocks
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0]
            elif "```" in text:
                text = text.split("```")[1].split("```")[0]
            insights = json.loads(text.strip())
        except json.JSONDecodeError as e:
            log.error(f"Failed to parse Claude response as JSON: {e}")
            log.error(f"Text length: {len(text)}, first 200 chars: {text[:200]}")
            log.error(f"Last 200 chars: {text[-200:]}")
            insights = {
                "sections": [{
                    "id": "error",
                    "title": "Analysis Error",
                    "content_html": f"<p>The analysis engine returned a non-structured response. Raw output has been saved in the audit log.</p>",
                    "severity": "warning",
                    "confidence": "low",
                    "based_on": "N/A — parsing error",
                }],
                "raw_text": text,
            }

        return {
            "system_prompt": SYSTEM_PROMPT,
            "user_prompt": user_prompt,
            "raw_response": raw_response,
            "insights": insights,
        }

    def _build_user_prompt(self, agentway_data: dict) -> str:
        parts = []

        parts.append("# Customer Support Insights Data\n")
        parts.append(f"Total tickets analyzed: {agentway_data.get('total_tickets', 0)}\n")

        # Structured metrics (topic counts, trends, resolution times)
        metrics_data = {k: v for k, v in agentway_data.items() if k != "topic_summaries_sample"}
        parts.append("## Structured Metrics (topic counts, trends, resolution times)\n")
        parts.append(json.dumps(metrics_data, indent=2))

        # Topic summaries — rich context for deeper understanding
        summaries = agentway_data.get("topic_summaries_sample", [])
        if summaries:
            parts.append("\n## Sample Ticket Summaries (real customer conversations)\n")
            parts.append("These are detailed summaries of actual support conversations. Use them to understand")
            parts.append("the NATURE and NUANCE of customer issues beyond what topic names alone reveal.\n")
            for s in summaries:
                topics_str = ", ".join(s.get("topics", [])) or "uncategorized"
                parts.append(f"**Ticket {s['friendly_id']}** ({s.get('status', 'unknown')}) — Topics: {topics_str}")
                parts.append(f"> {s['topic_summary']}\n")

        parts.append("\n---\n")
        parts.append("Analyze this support ticket data and return the JSON response as specified in your instructions.")
        parts.append("Use the structured metrics for quantitative claims (percentages, counts, trends).")
        if summaries:
            parts.append("Use the ticket summaries to add qualitative depth — what are customers actually saying and feeling?")
        parts.append("Remember: every insight must cite specific numbers. Do not invent statistics.")

        return "\n".join(parts)
