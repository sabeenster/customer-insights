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

SYSTEM_PROMPT = """You are a senior customer analytics consultant preparing a weekly executive briefing for a brand CEO.

CRITICAL RULES:
1. Every insight MUST reference specific numbers from the data provided. Never invent or estimate statistics.
2. If the data is insufficient to support a conclusion, explicitly say "Insufficient data for this analysis" rather than guessing.
3. Cite the metric name and value for every claim (e.g., "repeat purchase rate of 34.2%").
4. Compare numbers where possible (e.g., "up from 28.1% last month" — only if the data supports it).
5. Be direct and specific. No filler, no generic business advice that could apply to any company.
6. If a data source was unavailable, clearly state which sections are affected and why.

STYLE:
- Write for a CEO who is busy but sharp. Lead with the most important finding.
- Use plain language, not jargon. If you must use a term, define it briefly.
- Be honest about what the data shows, even if it's unflattering.
- Prioritize actionable observations over descriptive summaries.

OUTPUT FORMAT:
Return a JSON object with this exact structure:
{
  "sections": [
    {
      "id": "executive_summary",
      "title": "Executive Summary",
      "content_html": "<p>HTML content with <strong>bold</strong> for key numbers...</p>",
      "severity": "info|positive|warning|critical",
      "confidence": "high|medium|low",
      "based_on": "Description of data source and key metrics used"
    },
    ...additional sections...
  ]
}

REQUIRED SECTIONS (in order):
1. executive_summary — 3-5 bullet points, most important findings. Severity reflects overall health.
2. retention_health — Repeat purchase rate, cohort retention, churn signals. This is the most important section for customer behavior.
3. revenue_patterns — AOV trends, revenue concentration, product mix.
4. customer_behavior — Purchase frequency changes, segment movements, new vs returning ratio.
5. support_signals — Only if Agentway/support data is provided. Topic spikes, sentiment shifts, emerging issues.
6. recommended_actions — 2-3 specific, prioritized actions with expected impact. Each must tie back to a data point.

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

    async def generate_insights(self, shopify_metrics: dict, agentway_data: dict | None = None) -> dict:
        """
        Generate AI-powered insights from metrics data.

        Returns the full prompt and response for audit logging, plus parsed sections.
        """
        user_prompt = self._build_user_prompt(shopify_metrics, agentway_data)

        payload = {
            "model": CLAUDE_MODEL,
            "max_tokens": 4096,
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

    def _build_user_prompt(self, shopify_metrics: dict, agentway_data: dict | None) -> str:
        parts = []

        parts.append("# Customer Insights Data\n")
        parts.append(f"Report period: {shopify_metrics.get('period', {}).get('start', '?')} to {shopify_metrics.get('period', {}).get('end', '?')}\n")

        # Data source availability
        parts.append("## Data Sources Available")
        parts.append(f"- Shopify: ✓ (orders and customers)")
        if agentway_data:
            parts.append(f"- Support/Agentway: ✓ (tickets and topics)")
        else:
            parts.append(f"- Support/Agentway: ✗ (not available — skip the support_signals section or note data is missing)")

        # Shopify metrics
        parts.append("\n## Shopify Metrics\n")
        parts.append("### Order Metrics")
        parts.append(json.dumps(shopify_metrics.get("orders", {}), indent=2))

        parts.append("\n### Customer Metrics")
        parts.append(json.dumps(shopify_metrics.get("customers", {}), indent=2))

        parts.append("\n### Cohort Analysis")
        parts.append(json.dumps(shopify_metrics.get("cohorts", {}), indent=2))

        parts.append("\n### Product Performance")
        parts.append(json.dumps(shopify_metrics.get("products", {}), indent=2))

        # Agentway data
        if agentway_data:
            parts.append("\n## Support / Agentway Data\n")
            parts.append(json.dumps(agentway_data, indent=2))

        parts.append("\n---\n")
        parts.append("Analyze this data and return the JSON response as specified in your instructions.")
        parts.append("Remember: every insight must cite specific numbers. Do not invent statistics.")

        return "\n".join(parts)
