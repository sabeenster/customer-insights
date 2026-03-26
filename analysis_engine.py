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

SYSTEM_PROMPT_AGENTWAY = """You are a senior customer experience consultant preparing a concise executive briefing for a brand CEO.
Your job is to analyze customer support ticket data and deliver data-backed insights paired with concrete actions.

CRITICAL RULES:
1. Every insight MUST reference specific numbers from the data provided. Never invent or estimate statistics.
2. If the data is insufficient to support a conclusion, explicitly say so rather than guessing.
3. Prefer RATIOS and PERCENTAGES over raw ticket counts. Say "~15% of tickets" not "196 tickets". Use exact counts only when emphasizing scale or in parentheses for context.
4. Compare numbers where possible (e.g., trending topics vs prior period — only if the data supports it).
5. Be direct and specific. No filler, no generic business advice that could apply to any company.
6. Be CONCISE. Every sentence must earn its place.

TONE RULES (important):
- This report is presented BY Agentway TO the brand. It is a ticket topic analysis, not an operational audit.
- Do NOT assess, judge, or grade the support team's performance (e.g., "resolution times are too slow", "team is understaffed").
- Do NOT report on resolution time outliers or average resolution time. If first response time data is available, you may reference it as a factual SLA metric — but do not editorialize on whether it's good or bad.
- Do NOT open the dirty laundry. Focus on WHAT customers are experiencing, not HOW the support team is performing.

STYLE:
- Write for a CEO who is busy but sharp. Lead with the most important finding.
- Use plain language, not jargon.
- Frame insights around customer experience and business impact, not support team efficiency.
- The value is INSIGHT + ACTION, not data summaries. Dashboards show data; this report tells you what it means and what to do.

OUTPUT FORMAT:
Return a JSON object with this exact structure:
{
  "sections": [
    {
      "id": "section_id",
      "title": "Section Title",
      "content_html": "<p>HTML content with <strong>bold</strong> for key numbers...</p>",
      "severity": "info|positive|warning|critical",
      "based_on": "Description of data source and key metrics used"
    }
  ]
}

CRITICAL JSON RULE: Inside content_html and based_on string values, NEVER use literal double-quote characters ("). Instead use &quot; for any quoted text. For example: &quot;uncategorized&quot; not "uncategorized". This is essential for valid JSON.

REQUIRED SECTIONS (exactly 2, in order):

1. key_insights — Title: "Key Insights & Actions"
   This is the hero section. Present 3-4 findings MAX. For EACH finding, use this structure:
   - Start with the DATA POINT (bold the key number)
   - 1-2 bullet insight explaining what it means for the business
   - A concrete SUGGESTED ACTION with expected impact
   Format each finding as a distinct block using <h3> for the finding title.
   Combine what was previously "executive summary" and "recommended actions" into one tight package.
   Severity reflects overall CX health.

2. whats_changing — Title: "What's Changing"
   2-3 bullets ONLY. Each bullet:
   - What topic is spiking or declining (with numbers from trending data)
   - A suggested investigation approach or action
   Focus on: "here's what to dig into and how." This is where Agentway provides intelligence beyond dashboards.
   Severity reflects whether trends are positive or concerning.

DO NOT include sections for: top issues by volume (redundant with key_insights), resolution quality, or conversation complexity.

SEVERITY GUIDE:
- "positive": metric is good or improving
- "info": neutral observation, for awareness
- "warning": metric declining or approaching a threshold
- "critical": requires immediate attention"""

SYSTEM_PROMPT_RICHPANEL = """You are a senior customer experience consultant preparing a concise executive briefing for a brand CEO.
Your job is to analyze customer support conversation data and deliver data-backed insights paired with concrete actions, with a focus on automation opportunities.

This data comes from a helpdesk platform (Rich Panel) with real customer conversations. There are NO pre-assigned topics — identify key themes from the conversation samples provided.

CRITICAL RULES:
1. Every insight MUST reference specific numbers from the structured metrics provided. Never invent statistics.
2. When identifying themes from conversation samples, be clear these are OBSERVED PATTERNS. Say "based on the sample of X conversations reviewed" not "X% of all tickets."
3. Prefer RATIOS and PERCENTAGES over raw counts. Say "~86% of volume comes from Instagram" not "2,395 Instagram messages". Use exact counts only in parentheses for context.
4. Quantitative claims (channel breakdown, volume trends) come from structured metrics — these are exact.
5. Qualitative claims (themes, automation candidates) come from conversation samples — flag as sample-based.
6. Be direct, specific, and CONCISE. Every sentence must earn its place.

TONE RULES (important):
- This report is presented BY Agentway TO the brand. It is a ticket analysis, not an operational audit.
- Do NOT assess, judge, or grade the support team's performance (e.g., "team is overwhelmed", "response times need improvement").
- Do NOT report on resolution time outliers or average resolution time. If first response time data is available, you may reference it factually — but do not editorialize.
- Focus on WHAT customers are experiencing and WHERE automation can help, not HOW the support team is performing.

STYLE:
- Write for a CEO evaluating customer experience and automation opportunities. Lead with the biggest opportunity.
- The value is INSIGHT + ACTION, not data summaries.
- Frame findings around customer experience and business impact.

OUTPUT FORMAT:
Return a JSON object with this exact structure:
{
  "sections": [
    {
      "id": "section_id",
      "title": "Section Title",
      "content_html": "<p>HTML content with <strong>bold</strong> for key numbers...</p>",
      "severity": "info|positive|warning|critical",
      "based_on": "Description of data source and key metrics used"
    }
  ]
}

CRITICAL JSON RULE: Inside content_html and based_on string values, NEVER use literal double-quote characters ("). Instead use &quot; for any quoted text. For example: &quot;uncategorized&quot; not "uncategorized". This is essential for valid JSON.

REQUIRED SECTIONS (exactly 3, in order):

1. key_insights — Title: "Key Insights & Actions"
   Present 3-4 findings MAX. For EACH finding:
   - DATA POINT (bold the key number)
   - 1-2 bullet insight explaining what it means
   - Concrete SUGGESTED ACTION with expected impact
   Format each finding as a distinct block using <h3>.
   Include channel breakdown and team distribution findings here.

2. automation_opportunities — Title: "Automation Opportunities"
   Based on conversation samples, which inquiry types are repetitive and could be handled by AI?
   2-3 bullets MAX. For each: what type of question, why it's automatable, estimated % of volume.
   This is Agentway's core value proposition — be specific and actionable.

3. whats_changing — Title: "What's Changing"
   2-3 bullets ONLY. Volume trends, emerging patterns, suggested investigation approach.

DO NOT include separate sections for: top issues by volume, resolution quality, team efficiency (fold relevant data into key_insights).

SEVERITY GUIDE:
- "positive": metric is good or improving
- "info": neutral observation, for awareness
- "warning": metric declining or needs attention
- "critical": requires immediate action"""


class AnalysisEngine:
    def __init__(self):
        self.http = httpx.AsyncClient(timeout=120.0)

    @staticmethod
    def _parse_json_response(text: str) -> dict:
        """
        Robustly parse JSON from Claude's response, handling:
        - Markdown code block wrappers
        - Invalid escape sequences
        - Unescaped quotes inside JSON string values
        """
        import re

        # Strip markdown code blocks
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]

        # Fix invalid JSON escapes
        text = text.replace("\\'", "'")

        # Replace smart/curly quotes with HTML entities
        text = text.replace("\u201c", "&ldquo;")
        text = text.replace("\u201d", "&rdquo;")
        text = text.replace("\u2018", "&#39;")
        text = text.replace("\u2019", "&#39;")

        # Try parsing directly first
        try:
            return json.loads(text.strip())
        except json.JSONDecodeError:
            pass

        # Fix unescaped quotes inside JSON string values.
        # Strategy: find content_html values and escape quotes within them.
        # Pattern: "content_html": "..." — the value often contains unescaped "
        def fix_html_values(m):
            """Escape unescaped double quotes inside JSON string values for HTML keys."""
            prefix = m.group(1)  # "content_html": "
            inner = m.group(2)   # the actual HTML content
            # Escape any unescaped double quotes inside the HTML
            fixed = inner.replace('"', '&quot;')
            return prefix + fixed + '"'

        # Match: "content_html": "...(greedy across the value)..."
        # We look for the pattern and fix inner quotes
        fixed = re.sub(
            r'("(?:content_html|based_on|title)":\s*")(.*?)("(?:\s*[,}]))',
            lambda m: m.group(1) + m.group(2).replace('"', '&quot;') + m.group(3),
            text,
            flags=re.DOTALL,
        )

        try:
            return json.loads(fixed.strip())
        except json.JSONDecodeError:
            pass

        # Last fallback: find JSON boundaries
        stripped = text.strip()
        start = stripped.find("{")
        end = stripped.rfind("}") + 1
        if start >= 0 and end > start:
            chunk = stripped[start:end]
            # Apply same fix
            fixed2 = re.sub(
                r'("(?:content_html|based_on|title)":\s*")(.*?)("(?:\s*[,}]))',
                lambda m: m.group(1) + m.group(2).replace('"', '&quot;') + m.group(3),
                chunk,
                flags=re.DOTALL,
            )
            try:
                return json.loads(fixed2)
            except json.JSONDecodeError:
                pass

        raise json.JSONDecodeError("Could not parse JSON from Claude response", text, 0)

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

    async def generate_insights(self, agentway_data: dict, shopify_summary: dict = None) -> dict:
        """
        Generate AI-powered insights from support ticket data.

        Returns the full prompt and response for audit logging, plus parsed sections.
        """
        is_richpanel = agentway_data.get("data_source") == "richpanel"
        system_prompt = SYSTEM_PROMPT_RICHPANEL if is_richpanel else SYSTEM_PROMPT_AGENTWAY
        user_prompt = self._build_user_prompt(agentway_data, shopify_summary)

        payload = {
            "model": CLAUDE_MODEL,
            "max_tokens": 8192,
            "system": system_prompt,
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
            insights = self._parse_json_response(text)
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
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "raw_response": raw_response,
            "insights": insights,
        }

    def _build_user_prompt(self, agentway_data: dict, shopify_summary: dict = None) -> str:
        is_richpanel = agentway_data.get("data_source") == "richpanel"
        parts = []

        parts.append("# Customer Support Insights Data\n")
        parts.append(f"Total tickets analyzed: {agentway_data.get('total_tickets', 0)}\n")

        if is_richpanel:
            return self._build_richpanel_prompt(agentway_data, parts, shopify_summary)
        else:
            return self._build_agentway_prompt(agentway_data, parts, shopify_summary)

    def _build_agentway_prompt(self, data: dict, parts: list, shopify_summary: dict = None) -> str:
        # Structured metrics (topic counts, trends, resolution times)
        metrics_data = {k: v for k, v in data.items() if k != "topic_summaries_sample"}
        parts.append("## Structured Metrics (topic counts, trends, resolution times)\n")
        parts.append(json.dumps(metrics_data, indent=2))

        # Topic summaries — rich context for deeper understanding
        summaries = data.get("topic_summaries_sample", [])
        if summaries:
            parts.append("\n## Sample Ticket Summaries (real customer conversations)\n")
            parts.append("These are detailed summaries of actual support conversations. Use them to understand")
            parts.append("the NATURE and NUANCE of customer issues beyond what topic names alone reveal.\n")
            for s in summaries:
                topics_str = ", ".join(s.get("topics", [])) or "uncategorized"
                parts.append(f"**Ticket {s['friendly_id']}** ({s.get('status', 'unknown')}) — Topics: {topics_str}")
                parts.append(f"> {s['topic_summary']}\n")

        self._append_shopify_context(parts, shopify_summary)

        parts.append("\n---\n")
        parts.append("Analyze this support ticket data and return the JSON response as specified in your instructions.")
        parts.append("Use the structured metrics for quantitative claims (percentages, counts, trends).")
        if summaries:
            parts.append("Use the ticket summaries to add qualitative depth — what are customers actually saying and feeling?")
        if shopify_summary:
            parts.append("Use Shopify data only for context (contact rates, product correlation). Do NOT fabricate Shopify numbers.")
        parts.append("Remember: every insight must cite specific numbers. Do not invent statistics.")

        return "\n".join(parts)

    def _build_richpanel_prompt(self, data: dict, parts: list, shopify_summary: dict = None) -> str:
        # Structured metrics (exact counts from Python, not LLM-generated)
        metrics_data = {k: v for k, v in data.items() if k != "conversation_samples"}
        parts.append("## Structured Metrics (exact counts from data)\n")
        parts.append("These numbers are computed directly from the CSV. Use them for all quantitative claims.\n")
        parts.append(json.dumps(metrics_data, indent=2))

        # Conversation samples — for qualitative theme identification
        samples = data.get("conversation_samples", [])
        if samples:
            parts.append(f"\n## Conversation Samples ({len(samples)} representative tickets)\n")
            parts.append("These are REAL customer conversations sampled across channels.")
            parts.append("Use them to identify the TOP THEMES customers are contacting about.")
            parts.append("Group similar conversations into categories. Be specific about what customers are asking.\n")
            for s in samples:
                parts.append(f"**#{s['friendly_id']}** [{s.get('channel', 'unknown')}] ({s.get('status', '')})")
                parts.append(f"Subject: {s.get('subject', 'N/A')}")
                if s.get("conversation_snippet"):
                    parts.append(f"> {s['conversation_snippet']}")
                parts.append("")

        self._append_shopify_context(parts, shopify_summary)

        parts.append("\n---\n")
        parts.append("Analyze this support data and return the JSON response as specified in your instructions.")
        parts.append("Use structured metrics for quantitative claims (channel breakdown, volume, assignee data).")
        parts.append("Use conversation samples to identify customer themes and automation opportunities.")
        parts.append("When citing themes from samples, say 'based on sample review' — do not present sample patterns as exact percentages of the full dataset.")
        parts.append("Remember: every insight must be traceable to the data provided. Do not invent statistics.")

        return "\n".join(parts)

    def _append_shopify_context(self, parts: list, shopify_summary: dict = None):
        if not shopify_summary:
            return
        parts.append("\n## Shopify Store Context (aggregate metrics for reference)\n")
        parts.append("Use these numbers ONLY to provide context (e.g., support contact rate as % of orders).")
        parts.append("Do NOT invent Shopify metrics not listed here.\n")
        cp = shopify_summary.get("current_period", {})
        pp = shopify_summary.get("prior_period", {})
        parts.append(f"- **Current period** ({cp.get('start')} to {cp.get('end')}): {cp.get('total_orders', 0)} orders, ${cp.get('total_revenue', 0):,.0f} revenue, ${cp.get('avg_order_value', 0):.2f} AOV")
        parts.append(f"- **Prior period** ({pp.get('start')} to {pp.get('end')}): {pp.get('total_orders', 0)} orders, ${pp.get('total_revenue', 0):,.0f} revenue")
        parts.append(f"- Order trend: {shopify_summary.get('order_trend', 'unknown')}")
        parts.append(f"- Revenue trend: {shopify_summary.get('revenue_trend', 'unknown')}")
        top_prods = shopify_summary.get("top_products", [])
        if top_prods:
            parts.append("- Top products by order frequency:")
            for p in top_prods[:5]:
                parts.append(f"  - {p['name']}: {p['order_count']} orders, {p['total_quantity']} units")
