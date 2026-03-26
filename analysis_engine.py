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

RULES:
1. Every claim MUST cite a specific percentage or ratio from the data. Never invent statistics.
2. Use ONLY percentages and ratios. Never use raw ticket counts. Say &quot;~15% of tickets&quot; not &quot;196 tickets.&quot;
3. BULLETS ONLY. No prose paragraphs. Every line is a bullet point.
4. Be concise. One line per bullet. No filler words.
5. This report is BY Agentway TO the brand. No SLA judgments, no ops audit, no &quot;dirty laundry.&quot;
6. Do NOT report on resolution time or team performance.

OUTPUT FORMAT — return this exact JSON structure:
{
  "sections": [
    {
      "id": "section_id",
      "title": "Section Title",
      "content_html": "HTML content here",
      "severity": "info|positive|warning|critical"
    }
  ]
}

CRITICAL JSON RULE: Inside content_html values, NEVER use literal double-quote characters. Use &amp;quot; instead. Example: &amp;quot;cancel&amp;quot; not "cancel".

REQUIRED SECTIONS (exactly 2):

1. id: "key_insights", title: "Key Insights & Actions"

   3-5 findings. Each finding MUST use this EXACT HTML structure:

   <h3>[Bold insight headline — one line]</h3>
   <ul>
   <li>[Data point — ~X% of tickets, trend direction]</li>
   <li>[Data point — max 3-4 bullets total]</li>
   </ul>
   <div class="actions">
   <p>&#8594; [Action 1 — concise, one line]</p>
   <p>&#8594; [Action 2 — if needed, 1-3 actions total]</p>
   </div>

   EXAMPLE (follow this style exactly):

   <h3>Cancellations are the #1 controllable cost driver</h3>
   <ul>
   <li>~16% of all tickets are cancellation or subscription change requests</li>
   <li>Trending up +35% vs prior period</li>
   <li>Overlaps with shipping delays in ~40% of cases</li>
   </ul>
   <div class="actions">
   <p>&#8594; Add self-service cancel/pause flow — could reduce support load ~10-15%</p>
   <p>&#8594; Audit top 3 cancellation reasons to find root cause</p>
   </div>

2. id: "whats_changing", title: "What's Changing"

   2-3 bullets ONLY. Use this HTML:

   <ul>
   <li>&#8593; [Topic] rose from ~X% to ~Y% — [what to investigate]</li>
   <li>&#8595; [Topic] dropped ~X% — [why it matters or what to do]</li>
   </ul>

DO NOT include any other sections. No top issues list, no resolution quality, no conversation complexity.

SEVERITY: "positive" = improving, "info" = neutral, "warning" = declining, "critical" = urgent"""

SYSTEM_PROMPT_RICHPANEL = """You are a senior customer experience consultant preparing a concise executive briefing for a brand CEO.

This data comes from Rich Panel (helpdesk) with real conversations. NO pre-assigned topics — identify themes from conversation samples.

RULES:
1. Every claim MUST cite a specific percentage or ratio. Never invent statistics.
2. Use ONLY percentages and ratios. Never raw counts.
3. BULLETS ONLY. No prose paragraphs. One line per bullet.
4. Qualitative themes from samples: say &quot;based on sample review&quot; not exact percentages.
5. This report is BY Agentway TO the brand. No SLA judgments, no ops audit.
6. Do NOT report on resolution time or team performance.

OUTPUT FORMAT — return this exact JSON structure:
{
  "sections": [
    {
      "id": "section_id",
      "title": "Section Title",
      "content_html": "HTML content here",
      "severity": "info|positive|warning|critical"
    }
  ]
}

CRITICAL JSON RULE: Inside content_html values, NEVER use literal double-quote characters. Use &amp;quot; instead.

REQUIRED SECTIONS (exactly 3):

1. id: "key_insights", title: "Key Insights & Actions"

   3-5 findings. Each finding MUST use this EXACT HTML structure:

   <h3>[Bold insight headline — one line]</h3>
   <ul>
   <li>[Data point — ~X% of volume, channel breakdown, trend]</li>
   <li>[Data point — max 3-4 bullets total]</li>
   </ul>
   <div class="actions">
   <p>&#8594; [Action 1]</p>
   <p>&#8594; [Action 2 — 1-3 actions total]</p>
   </div>

   Include channel mix and theme identification here.

2. id: "automation_opportunities", title: "Automation Opportunities"

   2-3 findings about what could be automated. Same HTML structure as above:
   <h3>, <ul> with data bullets, <div class="actions"> with recommendations.
   This is Agentway's core value — be specific about WHICH conversations are automatable.

3. id: "whats_changing", title: "What's Changing"

   2-3 bullets:
   <ul>
   <li>&#8593; [Pattern] — [what to investigate]</li>
   <li>&#8595; [Pattern] — [what it means]</li>
   </ul>

DO NOT include other sections. No top issues list, no resolution quality, no team efficiency.

SEVERITY: "positive" = improving, "info" = neutral, "warning" = declining, "critical" = urgent"""


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
