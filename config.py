"""
Configuration
=============
Centralized environment variable loading for the Customer Insights app.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Anthropic
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL = "claude-opus-4-6"

# Shopify Admin REST API
SHOPIFY_STORE_URL = os.getenv("SHOPIFY_STORE_URL", "")  # e.g. "mystore.myshopify.com"
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "")
SHOPIFY_API_VERSION = "2024-01"

# Agentway
AGENTWAY_API_URL = os.getenv("AGENTWAY_API_URL", "")
AGENTWAY_API_KEY = os.getenv("AGENTWAY_API_KEY", "")

# Email (Resend)
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
REPORT_RECIPIENTS = [
    e.strip() for e in os.getenv("REPORT_RECIPIENTS", "").split(",") if e.strip()
]
REPORT_FROM_EMAIL = os.getenv("REPORT_FROM_EMAIL", "insights@yourdomain.com")

# Server
PORT = int(os.getenv("PORT", "8000"))

# Scheduler
REPORT_SCHEDULE_DAY = os.getenv("REPORT_SCHEDULE_DAY", "mon")  # mon, tue, wed, etc.
REPORT_SCHEDULE_HOUR = int(os.getenv("REPORT_SCHEDULE_HOUR", "7"))
REPORT_SCHEDULE_MINUTE = int(os.getenv("REPORT_SCHEDULE_MINUTE", "0"))

# Data
DEFAULT_LOOKBACK_DAYS = int(os.getenv("DEFAULT_LOOKBACK_DAYS", "90"))
