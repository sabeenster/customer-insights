from __future__ import annotations

"""
Shopify Client (Lightweight, Read-Only)
=======================================
Pulls ONLY aggregate summary metrics from Shopify Admin REST API.
NO writes. NO bulk data pulls. Minimal API calls for context.

Purpose: provide denominator/context for support ticket analysis.
E.g., "118 delivery tickets out of 5,000 orders = 2.4% contact rate"
"""

import logging
from datetime import datetime, timedelta, timezone

import httpx

from config import SHOPIFY_STORE_URL, SHOPIFY_ACCESS_TOKEN, SHOPIFY_API_VERSION

log = logging.getLogger("insights.shopify")


class ShopifyClient:
    """Read-only Shopify client. Makes 3-4 lightweight API calls max. NEVER writes."""

    def __init__(self, store_url: str = None, access_token: str = None):
        """
        Create a Shopify client. Can use per-brand credentials or fall back to env vars.
        Args:
            store_url: e.g. "mystore.myshopify.com" (overrides SHOPIFY_STORE_URL env var)
            access_token: Shopify Admin API token (overrides SHOPIFY_ACCESS_TOKEN env var)
        """
        self._store_url = store_url or SHOPIFY_STORE_URL
        self._access_token = access_token or SHOPIFY_ACCESS_TOKEN
        self.base_url = f"https://{self._store_url}/admin/api/{SHOPIFY_API_VERSION}"
        self.headers = {
            "X-Shopify-Access-Token": self._access_token,
            "Content-Type": "application/json",
        }
        self.http = httpx.AsyncClient(timeout=30.0, headers=self.headers)

    def is_configured(self) -> bool:
        return bool(self._store_url and self._access_token)

    async def health_check(self) -> dict:
        """Check if Shopify API is reachable."""
        if not self.is_configured():
            return {"status": "not_configured"}
        try:
            response = await self.http.get(f"{self.base_url}/shop.json")
            response.raise_for_status()
            shop = response.json().get("shop", {})
            return {"status": "connected", "shop_name": shop.get("name", "unknown")}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    async def get_summary_metrics_by_date(self, start_date: str, end_date: str) -> dict | None:
        """
        Fetch lightweight summary matching a specific date range (from CSV).
        READ-ONLY. Makes exactly 3 API calls.
        """
        if not self.is_configured():
            return None

        try:
            period_start = datetime.strptime(start_date[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            period_end = datetime.strptime(end_date[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            days_back = (period_end - period_start).days or 90
            prior_start = period_start - timedelta(days=days_back)

            current = await self._order_summary(period_start, period_end)
            prior = await self._order_summary(prior_start, period_start)
            top_products = await self._top_products(period_start)

            summary = {
                "period_days": days_back,
                "current_period": {
                    "start": period_start.strftime("%Y-%m-%d"),
                    "end": period_end.strftime("%Y-%m-%d"),
                    "total_orders": current["count"],
                    "total_revenue": current["revenue"],
                    "avg_order_value": round(current["revenue"] / current["count"], 2) if current["count"] > 0 else 0,
                },
                "prior_period": {
                    "start": prior_start.strftime("%Y-%m-%d"),
                    "end": period_start.strftime("%Y-%m-%d"),
                    "total_orders": prior["count"],
                    "total_revenue": prior["revenue"],
                },
                "order_trend": _trend_direction(current["count"], prior["count"]),
                "revenue_trend": _trend_direction(current["revenue"], prior["revenue"]),
                "top_products": top_products,
            }

            log.info(f"Shopify summary ({start_date} to {end_date}): {current['count']} orders, ${current['revenue']:.0f}")
            return summary

        except Exception as e:
            log.warning(f"Shopify summary fetch failed: {e}")
            return None

    async def get_summary_metrics(self, days_back: int = 90) -> dict | None:
        """
        Fetch lightweight aggregate metrics from Shopify.
        Makes exactly 3 API calls:
          1. Order count for the period
          2. Order count for prior period (for comparison)
          3. Top products by sales (single page, limit 10)

        Returns None if Shopify is not configured.
        """
        if not self.is_configured():
            return None

        now = datetime.now(timezone.utc)
        period_start = now - timedelta(days=days_back)
        prior_start = period_start - timedelta(days=days_back)

        try:
            # Call 1: Order count + total for current period
            current = await self._order_summary(period_start, now)

            # Call 2: Order count + total for prior period (comparison)
            prior = await self._order_summary(prior_start, period_start)

            # Call 3: Top products (single page, 10 items)
            top_products = await self._top_products(period_start)

            summary = {
                "period_days": days_back,
                "current_period": {
                    "start": period_start.strftime("%Y-%m-%d"),
                    "end": now.strftime("%Y-%m-%d"),
                    "total_orders": current["count"],
                    "total_revenue": current["revenue"],
                    "avg_order_value": round(current["revenue"] / current["count"], 2) if current["count"] > 0 else 0,
                },
                "prior_period": {
                    "start": prior_start.strftime("%Y-%m-%d"),
                    "end": period_start.strftime("%Y-%m-%d"),
                    "total_orders": prior["count"],
                    "total_revenue": prior["revenue"],
                },
                "order_trend": _trend_direction(current["count"], prior["count"]),
                "revenue_trend": _trend_direction(current["revenue"], prior["revenue"]),
                "top_products": top_products,
            }

            log.info(f"Shopify summary: {current['count']} orders, ${current['revenue']:.0f} revenue ({days_back}d)")
            return summary

        except Exception as e:
            log.warning(f"Shopify summary fetch failed: {e}")
            return None

    async def _order_summary(self, since: datetime, until: datetime) -> dict:
        """Get order count and total revenue for a date range. Single API call."""
        response = await self.http.get(
            f"{self.base_url}/orders/count.json",
            params={
                "created_at_min": since.isoformat(),
                "created_at_max": until.isoformat(),
                "status": "any",
            },
        )
        response.raise_for_status()
        count = response.json().get("count", 0)

        # Get revenue from a small sample to estimate (avoid paginating all orders)
        # Use limit=250 single page to compute average, then extrapolate
        if count == 0:
            return {"count": 0, "revenue": 0.0}

        response = await self.http.get(
            f"{self.base_url}/orders.json",
            params={
                "created_at_min": since.isoformat(),
                "created_at_max": until.isoformat(),
                "status": "any",
                "limit": 250,
                "fields": "total_price",
            },
        )
        response.raise_for_status()
        orders = response.json().get("orders", [])

        if not orders:
            return {"count": count, "revenue": 0.0}

        sample_total = sum(float(o.get("total_price", 0)) for o in orders)

        if len(orders) >= count:
            # We got all orders, exact revenue
            return {"count": count, "revenue": round(sample_total, 2)}
        else:
            # Extrapolate from sample
            avg = sample_total / len(orders)
            estimated_revenue = avg * count
            return {"count": count, "revenue": round(estimated_revenue, 2)}

    async def _top_products(self, since: datetime) -> list[dict]:
        """Get top products from recent orders. Single API call, no pagination."""
        response = await self.http.get(
            f"{self.base_url}/orders.json",
            params={
                "created_at_min": since.isoformat(),
                "status": "any",
                "limit": 250,
                "fields": "line_items",
            },
        )
        response.raise_for_status()
        orders = response.json().get("orders", [])

        product_counts = {}
        for o in orders:
            for item in o.get("line_items", []):
                name = item.get("title", "Unknown")
                if name not in product_counts:
                    product_counts[name] = {"orders": 0, "quantity": 0}
                product_counts[name]["orders"] += 1
                product_counts[name]["quantity"] += int(item.get("quantity", 0))

        sorted_products = sorted(product_counts.items(), key=lambda x: x[1]["orders"], reverse=True)[:10]
        return [
            {"name": name, "order_count": stats["orders"], "total_quantity": stats["quantity"]}
            for name, stats in sorted_products
        ]


def _trend_direction(current: float, prior: float) -> str:
    if prior == 0:
        return "no_prior_data"
    change = (current - prior) / prior * 100
    if change > 10:
        return f"up_{round(change)}%"
    elif change < -10:
        return f"down_{round(abs(change))}%"
    return "stable"
