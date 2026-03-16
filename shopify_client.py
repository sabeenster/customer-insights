from __future__ import annotations

"""
Shopify Client
==============
Fetches order and customer data from Shopify Admin REST API.
Computes customer behavior metrics for the insights report.
"""

import logging
import asyncio
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from urllib.parse import urljoin

import httpx

from config import SHOPIFY_STORE_URL, SHOPIFY_ACCESS_TOKEN, SHOPIFY_API_VERSION, DEFAULT_LOOKBACK_DAYS

log = logging.getLogger("insights.shopify")


class ShopifyClient:
    def __init__(self):
        self.base_url = f"https://{SHOPIFY_STORE_URL}/admin/api/{SHOPIFY_API_VERSION}"
        self.headers = {
            "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
            "Content-Type": "application/json",
        }
        self.http = httpx.AsyncClient(timeout=30.0, headers=self.headers)

    async def _paginated_get(self, endpoint: str, params: dict, resource_key: str) -> list:
        """Fetch all pages from a Shopify REST endpoint using Link-header pagination."""
        all_items = []
        url = f"{self.base_url}/{endpoint}.json"

        while url:
            await asyncio.sleep(0.5)  # Shopify rate limit: 2 req/s
            response = await self.http.get(url, params=params if not all_items else None)
            response.raise_for_status()
            data = response.json()
            items = data.get(resource_key, [])
            all_items.extend(items)
            log.info(f"  Fetched {len(items)} {resource_key} (total: {len(all_items)})")

            # Cursor-based pagination via Link header
            url = None
            link_header = response.headers.get("link", "")
            if 'rel="next"' in link_header:
                for part in link_header.split(","):
                    if 'rel="next"' in part:
                        url = part.split("<")[1].split(">")[0]
                        params = None  # params are embedded in the next URL
                        break

        return all_items

    async def fetch_orders(self, days_back: int = DEFAULT_LOOKBACK_DAYS) -> list[dict]:
        """Fetch orders from the last N days."""
        since = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
        log.info(f"Fetching orders since {since}")
        params = {
            "created_at_min": since,
            "status": "any",
            "limit": 250,
            "fields": "id,created_at,total_price,customer,line_items,financial_status,order_number",
        }
        return await self._paginated_get("orders", params, "orders")

    async def fetch_customers(self, days_back: int = DEFAULT_LOOKBACK_DAYS) -> list[dict]:
        """Fetch customers updated in the last N days."""
        since = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
        log.info(f"Fetching customers since {since}")
        params = {
            "updated_at_min": since,
            "limit": 250,
            "fields": "id,created_at,orders_count,total_spent,tags,first_name,last_name",
        }
        return await self._paginated_get("customers", params, "customers")

    def compute_metrics(self, orders: list[dict], customers: list[dict], days_back: int = DEFAULT_LOOKBACK_DAYS) -> dict:
        """Compute customer behavior metrics from raw Shopify data."""
        now = datetime.now(timezone.utc)
        period_start = now - timedelta(days=days_back)

        metrics = {
            "period": {"start": period_start.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d"), "days": days_back},
            "orders": self._order_metrics(orders),
            "customers": self._customer_metrics(customers),
            "cohorts": self._cohort_analysis(orders),
            "products": self._product_metrics(orders),
        }
        return metrics

    def _order_metrics(self, orders: list[dict]) -> dict:
        if not orders:
            return {"total_orders": 0, "note": "No order data available"}

        totals = []
        weekly_counts = defaultdict(int)
        daily_counts = defaultdict(int)

        for o in orders:
            try:
                price = float(o.get("total_price", 0))
            except (ValueError, TypeError):
                price = 0
            totals.append(price)

            created = o.get("created_at", "")[:10]
            daily_counts[created] += 1
            # ISO week
            try:
                dt = datetime.fromisoformat(created)
                week_key = dt.strftime("%Y-W%W")
                weekly_counts[week_key] += 1
            except ValueError:
                pass

        avg_order_value = sum(totals) / len(totals) if totals else 0
        total_revenue = sum(totals)

        # Weekly trend (last 4 weeks sorted)
        sorted_weeks = sorted(weekly_counts.items())[-4:]

        return {
            "total_orders": len(orders),
            "total_revenue": round(total_revenue, 2),
            "average_order_value": round(avg_order_value, 2),
            "min_order_value": round(min(totals), 2) if totals else 0,
            "max_order_value": round(max(totals), 2) if totals else 0,
            "weekly_trend": [{"week": w, "orders": c} for w, c in sorted_weeks],
        }

    def _customer_metrics(self, customers: list[dict]) -> dict:
        if not customers:
            return {"total_customers": 0, "note": "No customer data available"}

        total = len(customers)
        order_counts = []
        spend_amounts = []

        for c in customers:
            try:
                oc = int(c.get("orders_count", 0))
            except (ValueError, TypeError):
                oc = 0
            order_counts.append(oc)

            try:
                spent = float(c.get("total_spent", 0))
            except (ValueError, TypeError):
                spent = 0
            spend_amounts.append(spent)

        one_time = sum(1 for oc in order_counts if oc == 1)
        repeat = sum(1 for oc in order_counts if oc > 1)
        never_ordered = sum(1 for oc in order_counts if oc == 0)

        repeat_rate = (repeat / (one_time + repeat) * 100) if (one_time + repeat) > 0 else 0
        avg_lifetime_value = sum(spend_amounts) / total if total else 0

        # Order frequency distribution
        freq_dist = defaultdict(int)
        for oc in order_counts:
            if oc == 0:
                freq_dist["0 orders"] += 1
            elif oc == 1:
                freq_dist["1 order"] += 1
            elif oc <= 3:
                freq_dist["2-3 orders"] += 1
            elif oc <= 5:
                freq_dist["4-5 orders"] += 1
            else:
                freq_dist["6+ orders"] += 1

        return {
            "total_customers": total,
            "one_time_buyers": one_time,
            "repeat_buyers": repeat,
            "never_ordered": never_ordered,
            "repeat_purchase_rate": round(repeat_rate, 1),
            "average_lifetime_value": round(avg_lifetime_value, 2),
            "avg_orders_per_customer": round(sum(order_counts) / total, 1) if total else 0,
            "order_frequency_distribution": dict(freq_dist),
        }

    def _cohort_analysis(self, orders: list[dict]) -> dict:
        """Group customers by first-order month, track repurchase within 30/60/90 days."""
        customer_orders = defaultdict(list)
        for o in orders:
            cust = o.get("customer")
            if not cust:
                continue
            cid = cust.get("id")
            if cid:
                try:
                    dt = datetime.fromisoformat(o["created_at"].replace("Z", "+00:00"))
                    customer_orders[cid].append(dt)
                except (ValueError, KeyError):
                    pass

        cohorts = defaultdict(lambda: {"total": 0, "repurchased_30d": 0, "repurchased_60d": 0, "repurchased_90d": 0})

        for cid, dates in customer_orders.items():
            dates.sort()
            first = dates[0]
            cohort_key = first.strftime("%Y-%m")
            cohorts[cohort_key]["total"] += 1

            if len(dates) > 1:
                gap = (dates[1] - first).days
                if gap <= 30:
                    cohorts[cohort_key]["repurchased_30d"] += 1
                if gap <= 60:
                    cohorts[cohort_key]["repurchased_60d"] += 1
                if gap <= 90:
                    cohorts[cohort_key]["repurchased_90d"] += 1

        # Convert to list sorted by month
        result = []
        for month in sorted(cohorts.keys()):
            c = cohorts[month]
            result.append({
                "month": month,
                "new_customers": c["total"],
                "repurchased_30d": c["repurchased_30d"],
                "repurchased_60d": c["repurchased_60d"],
                "repurchased_90d": c["repurchased_90d"],
                "retention_30d_pct": round(c["repurchased_30d"] / c["total"] * 100, 1) if c["total"] else 0,
            })
        return {"monthly_cohorts": result}

    def _product_metrics(self, orders: list[dict]) -> dict:
        """Top products by revenue and order frequency."""
        product_stats = defaultdict(lambda: {"revenue": 0, "quantity": 0, "order_count": 0})

        for o in orders:
            seen_products = set()
            for item in o.get("line_items", []):
                name = item.get("title", "Unknown")
                try:
                    qty = int(item.get("quantity", 0))
                    price = float(item.get("price", 0))
                except (ValueError, TypeError):
                    qty, price = 0, 0

                product_stats[name]["revenue"] += price * qty
                product_stats[name]["quantity"] += qty
                if name not in seen_products:
                    product_stats[name]["order_count"] += 1
                    seen_products.add(name)

        # Top 10 by revenue
        by_revenue = sorted(product_stats.items(), key=lambda x: x[1]["revenue"], reverse=True)[:10]
        top_products = [
            {"name": name, "revenue": round(s["revenue"], 2), "quantity": s["quantity"], "order_count": s["order_count"]}
            for name, s in by_revenue
        ]

        return {
            "unique_products_sold": len(product_stats),
            "top_products_by_revenue": top_products,
        }

    async def health_check(self) -> dict:
        """Check if Shopify API is reachable."""
        if not SHOPIFY_STORE_URL or not SHOPIFY_ACCESS_TOKEN:
            return {"status": "not_configured", "error": "Missing SHOPIFY_STORE_URL or SHOPIFY_ACCESS_TOKEN"}
        try:
            response = await self.http.get(f"{self.base_url}/shop.json")
            response.raise_for_status()
            shop = response.json().get("shop", {})
            return {"status": "connected", "shop_name": shop.get("name", "unknown")}
        except Exception as e:
            return {"status": "error", "error": str(e)}
