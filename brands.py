from __future__ import annotations

"""
Brands Configuration
====================
Manages per-brand settings: name, slug, Shopify credentials (optional).
Stored as a JSON file on disk. CRUD via API endpoints.

Shopify credentials are stored per-brand so each brand's report
pulls from the correct Shopify store. Credentials are READ-ONLY —
the system NEVER writes to Shopify.
"""

import os
import json
import logging
from pathlib import Path

log = logging.getLogger("insights.brands")

BRANDS_FILE = os.path.join(os.path.dirname(__file__), "audit_logs", "brands.json")


def _load_brands() -> dict:
    """Load brands config from disk. Returns {slug: brand_dict}."""
    if not os.path.exists(BRANDS_FILE):
        return {}
    try:
        with open(BRANDS_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def _save_brands(brands: dict):
    """Save brands config to disk."""
    os.makedirs(os.path.dirname(BRANDS_FILE), exist_ok=True)
    with open(BRANDS_FILE, "w") as f:
        json.dump(brands, f, indent=2)


def list_brands() -> list[dict]:
    """Return all brands as a list, sorted by name."""
    brands = _load_brands()
    result = []
    for slug, data in sorted(brands.items(), key=lambda x: x[1].get("name", "")):
        result.append({
            "slug": slug,
            "name": data.get("name", slug),
            "has_shopify": bool(data.get("shopify_store_url") and data.get("shopify_access_token")),
        })
    return result


def get_brand(slug: str) -> dict | None:
    """Get a brand by slug. Returns full config including Shopify creds."""
    brands = _load_brands()
    return brands.get(slug)


def add_brand(
    name: str,
    slug: str = None,
    shopify_store_url: str = None,
    shopify_access_token: str = None,
) -> dict:
    """Add or update a brand. Returns the brand config."""
    if not slug:
        slug = name.lower().replace(" ", "-").replace("'", "")

    brands = _load_brands()
    brands[slug] = {
        "name": name,
        "slug": slug,
        "shopify_store_url": (shopify_store_url or "").strip() or None,
        "shopify_access_token": (shopify_access_token or "").strip() or None,
    }
    _save_brands(brands)
    log.info(f"Brand saved: {name} (slug: {slug}, shopify: {'yes' if shopify_store_url else 'no'})")
    return brands[slug]


def delete_brand(slug: str) -> bool:
    """Remove a brand by slug. Returns True if found and deleted."""
    brands = _load_brands()
    if slug in brands:
        del brands[slug]
        _save_brands(brands)
        log.info(f"Brand deleted: {slug}")
        return True
    return False


def get_shopify_creds(brand_name: str) -> dict | None:
    """
    Look up Shopify credentials for a brand by name (fuzzy match).
    Returns {"store_url": ..., "access_token": ...} or None.
    """
    brands = _load_brands()
    name_lower = (brand_name or "").lower().strip()

    for slug, data in brands.items():
        brand_name_lower = (data.get("name") or "").lower()
        if name_lower == brand_name_lower or name_lower == slug:
            store_url = data.get("shopify_store_url")
            token = data.get("shopify_access_token")
            if store_url and token:
                return {"store_url": store_url, "access_token": token}
            return None

    return None


def seed_defaults():
    """Seed default brands if none exist (first run only)."""
    brands = _load_brands()
    if brands:
        return  # Already configured

    defaults = [
        {"name": "Future Kind", "slug": "future-kind"},
        {"name": "Emme Mama", "slug": "emme-mama"},
        {"name": "Dippin Daisy", "slug": "dippin-daisy"},
        {"name": "Big Moods", "slug": "big-moods"},
        {"name": "KNKG", "slug": "knkg"},
    ]

    # Migrate existing env var Shopify creds to Future Kind
    env_store = os.getenv("SHOPIFY_STORE_URL")
    env_token = os.getenv("SHOPIFY_ACCESS_TOKEN")

    for d in defaults:
        kwargs = {"name": d["name"], "slug": d["slug"]}
        if d["slug"] == "future-kind" and env_store and env_token:
            kwargs["shopify_store_url"] = env_store
            kwargs["shopify_access_token"] = env_token
        add_brand(**kwargs)

    log.info(f"Seeded {len(defaults)} default brands")
