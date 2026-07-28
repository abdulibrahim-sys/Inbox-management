"""
Trendtrack Public API client — brand resolution for the reply agent.

Docs: https://api.trendtrack.io/v1/openapi.json
Auth: Bearer sk_tt_... in the Authorization header.

The reply agent uses this for section-6 brand resolution:
  1. Resolve the prospect's email domain (or brand name fallback) to a Trendtrack shop
  2. Pull monthly visits + active Meta ads count for the intel block
  3. Pull country signal (top traffic countries) for the geography gate

Only 2 network round-trips per reply: /v1/lookup then /v1/shops/{id} for
country detail. All calls are cheap read-only.
"""
import logging
import os
from typing import Optional

import httpx

log = logging.getLogger(__name__)

TRENDTRACK_BASE = "https://api.trendtrack.io/v1"
API_KEY = os.getenv("TRENDTRACK_API_KEY", "")

# Countries Trendfeed will work with (section 3 + user resolution of section 12).
ALLOWED_COUNTRIES = {
    "US", "CA", "AU", "NZ", "GB",
    # Western Europe
    "IE", "FR", "DE", "NL", "BE", "LU", "AT", "CH", "IT", "ES", "PT",
    "DK", "SE", "NO", "FI", "IS",
}

# Hard-exclude countries (per user's section 12 resolution).
# A brand whose top-country traffic is dominated by these is treated as
# based there and disqualified, regardless of the domain TLD.
EXCLUDED_COUNTRIES = {"IN", "PK"}


def _headers() -> dict:
    return {"Authorization": f"Bearer {API_KEY}"}


async def resolve_brand(query: str) -> Optional[dict]:
    """
    Resolve a domain or brand name to a Trendtrack shop.

    Returns a normalised dict with:
      shop_id, domain, name, active_ads, monthly_visits,
      match_type (exact|fuzzy|multi|none), matched_on (domain|name|multiple)

    Or None if Trendtrack has nothing on this brand.

    The caller decides whether a `fuzzy` name match is trustworthy. For emails
    from prospects at a corporate domain, prefer resolve_brand(prospect_domain)
    first; only fall back to brand-name lookup if that returns None.
    """
    if not API_KEY:
        log.warning("TRENDTRACK_API_KEY not set — brand resolution skipped")
        return None

    params = {"q": query, "type": "shop", "limit": 5}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"{TRENDTRACK_BASE}/lookup", headers=_headers(), params=params
            )
            r.raise_for_status()
            data = r.json().get("data") or []
    except Exception as e:
        log.error(f"Trendtrack lookup failed for {query!r}: {e}")
        return None

    if not data:
        return None

    top = data[0]
    shop = top.get("shop") or {}
    advertiser = top.get("advertiser") or {}
    signals = top.get("signals") or {}
    match_type = top.get("matchType") or "fuzzy"
    matched_on = top.get("matchField") or "name"

    # If we got more than one exact result on the same field, flag it — the
    # caller should escalate or take the highest-visits option.
    exact_hits = [
        d for d in data if (d.get("matchType") == "exact")
    ]
    if len(exact_hits) > 1:
        match_type = "multi"
        matched_on = "multiple"

    return {
        "shop_id": shop.get("id") or "",
        "advertiser_id": advertiser.get("id") or "",
        "domain": shop.get("domain") or "",
        "name": shop.get("name") or "",
        "active_ads": int(signals.get("activeAds") or 0),
        "monthly_visits": int(signals.get("monthlyVisits") or 0),
        "match_type": match_type,
        "matched_on": matched_on,
        "candidates_returned": len(data),
    }


async def get_shop_detail(shop_id: str) -> Optional[dict]:
    """
    Pull the fuller shop record — country code + top traffic countries.

    Returns:
      {
        "country_code": "US" | None,
        "top_countries": [{"code": "US", "share": 0.81}, ...],
        "monthly_visits": 106087,
        "active_ads": 148,
      }
    """
    if not API_KEY or not shop_id:
        return None
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"{TRENDTRACK_BASE}/shops/{shop_id}", headers=_headers()
            )
            r.raise_for_status()
            body = r.json()
    except Exception as e:
        log.error(f"Trendtrack shop detail failed for {shop_id}: {e}")
        return None

    # The endpoint sometimes returns the shop directly, sometimes wrapped in
    # {"data": {...}}. Normalise.
    shop = body.get("data") if isinstance(body.get("data"), dict) else body
    profile = shop.get("profile") or {}
    traffic = shop.get("traffic") or {}
    advertising = shop.get("advertising") or {}
    top_countries = [
        {"code": (c.get("countryCode") or "").upper(), "share": float(c.get("share") or 0)}
        for c in (traffic.get("topCountries") or [])
    ]
    return {
        "country_code": (profile.get("countryCode") or "").upper() or None,
        "top_countries": top_countries,
        "monthly_visits": int(traffic.get("monthlyVisits") or 0),
        "active_ads": int(advertising.get("activeAds") or 0),
    }


def classify_geography(country_code: Optional[str], top_countries: list[dict]) -> dict:
    """
    Apply the section 3 + section 12 geography rules.

    Returns:
      {
        "verdict": "allowed" | "not_allowed" | "unknown",
        "label": human-readable one-line summary for the Slack intel block,
        "top_country_code": e.g. "US",
        "top_country_share": e.g. 0.81,
      }

    Rules (user's section 12 resolution):
      - Traffic dominated by IN or PK (top country) → not allowed
      - Top country is in ALLOWED_COUNTRIES → allowed
      - Otherwise unknown (approver decides)

    profile.countryCode is treated as a secondary hint only; the deciding
    signal is traffic origin, since a UK-registered brand can serve a US
    audience and vice versa.
    """
    top_code = top_countries[0]["code"] if top_countries else ""
    top_share = top_countries[0]["share"] if top_countries else 0.0

    if not top_code and not country_code:
        return {
            "verdict": "unknown",
            "label": "unknown",
            "top_country_code": "",
            "top_country_share": 0.0,
        }

    # Hard exclusion: dominated by IN/PK.
    if top_code in EXCLUDED_COUNTRIES:
        return {
            "verdict": "not_allowed",
            "label": f"{top_code} ({int(top_share * 100)}%) — excluded",
            "top_country_code": top_code,
            "top_country_share": top_share,
        }

    if top_code in ALLOWED_COUNTRIES:
        return {
            "verdict": "allowed",
            "label": f"{top_code} ({int(top_share * 100)}% of traffic)",
            "top_country_code": top_code,
            "top_country_share": top_share,
        }

    # Fall back to profile countryCode if traffic didn't decide.
    if country_code in EXCLUDED_COUNTRIES:
        return {
            "verdict": "not_allowed",
            "label": f"{country_code} (profile) — excluded",
            "top_country_code": top_code,
            "top_country_share": top_share,
        }
    if country_code in ALLOWED_COUNTRIES:
        return {
            "verdict": "allowed",
            "label": f"{country_code} (profile)",
            "top_country_code": top_code,
            "top_country_share": top_share,
        }

    return {
        "verdict": "unknown",
        "label": f"{top_code or country_code}",
        "top_country_code": top_code,
        "top_country_share": top_share,
    }


async def list_static_ads(advertiser_id: str, limit: int = 3) -> list[dict]:
    """
    List the most recent ACTIVE static (image) ads for an advertiser.

    Returns a list ordered newest-first, each entry:
      { "id": "facebook_...", "image_url": "https://medias.trendtrack.io/..." }

    Empty list if the API errors, if no key is set, or if the brand has no
    active static ads. Video ads are excluded — see the follow-up spec.
    """
    if not API_KEY or not advertiser_id:
        return []
    params = {
        "limit": str(limit),
        "status": "active",
        "mediaType": "image",
        "sortBy": "newest",
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"{TRENDTRACK_BASE}/advertisers/{advertiser_id}/ads",
                headers=_headers(),
                params=params,
            )
            r.raise_for_status()
            rows = r.json().get("data") or []
    except Exception as e:
        log.error(f"Trendtrack ad list failed for {advertiser_id}: {e}")
        return []

    result = []
    for row in rows:
        media = row.get("media") or {}
        media_url = (
            media.get("mediaUrl") if isinstance(media, dict) else None
        ) or row.get("mediaUrl")
        if not media_url:
            continue
        result.append({"id": row.get("id"), "image_url": media_url})
    return result


async def get_ad_detail(ad_id: str) -> Optional[dict]:
    """
    Pull full ad detail. Returns a normalised dict with just the fields the
    drafter needs:
      { "id", "image_url", "ad_copy", "cta", "landing_product",
        "landing_url", "ad_library_url" }

    None if lookup fails.
    """
    if not API_KEY or not ad_id:
        return None
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"{TRENDTRACK_BASE}/ads/{ad_id}",
                headers=_headers(),
            )
            r.raise_for_status()
            body = r.json()
    except Exception as e:
        log.error(f"Trendtrack ad detail failed for {ad_id}: {e}")
        return None

    data = body.get("data") if isinstance(body.get("data"), dict) else body
    media = data.get("media") or {}
    content = data.get("content") or {}
    links = data.get("links") or {}
    return {
        "id": data.get("id"),
        "image_url": (media.get("mediaUrl") if isinstance(media, dict) else None) or "",
        "ad_copy": (content.get("body") or "")[:600],
        "cta": content.get("callToAction") or "",
        "landing_product": content.get("ctaDescription") or "",
        "landing_url": content.get("landingPageUrl") or links.get("landingPageUrl") or "",
        "ad_library_url": links.get("adLibraryUrl") or "",
    }


async def get_top_static_ads_for_brand(advertiser_id: str, limit: int = 3) -> list[dict]:
    """
    Convenience wrapper for the follow-up engine.

    Lists the top N static ads, then hydrates each with full detail (ad copy
    + landing product + image URL). Returns the enriched list, dropping any
    ads that fail to hydrate.
    """
    listing = await list_static_ads(advertiser_id, limit=limit)
    hydrated = []
    for ad in listing:
        detail = await get_ad_detail(ad["id"])
        if detail and detail.get("image_url"):
            hydrated.append(detail)
    return hydrated


async def resolve_and_score(
    email_domain: str,
    brand_name_fallback: str = "",
) -> dict:
    """
    One-shot resolver used by the reply agent per section 6.

    Prefers the prospect's email domain; falls back to brand name if the
    domain is a free provider (gmail/outlook/etc.) or Trendtrack returns
    nothing.

    Returns a dict shaped for the Slack intel block:
      {
        "resolved": True/False,
        "confidence_label": "confirmed via email domain" | "name match only" | "unconfirmed",
        "shop_id": "...",
        "domain": "frownies.com",
        "name": "The Frownies",
        "monthly_visits": 106087,
        "active_ads": 148,
        "candidates_returned": 1,
        "geography": {verdict, label, top_country_code, top_country_share},
      }
    """
    free_providers = {
        "gmail.com", "googlemail.com", "yahoo.com", "yahoo.co.uk",
        "outlook.com", "hotmail.com", "hotmail.co.uk", "live.com",
        "icloud.com", "me.com", "aol.com", "proton.me", "protonmail.com",
        "msn.com",
    }
    domain = (email_domain or "").lower().strip().lstrip("@")

    result: Optional[dict] = None
    confidence = "unconfirmed"

    if domain and domain not in free_providers:
        result = await resolve_brand(domain)
        if result and result["match_type"] == "exact":
            confidence = "confirmed via email domain"
        elif result:
            confidence = "name match only"

    if not result and brand_name_fallback:
        result = await resolve_brand(brand_name_fallback)
        if result:
            confidence = "name match only"

    if not result:
        return {
            "resolved": False,
            "confidence_label": "unconfirmed",
            "shop_id": "",
            "advertiser_id": "",
            "domain": domain,
            "name": brand_name_fallback,
            "monthly_visits": 0,
            "active_ads": 0,
            "candidates_returned": 0,
            "geography": {"verdict": "unknown", "label": "unknown",
                          "top_country_code": "", "top_country_share": 0.0},
        }

    # Multiple exact hits — force approver review.
    if result["match_type"] == "multi":
        confidence = "multiple exact matches — needs review"

    detail = await get_shop_detail(result["shop_id"])
    geography = classify_geography(
        detail.get("country_code") if detail else None,
        detail.get("top_countries") if detail else [],
    )

    return {
        "resolved": True,
        "confidence_label": confidence,
        "shop_id": result["shop_id"],
        "advertiser_id": result.get("advertiser_id") or "",
        "domain": result["domain"],
        "name": result["name"],
        "monthly_visits": result["monthly_visits"] or (detail or {}).get("monthly_visits", 0),
        "active_ads": result["active_ads"] or (detail or {}).get("active_ads", 0),
        "candidates_returned": result["candidates_returned"],
        "geography": geography,
    }
