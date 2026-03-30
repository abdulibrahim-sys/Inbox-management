"""
Backfill tool: fetch all leads marked as interested from PlusVibe
and subscribe them to Beehiiv if not already subscribed.

Usage:
    python tools/beehiiv_backfill.py
"""
import asyncio
import os
import sys
from pathlib import Path

# Load .env from project root
sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import httpx

PLUSVIBE_BASE = "https://api.plusvibe.ai/api/v1"
BEEHIIV_API_URL = "https://api.beehiiv.com/v2"

PLUSVIBE_API_KEY = os.getenv("PLUSVIBE_API_KEY")
WORKSPACE_ID = os.getenv("PLUSVIBE_WORKSPACE_ID")
BEEHIIV_API_KEY = os.getenv("BEEHIIV_API_KEY")
BEEHIIV_PUB_ID = os.getenv("BEEHIIV_PUBLICATION_ID")

PV_HEADERS = {"x-api-key": PLUSVIBE_API_KEY}


async def fetch_interested_leads() -> list[dict]:
    """Fetch all leads marked as interested from PlusVibe, paginating through all pages."""
    leads = []
    page = 1
    per_page = 100

    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            params = {
                "workspace_id": WORKSPACE_ID,
                "label": "interested",
                "page": page,
                "per_page": per_page,
            }
            resp = await client.get(
                f"{PLUSVIBE_BASE}/leads",
                headers=PV_HEADERS,
                params=params,
            )

            if resp.status_code != 200:
                print(f"PlusVibe leads endpoint returned {resp.status_code}: {resp.text[:300]}")
                # Try alternative endpoint
                resp2 = await client.get(
                    f"{PLUSVIBE_BASE}/lead/list",
                    headers=PV_HEADERS,
                    params=params,
                )
                if resp2.status_code != 200:
                    print(f"Alternative endpoint also failed {resp2.status_code}: {resp2.text[:300]}")
                    break
                data = resp2.json()
            else:
                data = resp.json()

            batch = data.get("data") or data.get("leads") or []
            if not batch:
                break

            leads.extend(batch)
            print(f"  Page {page}: fetched {len(batch)} leads (total so far: {len(leads)})")

            # Check if there are more pages
            total = data.get("total") or data.get("meta", {}).get("total") or 0
            if len(leads) >= total or len(batch) < per_page:
                break
            page += 1

    return leads


async def subscribe_to_beehiiv(email: str, first_name: str = "", last_name: str = "") -> str:
    """Subscribe a single lead to Beehiiv. Returns 'subscribed', 'already_exists', or 'failed'."""
    payload = {
        "email": email,
        "status": "active",
        "send_welcome_email": False,
    }
    if first_name:
        payload["first_name"] = first_name
    if last_name:
        payload["last_name"] = last_name

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{BEEHIIV_API_URL}/publications/{BEEHIIV_PUB_ID}/subscriptions",
            json=payload,
            headers={
                "Authorization": f"Bearer {BEEHIIV_API_KEY}",
                "Content-Type": "application/json",
            },
        )

    if resp.status_code in (200, 201):
        return "subscribed"
    elif resp.status_code == 409:
        return "already_exists"
    else:
        print(f"  Beehiiv error for {email}: {resp.status_code} {resp.text[:200]}")
        return "failed"


async def main():
    print("=" * 50)
    print("Beehiiv Backfill — Interested Leads")
    print("=" * 50)

    if not all([PLUSVIBE_API_KEY, WORKSPACE_ID, BEEHIIV_API_KEY, BEEHIIV_PUB_ID]):
        print("ERROR: Missing env vars. Check PLUSVIBE_API_KEY, PLUSVIBE_WORKSPACE_ID, BEEHIIV_API_KEY, BEEHIIV_PUBLICATION_ID")
        return

    print("\nFetching interested leads from PlusVibe...")
    leads = await fetch_interested_leads()
    print(f"\nFound {len(leads)} interested leads total.\n")

    if not leads:
        print("No leads found. Check if PlusVibe API endpoint supports label filtering.")
        return

    subscribed = 0
    already_exists = 0
    failed = 0

    for i, lead in enumerate(leads, 1):
        email = lead.get("email") or lead.get("from_email") or ""
        first_name = lead.get("first_name") or ""
        last_name = lead.get("last_name") or ""

        if not email:
            print(f"  [{i}/{len(leads)}] Skipping — no email found in lead record")
            continue

        result = await subscribe_to_beehiiv(email, first_name, last_name)
        name = f"{first_name} {last_name}".strip() or email

        if result == "subscribed":
            print(f"  [{i}/{len(leads)}] Subscribed: {name} <{email}>")
            subscribed += 1
        elif result == "already_exists":
            print(f"  [{i}/{len(leads)}] Already subscribed: {email}")
            already_exists += 1
        else:
            print(f"  [{i}/{len(leads)}] Failed: {email}")
            failed += 1

        # Small delay to avoid rate limits
        await asyncio.sleep(0.2)

    print("\n" + "=" * 50)
    print(f"Done.")
    print(f"  Newly subscribed : {subscribed}")
    print(f"  Already on list  : {already_exists}")
    print(f"  Failed           : {failed}")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
