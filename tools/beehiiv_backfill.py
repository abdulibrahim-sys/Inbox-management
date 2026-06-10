"""
Backfill tool: pull every positive-reply lead (INTERESTED or MEETING_BOOKED)
from every PlusVibe campaign and subscribe them to Beehiiv.

- Iterates all campaigns (ACTIVE/PAUSED/ARCHIVED) so historical positives
  don't get missed.
- Uses /unibox/emails?label=… with page_trail pagination (the only working
  filter — /leads endpoint returns 404).
- Dedupes by email locally before calling Beehiiv. Beehiiv also dedupes via
  HTTP 409, so re-runs are safe.

Usage:
    python tools/beehiiv_backfill.py             # dry run, prints counts
    python tools/beehiiv_backfill.py --apply     # actually subscribes
"""
import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import httpx

PLUSVIBE_BASE = "https://api.plusvibe.ai/api/v1"
BEEHIIV_API_URL = "https://api.beehiiv.com/v2"

PLUSVIBE_API_KEY = os.getenv("PLUSVIBE_API_KEY")
WORKSPACE_ID = os.getenv("PLUSVIBE_WORKSPACE_ID")
BEEHIIV_API_KEY = os.getenv("BEEHIIV_API_KEY")
_pub_raw = os.getenv("BEEHIIV_PUBLICATION_ID", "")
BEEHIIV_PUB_ID = _pub_raw if _pub_raw.startswith("pub_") else f"pub_{_pub_raw}"

POSITIVE_LABELS = ("INTERESTED", "MEETING_BOOKED")
MAX_PAGES = 200


async def list_campaigns(client: httpx.AsyncClient) -> list[dict]:
    r = await client.get(
        f"{PLUSVIBE_BASE}/campaign/list",
        headers={"x-api-key": PLUSVIBE_API_KEY},
        params={"workspace_id": WORKSPACE_ID},
    )
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else (data.get("data") or [])


async def _get_with_retry(
    client: httpx.AsyncClient, url: str, *, headers: dict, params: dict, attempts: int = 4
) -> Optional[dict]:
    """GET with backoff on 502/503/504/timeouts. Returns parsed JSON or None on persistent failure."""
    delay = 1.0
    last_err: str = ""
    for i in range(attempts):
        try:
            r = await client.get(url, headers=headers, params=params)
            if r.status_code in (502, 503, 504):
                last_err = f"http_{r.status_code}"
                await asyncio.sleep(delay)
                delay *= 2
                continue
            r.raise_for_status()
            return r.json() or {}
        except httpx.HTTPStatusError as e:
            last_err = f"http_{e.response.status_code}"
            break
        except Exception as e:
            last_err = type(e).__name__
            await asyncio.sleep(delay)
            delay *= 2
    print(f"      page persisted failing: {last_err}")
    return None


async def list_emails_for_label(
    client: httpx.AsyncClient, campaign_id: str, label: str
) -> list[dict]:
    """Paginate through /unibox/emails for one (campaign, label) pair, with retries."""
    headers = {"x-api-key": PLUSVIBE_API_KEY}
    base = {
        "workspace_id": WORKSPACE_ID,
        "campaign_id": campaign_id,
        "email_type": "received",
        "label": label,
    }
    out: list[dict] = []
    cursor: Optional[str] = None
    for _ in range(MAX_PAGES):
        params = dict(base)
        if cursor:
            params["page_trail"] = cursor
        data = await _get_with_retry(
            client, f"{PLUSVIBE_BASE}/unibox/emails", headers=headers, params=params
        )
        if data is None:
            break
        page = data.get("data") or []
        if not page:
            break
        out.extend(page)
        nxt = data.get("page_trail")
        if not nxt or nxt == cursor:
            break
        cursor = nxt
    return out


async def get_lead_data(client: httpx.AsyncClient, email: str, campaign_id: str) -> dict:
    """Fetch first/last name from PlusVibe (best-effort)."""
    try:
        r = await client.get(
            f"{PLUSVIBE_BASE}/lead/get",
            headers={"x-api-key": PLUSVIBE_API_KEY},
            params={"workspace_id": WORKSPACE_ID, "email": email},
        )
        r.raise_for_status()
        rows = r.json() or []
        if not isinstance(rows, list):
            return {}
        for row in rows:
            if row.get("campaign") == campaign_id or row.get("campaign_id") == campaign_id:
                return row.get("lead_data") or {}
        return (rows[0].get("lead_data") or {}) if rows else {}
    except Exception:
        return {}


async def beehiiv_subscribe(
    client: httpx.AsyncClient, email: str, first_name: str, last_name: str
) -> str:
    payload: dict = {"email": email, "status": "active", "send_welcome_email": False}
    if first_name:
        payload["first_name"] = first_name
    if last_name:
        payload["last_name"] = last_name
    try:
        r = await client.post(
            f"{BEEHIIV_API_URL}/publications/{BEEHIIV_PUB_ID}/subscriptions",
            json=payload,
            headers={
                "Authorization": f"Bearer {BEEHIIV_API_KEY}",
                "Content-Type": "application/json",
            },
        )
    except Exception as e:
        return f"failed:{type(e).__name__}"
    if r.status_code in (200, 201):
        return "subscribed"
    if r.status_code == 409:
        return "already_exists"
    return f"failed:{r.status_code}:{r.text[:120]}"


async def main(apply_changes: bool):
    print("=" * 60)
    print(f"Beehiiv Backfill — POSITIVE LABELS ({', '.join(POSITIVE_LABELS)})")
    print(f"Mode: {'APPLY' if apply_changes else 'DRY RUN'}")
    print("=" * 60)

    if not all([PLUSVIBE_API_KEY, WORKSPACE_ID, BEEHIIV_API_KEY, BEEHIIV_PUB_ID]):
        print("ERROR: Missing env vars (PLUSVIBE/BEEHIIV credentials)")
        return

    # leads: {email: {first_name, last_name, campaign_id, label, latest_ts}}
    leads: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30) as client:
        camps = await list_campaigns(client)
        print(f"\nDiscovered {len(camps)} campaigns. Scanning each for positive labels...\n")

        for c in camps:
            cid = c.get("id") or c.get("_id")
            cname = c.get("name", "?")
            if not cid:
                continue
            for label in POSITIVE_LABELS:
                emails = await list_emails_for_label(client, cid, label)
                if not emails:
                    continue
                for e in emails:
                    addr = (e.get("from_address_email") or e.get("lead") or "").strip().lower()
                    if not addr:
                        continue
                    # PlusVibe's canonical lead identity (used for /lead/get enrichment).
                    # Sometimes differs from `from_address_email` if the lead replied
                    # from a different domain.
                    lead_email = (e.get("lead") or addr).strip().lower()
                    ts = e.get("timestamp_created") or ""
                    prev = leads.get(addr)
                    if prev is None or ts > prev.get("latest_ts", ""):
                        from_json = e.get("from_address_json") or []
                        display = (from_json[0].get("name") or "").strip() if from_json else ""
                        first, last = "", ""
                        if display and " " in display:
                            first, last = display.split(" ", 1)
                        elif display:
                            first = display
                        leads[addr] = {
                            "email": addr,
                            "lead_email": lead_email,
                            "first_name": first,
                            "last_name": last,
                            "campaign_id": cid,
                            "campaign_name": cname,
                            "label": label,
                            "latest_ts": ts,
                        }
                print(f"  [{cname}] {label}: {len(emails)} msgs (running unique leads: {len(leads)})")

        print(f"\nUnique positive leads across all campaigns: {len(leads)}")

        # Enrich names via /lead/get using PlusVibe's canonical lead identity
        # (some leads reply from a different domain than we mailed)
        print("\nEnriching first/last names via /lead/get …")
        missing_names = [
            l for l in leads.values()
            if not (l.get("first_name") and l.get("last_name"))
        ]
        BATCH = 10
        enriched_count = 0
        for i in range(0, len(missing_names), BATCH):
            batch = missing_names[i:i + BATCH]
            datas = await asyncio.gather(
                *(get_lead_data(client, l["lead_email"], l["campaign_id"]) for l in batch),
                return_exceptions=True,
            )
            for l, ld in zip(batch, datas):
                if isinstance(ld, dict) and ld:
                    if ld.get("first_name"):
                        l["first_name"] = ld["first_name"]
                        enriched_count += 1
                    if ld.get("last_name"):
                        l["last_name"] = ld["last_name"]
        print(f"  Enriched {enriched_count}/{len(missing_names)} leads")

        if not apply_changes:
            print("\n--- DRY RUN — sample of first 10 leads ---")
            for l in list(leads.values())[:10]:
                print(f"  {l['email']:40s} {l['first_name']!r:20s} {l['last_name']!r:20s} "
                      f"[{l['label']}] {l['campaign_name'][:30]}")
            print(f"\nRe-run with --apply to subscribe {len(leads)} leads to Beehiiv.")
            return

        print(f"\nSubscribing {len(leads)} leads to Beehiiv …\n")
        counts = {"subscribed": 0, "already_exists": 0, "failed": 0}
        failures: list[tuple[str, str]] = []
        for i, l in enumerate(leads.values(), 1):
            result = await beehiiv_subscribe(client, l["email"], l["first_name"], l["last_name"])
            if result == "subscribed":
                counts["subscribed"] += 1
                marker = "+"
            elif result == "already_exists":
                counts["already_exists"] += 1
                marker = "="
            else:
                counts["failed"] += 1
                failures.append((l["email"], result))
                marker = "x"
            if i % 25 == 0 or i == len(leads):
                print(f"  [{i}/{len(leads)}] +{counts['subscribed']} ={counts['already_exists']} x{counts['failed']}")
            else:
                print(f"  [{i}/{len(leads)}] {marker} {l['email']}")
            await asyncio.sleep(0.1)  # gentle pacing

        print("\n" + "=" * 60)
        print(f"  Newly subscribed : {counts['subscribed']}")
        print(f"  Already on list  : {counts['already_exists']}")
        print(f"  Failed           : {counts['failed']}")
        print("=" * 60)
        if failures:
            print("\nFailures (first 20):")
            for email, reason in failures[:20]:
                print(f"  {email}: {reason}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Actually subscribe (default is dry run)")
    args = parser.parse_args()
    asyncio.run(main(apply_changes=args.apply))
