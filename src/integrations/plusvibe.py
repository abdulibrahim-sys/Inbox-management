import os
import httpx
from pydantic import BaseModel
from typing import Optional


PLUSVIBE_BASE = "https://api.plusvibe.ai/api/v1"
API_KEY = os.getenv("PLUSVIBE_API_KEY")
WORKSPACE_ID = os.getenv("PLUSVIBE_WORKSPACE_ID")


class ReplyPayload(BaseModel):
    """Normalised reply data extracted from PlusVibe webhook."""
    email_id: str          # unibox email ID needed to send reply
    lead_id: Optional[str] = None
    from_email: str        # prospect's email
    to_email: str          # our sending email (actual_replied_from)
    subject: str
    body: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company_name: Optional[str] = None
    website: Optional[str] = None
    campaign_id: Optional[str] = None
    campaign_name: Optional[str] = None


def parse_webhook(payload: dict) -> ReplyPayload:
    """
    Parse a LEAD_MARKED_AS_INTERESTED webhook payload into a normalised ReplyPayload.

    Key fields from this event type:
      email, first_name, last_name, company_name, company_website,
      last_lead_reply / text_body, last_lead_reply_subject / latest_subject,
      actual_replied_from, lead_id, campaign_id, campaign_name
    """
    # The payload may be wrapped in a "data" key or flat
    data = payload.get("data", payload)

    body_text = (
        data.get("last_lead_reply")
        or data.get("text_body")
        or data.get("latest_message")
        or data.get("body")
        or ""
    )
    subject = (
        data.get("last_lead_reply_subject")
        or data.get("latest_subject")
        or data.get("subject")
        or ""
    )

    return ReplyPayload(
        # email_id will be populated after fetching from unibox if not present
        email_id=str(data.get("email_id") or data.get("id") or ""),
        lead_id=str(data.get("lead_id") or ""),
        from_email=data.get("email") or data.get("from_address") or data.get("from") or "",
        to_email=data.get("actual_replied_from") or data.get("to_address") or data.get("to") or "",
        subject=subject,
        body=_strip_html(body_text),
        first_name=data.get("first_name"),
        last_name=data.get("last_name"),
        company_name=data.get("company_name"),
        website=data.get("company_website") or data.get("website"),
        campaign_id=str(data.get("campaign_id") or ""),
        campaign_name=data.get("campaign_name"),
    )


async def fetch_latest_email_id(lead_email: str) -> Optional[str]:
    """
    Look up the most recent unibox email ID for a lead by their email address.
    Used when the webhook payload doesn't include a direct email_id.
    """
    headers = {"x-api-key": API_KEY}
    params = {
        "workspace_id": WORKSPACE_ID,
        "lead": lead_email,
        "email_type": "received",
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                f"{PLUSVIBE_BASE}/unibox/emails",
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            result = response.json()
            emails = result.get("data") or []
            if emails:
                return str(emails[0].get("id") or "")
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Unibox email lookup failed: {e}")
    return None


def _strip_html(text: str) -> str:
    """Remove basic HTML tags and decode common entities."""
    import re
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return " ".join(text.split()).strip()


async def send_reply(reply_to_id: str, subject: str, from_email: str, to_email: str, body: str) -> dict:
    """Send a reply via PlusVibe Unibox API."""
    headers = {
        "x-api-key": API_KEY,
        "Content-Type": "application/json",
    }
    params = {"workspace_id": WORKSPACE_ID}
    payload = {
        "reply_to_id": reply_to_id,
        "subject": subject if subject.startswith("Re:") else f"Re: {subject}",
        "from": from_email,
        "to": to_email,
        "body": body,
    }

    import logging
    log = logging.getLogger(__name__)
    log.info(f"Sending reply: reply_to_id={reply_to_id}, from={from_email}, to={to_email}")

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            f"{PLUSVIBE_BASE}/unibox/emails/reply",
            headers=headers,
            params=params,
            json=payload,
        )
        if response.status_code != 200:
            log.error(f"PlusVibe reply API error: {response.status_code} — {response.text}")
        response.raise_for_status()
        return response.json()


async def get_lead_status(lead_email: str) -> Optional[str]:
    """Check a lead's label/status in PlusVibe. Returns the label string or None."""
    headers = {"x-api-key": API_KEY}
    params = {
        "workspace_id": WORKSPACE_ID,
        "email": lead_email,
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                f"{PLUSVIBE_BASE}/lead/get",
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            data = response.json()
            # Label might be nested in lead_data or top-level
            return data.get("label") or data.get("lead_data", {}).get("label")
    except Exception:
        return None


async def get_email_thread(lead_email: str) -> list[dict]:
    """Fetch the full email thread for a lead from the unibox."""
    headers = {"x-api-key": API_KEY}
    params = {
        "workspace_id": WORKSPACE_ID,
        "lead": lead_email,
        "email_type": "all",
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                f"{PLUSVIBE_BASE}/unibox/emails",
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            result = response.json()
            emails = result.get("data") or []
            # PlusVibe unibox rows use `from_address_email` for the actual
            # sender. The old code was reading `lead` (which is always the
            # prospect) and lost direction info — every message looked like
            # it came from the prospect. Reads `to_address_email_list` as
            # the recipient field for direction cross-checks.
            def _from(e: dict) -> str:
                return (
                    e.get("from_address_email")
                    or e.get("from")
                    or (e.get("from_address") or {}).get("email", "")
                    if isinstance(e.get("from_address"), dict) else ""
                ) or e.get("from_address_email", "") or ""

            def _to(e: dict) -> str:
                tos = e.get("to_address_email_list") or []
                return tos[0] if tos else e.get("eaccount") or ""

            return [
                {
                    "id": e.get("id"),
                    "message_id": e.get("message_id"),
                    "from": _from(e),
                    "to": _to(e),
                    "subject": e.get("subject", ""),
                    "body": _strip_html(
                        (e.get("body") or {}).get("text", "")
                        or (e.get("body") or {}).get("html", "")
                        or e.get("content_preview", "")
                    ),
                    "timestamp": e.get("timestamp_created", ""),
                    "label": e.get("label") or "",
                }
                for e in emails
            ]
    except Exception:
        return []


async def save_draft(parent_message_id: str, from_email: str, subject: str, body: str) -> dict:
    """Save an email as a draft in PlusVibe unibox (not auto-send)."""
    headers = {
        "x-api-key": API_KEY,
        "Content-Type": "application/json",
    }
    params = {"workspace_id": WORKSPACE_ID}
    payload = {
        "parent_message_id": parent_message_id,
        "from": from_email,
        "subject": subject if subject.startswith("Re:") else f"Re: {subject}",
        "body": body,
    }

    import logging
    log = logging.getLogger(__name__)
    log.info(f"Saving draft: parent_message_id={parent_message_id}, from={from_email}")

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            f"{PLUSVIBE_BASE}/unibox/emails/save-as-draft",
            headers=headers,
            params=params,
            json=payload,
        )
        if response.status_code != 200:
            log.error(f"PlusVibe save-draft error: {response.status_code} — {response.text}")
        response.raise_for_status()
        return response.json()


async def get_workspaces() -> dict:
    """Utility: list accessible workspaces (useful for finding workspace_id)."""
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.get(
            f"{PLUSVIBE_BASE}/auth/workspaces",
            headers={"x-api-key": API_KEY},
        )
        response.raise_for_status()
        return response.json()


async def list_received_emails(campaign_id: str, label: Optional[str] = None) -> list[dict]:
    """
    Pull the latest page of received-email entries from PlusVibe's unibox.

    Used by the unibox poller as the trigger source for replies — independent
    of PlusVibe's `LEAD_MARKED_AS_INTERESTED` tagging, which can lag or be
    disabled on a campaign. Each entry includes id, lead, lead_id, subject,
    body.html, eaccount, label, timestamp_created.

    Single page only; the poller dedups via Redis and is called every 2 min
    so the newest page is enough. Pass `label` to restrict to one bucket
    (INTERESTED, MEETING_BOOKED, OUT_OF_OFFICE, AUTOMATIC_REPLY,
    NOT_INTERESTED) — critical for making sure INTERESTED replies aren't
    hidden by a surge of OOOs on the all-labels feed. For full historical
    pulls, use `list_received_emails_paginated`.
    """
    headers = {"x-api-key": API_KEY}
    params = {
        "workspace_id": WORKSPACE_ID,
        "campaign_id": campaign_id,
        "email_type": "received",
    }
    if label:
        params["label"] = label
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(
                f"{PLUSVIBE_BASE}/unibox/emails",
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            return response.json().get("data") or []
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"list_received_emails failed ({campaign_id}): {e}")
        return []


async def list_received_emails_paginated(
    campaign_id: str,
    label: Optional[str] = None,
    stop_before_iso: Optional[str] = None,
    max_pages: int = 50,
) -> list[dict]:
    """
    Walk every page of received emails for a campaign, optionally filtered by
    `label` (MEETING_BOOKED, INTERESTED, OUT_OF_OFFICE, AUTOMATIC_REPLY, ...).

    Pages are sorted newest-first by `timestamp_created`. If `stop_before_iso`
    is set (YYYY-MM-DD or full ISO), we stop pagination as soon as a page's
    oldest entry is older than that cutoff — cheap way to get "today only".
    """
    headers = {"x-api-key": API_KEY}
    base_params: dict = {
        "workspace_id": WORKSPACE_ID,
        "campaign_id": campaign_id,
        "email_type": "received",
    }
    if label:
        base_params["label"] = label

    results: list[dict] = []
    cursor: Optional[str] = None
    try:
        import asyncio as _asyncio
        async with httpx.AsyncClient(timeout=20) as client:
            for _ in range(max_pages):
                params = dict(base_params)
                if cursor:
                    params["page_trail"] = cursor
                # Retry 429s with exponential backoff — PlusVibe rate-limits
                # aggressively on paginated feeds.
                attempts = 0
                while True:
                    response = await client.get(
                        f"{PLUSVIBE_BASE}/unibox/emails",
                        headers=headers,
                        params=params,
                    )
                    if response.status_code != 429:
                        break
                    attempts += 1
                    if attempts > 5:
                        break
                    retry_after = float(response.headers.get("retry-after") or 0) or (2 ** attempts)
                    await _asyncio.sleep(min(retry_after, 30))
                response.raise_for_status()
                payload = response.json() or {}
                page = payload.get("data") or []
                if not page:
                    break
                # Space out subsequent pages so we don't burn back into 429.
                await _asyncio.sleep(0.4)
                results.extend(page)
                if stop_before_iso:
                    oldest = min((p.get("timestamp_created") or "") for p in page)
                    if oldest and oldest < stop_before_iso:
                        break
                next_cursor = payload.get("page_trail")
                if not next_cursor or next_cursor == cursor:
                    break
                cursor = next_cursor
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(
            f"list_received_emails_paginated failed ({campaign_id}, label={label}): {e}"
        )
    return results


async def get_lead_data(email: str, campaign_id: Optional[str] = None) -> Optional[dict]:
    """
    Fetch a lead's record (first_name/last_name/company_name/company_website/etc).

    PlusVibe returns one row per (lead, campaign) tuple. If `campaign_id` is
    given, prefer the matching row; otherwise return the first.
    """
    if not email:
        return None
    headers = {"x-api-key": API_KEY}
    params = {"workspace_id": WORKSPACE_ID, "email": email}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                f"{PLUSVIBE_BASE}/lead/get",
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            rows = response.json()
        if not isinstance(rows, list) or not rows:
            return None
        if campaign_id:
            for r in rows:
                if r.get("campaign") == campaign_id or r.get("campaign_id") == campaign_id:
                    return r.get("lead_data") or {}
        return rows[0].get("lead_data") or {}
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"get_lead_data failed ({email}): {e}")
        return None


async def get_campaign_stats(campaign_id: str, start_date: str, end_date: str) -> Optional[dict]:
    """
    Pull aggregate stats for a campaign over a date window.

    Dates are YYYY-MM-DD. The endpoint returns a list; we expect a single row
    keyed by campaign_id. Fields used downstream:
      sent_count, replied_count, positive_reply_count, bounced_count,
      unsubscribed_count, unique_opened_count, lead_contacted_count.
    """
    headers = {"x-api-key": API_KEY}
    params = {
        "workspace_id": WORKSPACE_ID,
        "campaign_id": campaign_id,
        "start_date": start_date,
        "end_date": end_date,
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                f"{PLUSVIBE_BASE}/campaign/stats",
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            rows = response.json()
            if isinstance(rows, list) and rows:
                return rows[0]
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Campaign stats fetch failed ({campaign_id}): {e}")
    return None


async def send_new_email(
    from_email: str, to_email: str, subject: str, body: str,
    camp_id: str, lead_id: str,
) -> dict:
    """
    Send a fresh (non-reply) email via PlusVibe's compose endpoint. Used by
    the follow-up engine when the original sending mailbox has been removed
    from PlusVibe — we open a new thread from a live mailbox.

    We use /unibox/emails/compose (not /unibox/emails/send) because /send
    accepts payloads and silently drops them, while /compose actually
    delivers. compose requires camp_id + lead_id — the send appears in the
    unibox and threads into that campaign's activity even if the campaign
    is paused. Discovered by trial 2026-07-28.

    Note the paired lookup:
      1. Call get_lead_records(to_email) to find valid (camp_id, lead_id)
         tuples for the prospect
      2. Prefer a record on an active campaign; fall back to any campaign
    """
    if not (camp_id and lead_id):
        raise ValueError("send_new_email requires camp_id and lead_id")

    headers = {
        "x-api-key": API_KEY,
        "Content-Type": "application/json",
    }
    params = {"workspace_id": WORKSPACE_ID}
    payload = {
        "camp_id": camp_id,
        "lead_id": lead_id,
        "from": from_email,
        "subject": subject,
        "body": body,
    }
    import logging
    log = logging.getLogger(__name__)
    log.info(
        f"send_new_email: from={from_email} to={to_email} "
        f"camp={camp_id} lead={lead_id}"
    )

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            f"{PLUSVIBE_BASE}/unibox/emails/compose",
            headers=headers, params=params, json=payload,
        )
        if response.status_code != 200:
            log.error(f"send_new_email error {response.status_code}: {response.text}")
        response.raise_for_status()
        return response.json()


async def get_lead_records(email: str) -> list[dict]:
    """
    Return every (lead_id, campaign_id) tuple PlusVibe has for the prospect.

    /lead/get returns one row per (email, campaign) pair — a lead added to
    5 campaigns has 5 rows. We surface the raw pair list so the caller can
    pick which campaign context to send under (prefer active > paused >
    archived).

    Each entry: {"lead_id": str, "campaign_id": str}
    """
    if not email:
        return []
    headers = {"x-api-key": API_KEY}
    params = {"workspace_id": WORKSPACE_ID, "email": email}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                f"{PLUSVIBE_BASE}/lead/get",
                headers=headers, params=params,
            )
            response.raise_for_status()
            rows = response.json()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"get_lead_records failed ({email}): {e}")
        return []
    if not isinstance(rows, list):
        return []
    out = []
    for r in rows:
        lead_id = str(r.get("_id") or r.get("id") or "")
        camp_id = str(r.get("campaign") or r.get("campaign_id") or "")
        if lead_id and camp_id:
            out.append({"lead_id": lead_id, "campaign_id": camp_id})
    return out


async def list_live_mailboxes(
    active_campaign_ids: list[str] | None = None,
    min_warmup_days: int = 21,
) -> list[dict]:
    """
    Return every mailbox connected to PlusVibe that's actually safe to send
    live outbound from. Filter chain (user's constraints, 2026-07-28):

    - status == ACTIVE (case-insensitive)
    - provider == GOOGLE_WORKSPACE — Google only, no Outlook/Microsoft365
    - warmup enabled at least `min_warmup_days` ago — skip pre-warmed
      mailboxes that are still ramping through initial reputation

    active_campaign_ids: retained for the API surface but no longer applied
      as a filter — account/list's cmps field is nested inside `payload` and
      not reliably populated, so we accept any mature Google mailbox. Pass
      the argument through if you want it available in future without a
      breaking change.

    Each entry: {email, provider, warmup_days, status, warmup_status}.
    """
    from datetime import datetime, timezone

    headers = {"x-api-key": API_KEY}
    params = {"workspace_id": WORKSPACE_ID}
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                f"{PLUSVIBE_BASE}/account/list",
                headers=headers, params=params,
            )
            response.raise_for_status()
            data = response.json()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"list_live_mailboxes failed: {e}")
        return []

    accounts = data.get("accounts", []) if isinstance(data, dict) else []
    now = datetime.now(timezone.utc)
    result = []
    for a in accounts:
        if (a.get("status") or "").upper() != "ACTIVE":
            continue
        if (a.get("provider") or "").upper() != "GOOGLE_WORKSPACE":
            continue
        # Warmup age
        enb = a.get("warmup_enb_dt") or ""
        warmup_days = 0
        if enb:
            try:
                wd = datetime.fromisoformat(enb.replace("Z", "+00:00"))
                warmup_days = (now - wd).days
            except Exception:
                pass
        if warmup_days < min_warmup_days:
            continue

        result.append({
            "email": a.get("email", ""),
            "provider": a.get("provider", ""),
            "status": a.get("status", ""),
            "warmup_status": a.get("warmup_status", ""),
            "warmup_days": warmup_days,
        })
    return result


async def list_campaign_mailboxes(campaign_id: str) -> list[dict]:
    """
    Return mailboxes (sending accounts) attached to a campaign.

    Each entry: {email, daily_limit, status, warmup_status}.
    Filtering by `cmps[].id == campaign_id`. Note the `/account/list` payload
    sometimes returns accounts with empty `cmps` — those are filtered out.
    """
    headers = {"x-api-key": API_KEY}
    params = {"workspace_id": WORKSPACE_ID}
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(
                f"{PLUSVIBE_BASE}/account/list",
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            data = response.json()
        accounts = data.get("accounts", []) if isinstance(data, dict) else []
        result = []
        for a in accounts:
            cmps = a.get("cmps") or []
            if any((c.get("id") or c.get("_id")) == campaign_id for c in cmps):
                result.append({
                    "email": a.get("email", ""),
                    "daily_limit": a.get("daily_limit"),
                    "status": a.get("status", ""),
                    "warmup_status": a.get("warmup_status", ""),
                })
        return result
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"list_campaign_mailboxes failed ({campaign_id}): {e}")
        return []


async def register_webhook(url: str, events: list[str] | None = None) -> dict:
    """Register our Railway URL as a PlusVibe webhook."""
    if events is None:
        events = ["LEAD_MARKED_AS_INTERESTED"]

    headers = {
        "x-api-key": API_KEY,
        "Content-Type": "application/json",
    }
    params = {"workspace_id": WORKSPACE_ID}
    payload = {
        "webhook_url": url,
        "event_types": events,
    }

    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(
            f"{PLUSVIBE_BASE}/webhooks",
            headers=headers,
            params=params,
            json=payload,
        )
        response.raise_for_status()
        return response.json()
