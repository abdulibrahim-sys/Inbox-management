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
            return [
                {
                    "id": e.get("id"),
                    "message_id": e.get("message_id"),
                    "from": e.get("lead") if e.get("is_unread") is not None else "",
                    "subject": e.get("subject", ""),
                    "body": _strip_html(
                        e.get("body", {}).get("text", "")
                        or e.get("content_preview", "")
                    ),
                    "timestamp": e.get("timestamp_created", ""),
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
