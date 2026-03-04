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
        "email": lead_email,
        "limit": 1,
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                f"{PLUSVIBE_BASE}/unibox/emails",
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            data = response.json()
            emails = data.get("emails") or data.get("data") or []
            if emails:
                return str(emails[0].get("id") or emails[0].get("email_id") or "")
    except Exception:
        pass
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

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            f"{PLUSVIBE_BASE}/unibox/emails/reply",
            headers=headers,
            params=params,
            json=payload,
        )
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
