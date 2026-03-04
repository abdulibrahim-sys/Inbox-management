import os
import httpx
from pydantic import BaseModel
from typing import Optional


PLUSVIBE_BASE = "https://api.plusvibe.ai/api/v1"
API_KEY = os.getenv("PLUSVIBE_API_KEY")
WORKSPACE_ID = os.getenv("PLUSVIBE_WORKSPACE_ID")


class ReplyPayload(BaseModel):
    """Normalised reply data extracted from PlusVibe webhook."""
    email_id: str
    thread_id: Optional[str] = None
    from_email: str
    to_email: str
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
    Parse a PlusVibe webhook payload into a normalised ReplyPayload.
    Handles the nested structure PlusVibe sends.
    """
    data = payload.get("data", payload)
    lead = data.get("lead", data.get("contact", {}))

    return ReplyPayload(
        email_id=str(data.get("email_id") or data.get("id") or ""),
        thread_id=str(data.get("thread_id") or data.get("email_thread_id") or ""),
        from_email=data.get("from_address") or data.get("from") or lead.get("email", ""),
        to_email=data.get("to_address") or data.get("to", ""),
        subject=data.get("subject", ""),
        body=_strip_html(data.get("email_body") or data.get("body") or data.get("text", "")),
        first_name=lead.get("first_name") or data.get("first_name"),
        last_name=lead.get("last_name") or data.get("last_name"),
        company_name=lead.get("company_name") or lead.get("company") or data.get("company_name"),
        website=lead.get("website") or lead.get("company_url") or data.get("website"),
        campaign_id=str(data.get("campaign_id") or ""),
        campaign_name=data.get("campaign_name"),
    )


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
        events = ["ALL_EMAIL_REPLIES"]

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
