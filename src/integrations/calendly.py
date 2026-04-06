"""
Calendly webhook integration.

Handles invitee.created (call booked) and invitee.canceled events.
Verifies webhook signatures using CALENDLY_WEBHOOK_SECRET.
"""

import hashlib
import hmac
import logging
import os
from datetime import datetime, timezone

log = logging.getLogger(__name__)


def verify_calendly_signature(body_bytes: bytes, signature_header: str) -> bool:
    """
    Verify Calendly webhook signature.
    Header format: "t=<epoch_ms>,v1=<hmac_sha256_hex>"
    """
    secret = os.getenv("CALENDLY_WEBHOOK_SECRET", "")
    if not secret:
        log.warning("CALENDLY_WEBHOOK_SECRET not set — skipping signature check")
        return True

    try:
        parts = dict(part.split("=", 1) for part in signature_header.split(","))
        timestamp = parts.get("t", "")
        signature = parts.get("v1", "")

        # Reject if timestamp is more than 5 minutes old
        ts = int(timestamp)
        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        if abs(now - ts) > 300_000:
            log.warning("Calendly webhook timestamp too old")
            return False

        signed_payload = f"{timestamp}.{body_bytes.decode()}"
        expected = hmac.new(
            secret.encode(),
            signed_payload.encode(),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature)
    except Exception as e:
        log.exception(f"Calendly signature verification error: {e}")
        return False


def parse_calendly_event(payload: dict) -> dict | None:
    """
    Parse a Calendly v2 webhook payload into a normalised dict.
    Returns None if the event type is not handled.
    """
    event_type = payload.get("event")
    if event_type not in ("invitee.created", "invitee.canceled"):
        log.info(f"Ignoring Calendly event type: {event_type}")
        return None

    p = payload.get("payload", {})
    invitee = p.get("invitee", {})
    scheduled_event = p.get("scheduled_event", {})

    name = invitee.get("name", "")
    email = invitee.get("email", "")
    cancel_reason = invitee.get("cancel_reason", "") if event_type == "invitee.canceled" else ""

    # Parse start time from ISO 8601
    start_time_raw = scheduled_event.get("start_time", "")
    call_date = ""
    call_time = ""
    if start_time_raw:
        try:
            dt = datetime.fromisoformat(start_time_raw.replace("Z", "+00:00"))
            call_date = dt.strftime("%m/%d/%Y")
            call_time = dt.strftime("%I:%M %p UTC")
        except Exception:
            call_date = start_time_raw[:10]

    # Extract questions and answers for company name
    company = ""
    questions = p.get("questions_and_answers", [])
    for qa in questions:
        q = qa.get("question", "").lower()
        if any(kw in q for kw in ("company", "business", "organisation", "organization")):
            company = qa.get("answer", "")
            break

    # Event URI can be used to match on cancellation
    event_uri = scheduled_event.get("uri", "")

    return {
        "event_type": event_type,
        "name": name,
        "email": email.strip().lower(),
        "company": company,
        "call_date": call_date,
        "call_time": call_time,
        "event_uri": event_uri,
        "cancel_reason": cancel_reason,
    }
