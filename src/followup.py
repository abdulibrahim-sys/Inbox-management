"""
Follow-up system for leads that don't book a call after our initial reply.

Schedule: 24 hours → 3 days → 5 days
Cancel if: lead's PlusVibe label becomes "Meeting booked" (case-insensitive)
Action on approve: save as draft in PlusVibe (manual send)
"""

import json
import logging
import os
import time
from typing import Optional

import anthropic
from upstash_redis import Redis

from src.integrations.plusvibe import (
    get_lead_status,
    get_email_thread,
    save_draft,
    _strip_html,
)

log = logging.getLogger(__name__)

_redis: Redis | None = None
_llm: anthropic.AsyncAnthropic | None = None

# Follow-up intervals in seconds
FOLLOWUP_SCHEDULE = {
    1: 24 * 60 * 60,      # 24 hours
    2: 3 * 24 * 60 * 60,  # 3 days
    3: 5 * 24 * 60 * 60,  # 5 days
}

CALENDLY = "https://calendly.com/trendfeed-media/free-email-marketing"
MEETING_BOOKED_LABELS = {"meeting booked", "meeting_booked", "meetingbooked"}


def _get_redis() -> Redis:
    global _redis
    if _redis is None:
        _redis = Redis(
            url=os.getenv("UPSTASH_REDIS_REST_URL"),
            token=os.getenv("UPSTASH_REDIS_REST_TOKEN"),
        )
    return _redis


def _get_llm() -> anthropic.AsyncAnthropic:
    global _llm
    if _llm is None:
        _llm = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    return _llm


# ── Schedule / Cancel ─────────────────────────────────────────────────────────

def schedule_followups(
    record_id: str,
    lead_email: str,
    lead_first_name: str,
    lead_last_name: str,
    company_name: str,
    from_email: str,
    subject: str,
) -> None:
    """Schedule follow-up checks after sending the initial reply."""
    r = _get_redis()
    now = int(time.time())

    data = {
        "record_id": record_id,
        "lead_email": lead_email,
        "first_name": lead_first_name,
        "last_name": lead_last_name,
        "company_name": company_name,
        "from_email": from_email,
        "subject": subject,
        "initial_reply_at": now,
        "next_stage": 1,
        "cancelled": False,
    }

    key = f"followup:{lead_email}"
    r.set(key, json.dumps(data), ex=60 * 60 * 24 * 30)  # 30-day TTL
    # Add to the set of active follow-ups for scanning
    r.sadd("followup:active", lead_email)
    log.info(f"Scheduled follow-ups for {lead_email} ({company_name})")


def cancel_followups(lead_email: str) -> None:
    """Cancel the follow-up sequence for a lead."""
    r = _get_redis()
    r.delete(f"followup:{lead_email}")
    r.srem("followup:active", lead_email)
    log.info(f"Cancelled follow-ups for {lead_email}")


# ── Check for due follow-ups ──────────────────────────────────────────────────

async def get_due_followups() -> list[dict]:
    """
    Scan all active follow-ups and return those that are due.
    Also cancels sequences where the lead has booked a meeting.
    """
    r = _get_redis()
    active = r.smembers("followup:active")
    if not active:
        return []

    now = int(time.time())
    due = []

    for lead_email in active:
        raw = r.get(f"followup:{lead_email}")
        if not raw:
            r.srem("followup:active", lead_email)
            continue

        data = json.loads(raw)
        if data.get("cancelled"):
            cancel_followups(lead_email)
            continue

        stage = data.get("next_stage", 1)
        if stage > 3:
            # All follow-ups sent, remove from active
            cancel_followups(lead_email)
            continue

        # Check if lead has booked a meeting
        label = await get_lead_status(lead_email)
        if label and label.lower().replace(" ", "").replace("_", "") in {
            l.replace(" ", "").replace("_", "") for l in MEETING_BOOKED_LABELS
        }:
            log.info(f"Lead {lead_email} booked a meeting, cancelling follow-ups")
            cancel_followups(lead_email)
            continue

        # Check if this stage is due
        initial_time = data["initial_reply_at"]
        due_at = initial_time + FOLLOWUP_SCHEDULE[stage]
        if now >= due_at:
            data["due_stage"] = stage
            due.append(data)

    return due


def advance_stage(lead_email: str) -> None:
    """Move a lead to the next follow-up stage after a follow-up is approved."""
    r = _get_redis()
    raw = r.get(f"followup:{lead_email}")
    if not raw:
        return

    data = json.loads(raw)
    data["next_stage"] = data.get("next_stage", 1) + 1

    if data["next_stage"] > 3:
        cancel_followups(lead_email)
    else:
        r.set(f"followup:{lead_email}", json.dumps(data), ex=60 * 60 * 24 * 30)


# ── Draft follow-up ──────────────────────────────────────────────────────────

async def draft_followup(
    stage: int,
    first_name: str,
    company_name: str,
    thread_context: str,
) -> str:
    """
    Draft a follow-up email using Alex Hormozi's cold email follow-up principles.
    Stage 1 = 24h, Stage 2 = 3 days, Stage 3 = 5 days (breakup).
    """
    stage_guidance = {
        1: (
            "STAGE 1 (24-hour follow-up): Light, short nudge. "
            "Reference the previous conversation briefly. "
            "Add one small piece of value (a quick insight about their industry or a relevant stat). "
            "Ask if they had a chance to look at the booking link. "
            "Keep it under 80 words. Very casual."
        ),
        2: (
            "STAGE 2 (3-day follow-up): Lead with value. "
            "Share a brief, compelling case study or result from a similar brand (use Trendfeed's real clients if relevant). "
            "Frame the call as a free audit that gives them useful takeaways whether they work with you or not. "
            "Keep it under 100 words."
        ),
        3: (
            "STAGE 3 (5-day breakup email): Final follow-up. "
            "Be honest and direct. Say you don't want to clog their inbox. "
            "Leave the door open with one final reason to connect. "
            "Mention this is the last email. "
            "Keep it under 80 words. No pressure."
        ),
    }

    system_prompt = f"""You write follow-up cold emails for Trendfeed, a boutique retention email marketing agency.

IDENTITY:
- You are Trendfeed only. Never reference any other agency.
- Calendly: {CALENDLY}
- Sign off: "Trendfeed Team"

ALEX HORMOZI FOLLOW-UP PRINCIPLES:
- Lead with value, not asks. Give before you request.
- Be specific, not generic. Reference their business directly.
- Short emails win. Every word must earn its place.
- Pattern interrupt. Don't sound like every other follow-up.
- Show you understand their world. Prove you did your homework.
- No guilt trips. No "just checking in." No "circling back."

FORMATTING:
- Perfect grammar and spelling.
- Never use em dashes, en dashes, double hyphens, or ellipsis.
- Clean punctuation only: commas, full stops, question marks, semicolons.
- Max 1 exclamation mark. Zero is better.

HARD RULES:
- Use their first name in the opening
- Mention their company at least once
- End with the Calendly link: {CALENDLY}
- Never fabricate specific numbers or client results"""

    user_prompt = f"""Write a follow-up email to {first_name} at {company_name}.

{stage_guidance.get(stage, stage_guidance[1])}

Here is the email thread so far (most recent first):
\"\"\"
{thread_context[:3000]}
\"\"\"

Output the email body only. Plain text. No subject line. No markdown."""

    response = await _get_llm().messages.create(
        model="claude-sonnet-4-6",
        max_tokens=300,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )

    return response.content[0].text.strip()


def format_thread_context(emails: list[dict]) -> str:
    """Format a list of email records into readable thread context."""
    if not emails:
        return "(No thread history available)"

    parts = []
    for e in emails[:10]:  # Limit to last 10 messages
        sender = e.get("from", "Unknown")
        body = e.get("body", "")[:500]
        ts = e.get("timestamp", "")
        parts.append(f"[{ts}] From: {sender}\n{body}")

    return "\n\n---\n\n".join(parts)
