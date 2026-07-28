"""
Reply drafter — writes the actual email body for a classified intent.

Only invoked when disposition == "draft". The drafter does NOT decide
whether to reply; that's the classifier upstream.

Move-1 vs move-2 timing (section 8) is decided by the drafter from the
thread history: the count of prior outbound messages from us on this thread
determines which move to use for intents that have both.

Intent 18 gets {company} interpolated; intent 26 gets {month} left as-is
for a human to fill (park intents).

Follow-up drafts (see draft_followup) work off the thread only — no brand
research. All personalisation comes from what the prospect and we said.
"""
import logging
import os
from typing import Optional

import anthropic

from data.reply_agent_spec import (
    CANONICAL_FACTS,
    NON_NEGOTIABLE,
    VOICE_GUIDE,
    drafter_system_prompt,
    get_intent,
)

log = logging.getLogger(__name__)

_client: anthropic.AsyncAnthropic | None = None
MODEL = "claude-sonnet-4-6"


def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        _client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    return _client


def _count_prior_outbound(thread: list[dict], prospect_email: str = "") -> int:
    """
    Count how many messages in the thread came from our side. Used to
    decide move-1 vs move-2 for intents like 4, 5, 8, 16, 17.

    We identify "us" by domain-difference from the prospect. This is more
    robust than matching against a persona list — every persona we spin up
    just works.
    """
    if not thread:
        return 0
    prospect_domain = (
        prospect_email.rsplit("@", 1)[-1].lower() if "@" in prospect_email else ""
    )
    n = 0
    for msg in thread:
        sender = (msg.get("from") or "").lower()
        if not sender or "@" not in sender:
            continue
        sender_domain = sender.rsplit("@", 1)[-1]
        if prospect_domain and sender_domain != prospect_domain:
            n += 1
    return n


async def draft_reply(
    classification: dict,
    prospect_body: str,
    first_name: str,
    company_name: str,
    prospect_email: str = "",
    thread: Optional[list[dict]] = None,
) -> str:
    """
    Draft a reply for a classified intent. Returns the email body only.

    Raises ValueError if disposition isn't 'draft' — caller should route
    non-draft dispositions to the appropriate Slack message instead.
    """
    disposition = classification.get("disposition")
    if disposition != "draft":
        raise ValueError(
            f"draft_reply called with disposition={disposition!r}; "
            f"only 'draft' intents should reach the drafter"
        )

    intent = get_intent(classification.get("intent_n") or 0)
    prior_outbound = _count_prior_outbound(thread or [], prospect_email)
    move = 2 if prior_outbound >= 1 else 1

    user_prompt = f"""INTENT SELECTED (from classifier):
  {intent['n']}. {intent['name']}

INTENT PLAYBOOK (follow this exactly — do not invent alternatives):
\"\"\"
{intent['playbook']}
\"\"\"

MOVE: {move}  (prior_outbound_from_us={prior_outbound})
  If the playbook has a MOVE 1 and MOVE 2 branch, use the MOVE {move} version.
  If the playbook has only one form, use that form regardless of move number.

Prospect first name: {first_name or '(unknown)'}
Prospect company (use for interpolation only where the playbook says {{company}}): {company_name or '(unknown)'}

Prospect's message:
\"\"\"
{prospect_body[:2500]}
\"\"\"

Write the email body. Follow the playbook literally when it gives you exact copy — you may lightly adapt one or two words for flow if the prospect's wording demands it, but do not add sentences the playbook doesn't have and do not remove ones it does. Follow the VOICE and NON-NEGOTIABLE rules from the system prompt. Output the body only — no subject line, no markdown, no signature block beyond an optional first-name sign-off."""

    response = await _get_client().messages.create(
        model=MODEL,
        max_tokens=500,
        system=drafter_system_prompt(),
        messages=[{"role": "user", "content": user_prompt}],
    )
    return response.content[0].text.strip()


# ── Follow-up drafter (thread-only, no brand research) ────────────────────

FOLLOWUP_SYSTEM = "\n\n".join([
    "You draft SHORT reactivation follow-up emails to prospects who went "
    "quiet after Trendfeed's previous reply. Your output is a plain-text "
    "email body only — no subject line, no markdown, no signature block.",
    NON_NEGOTIABLE,
    CANONICAL_FACTS,
    VOICE_GUIDE,
    """FOLLOW-UP RULES (on top of the voice + facts above):

1. 2 to 4 sentences. Shorter is better.
2. Personalise from the THREAD only. Reference something the prospect
   actually said, or something we already said and they haven't responded
   to. NEVER invent a brand-specific observation.
3. If the last thing WE said asked a question they didn't answer, reference
   that question specifically — that's the strongest re-entry point.
4. If the last thing THEY said was a specific concern (pricing, agency,
   guarantee), close the loop on that concern in one clean sentence.
5. Never re-explain the offer they've already been sent. Assume they read it.
6. ONE clear call to action. Either:
   (a) 'Worth 15 min?' + the Calendly link, OR
   (b) A single specific question that opens a reply.
7. No fake urgency. No 'just circling back'. No 'wanted to touch base'.
   No 'bumping this'. No 'following up on my previous email'.
8. If our previous message named a specific number (e.g. $25k/week), DO
   NOT invent a different number now.
9. Do not open with 'Hi <name>' unless the previous thread also did so —
   personas maintain their voice. First name alone is fine.

Output the body only."""
])


def _format_thread(thread: list[dict], limit: int = 6) -> str:
    """Compact newest-first thread rendering for the drafter prompt."""
    if not thread:
        return "(no thread history)"
    lines = []
    for m in thread[:limit]:
        sender = m.get("from") or "?"
        body = (m.get("body") or "")[:400]
        ts = m.get("timestamp") or ""
        lines.append(f"[{ts}] {sender}: {body}")
    return "\n---\n".join(lines)


async def draft_followup(
    thread: list[dict],
    first_name: str,
    company_name: str,
    prospect_email: str = "",
    followup_index: int = 1,
    days_since_our_reply: int = 0,
    calendly_url: str = "https://calendly.com/trendfeed-media/email-marketing-audit",
) -> str:
    """
    Draft a reactivation / cadence follow-up from thread context alone.

    Args:
      thread: newest-first email thread rows (dicts with from/body/timestamp)
      first_name: prospect first name (opener)
      company_name: prospect company (for context; never quote)
      prospect_email: used only to identify our-side messages in the thread
      followup_index: 1 for reactivation, 2/3 for cadence. Shapes the
                      "we've written before" framing.
      days_since_our_reply: for context, not quoted back to the prospect.
      calendly_url: prospect-facing Calendly. Never use the internal one.

    Returns the plain-text email body.
    """
    thread_str = _format_thread(thread)

    intro_hint = (
        "This is the FIRST reactivation touch after a period of silence. "
        "Do not say 'circling back' — write like a busy operator with a "
        "specific reason to re-open the thread. Reference the last question "
        "or concern from the thread."
        if followup_index == 1 else
        f"This is follow-up #{followup_index} in the cadence "
        f"({days_since_our_reply} days after our previous reply). Keep it "
        f"shorter and sharper than the previous message."
    )

    user_text = f"""FOLLOW-UP INDEX: {followup_index}
INTRO CONTEXT: {intro_hint}

PROSPECT
  first_name: {first_name or '(unknown)'}
  company:    {company_name or '(unknown)'}

THREAD HISTORY (newest first — read carefully; the whole follow-up must
personalise from what's here, nothing invented outside it)
\"\"\"
{thread_str}
\"\"\"

Calendly for CTA (if you use it): {calendly_url}

Write the follow-up now. Body only, 2 to 4 sentences, one CTA."""

    response = await _get_client().messages.create(
        model=MODEL,
        max_tokens=400,
        system=FOLLOWUP_SYSTEM,
        messages=[{"role": "user", "content": user_text}],
    )
    return response.content[0].text.strip()


def compute_diff(original: str, edited: str) -> str:
    """Return a short word-count diff summary (used by Slack update text)."""
    orig_words = set(original.lower().split())
    edit_words = set(edited.lower().split())
    added = edit_words - orig_words
    removed = orig_words - edit_words
    parts = []
    if added:
        parts.append(f"+{len(added)} words")
    if removed:
        parts.append(f"-{len(removed)} words")
    return ", ".join(parts) if parts else "no change"
