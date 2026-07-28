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
    "email body only — no subject line, no markdown, no signature block.\n\n"
    "PARAGRAPH STRUCTURE (required):\n"
    "The body MUST render as separate paragraphs. Separate each paragraph "
    "with a blank line (two consecutive newlines: `\\n\\n`). A wall-of-text "
    "reply is a bug. Aim for 3 short paragraphs by default:\n"
    "  ¶1: opener\n"
    "  ¶2: the substance (offer refresher + thread observation)\n"
    "  ¶3: single-line CTA / soft question\n"
    "Never merge these into one block. Even a 1-line CTA gets its own "
    "paragraph.",
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
    handoff_from_persona: str = "",
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

    if handoff_from_persona:
        # New thread from a live mailbox because the original persona's
        # mailbox no longer exists in PlusVibe. Must introduce as a colleague
        # taking over — never pretend to be the original sender.
        #
        # Deliverability constraint (2026-07-28): this email is coming from
        # a mailbox the prospect has NEVER received from. ANY link in this
        # first message hurts deliverability. Zero links — not the Calendly,
        # not the Gamma case-studies deck, nothing. The CTA is a soft
        # permission-to-send-info ask, and the actual case studies + calendar
        # link only get sent on the FOLLOWING message once they've engaged.
        intro_hint = (
            f"THIS IS A NEW THREAD FROM A DIFFERENT PERSONA. The original "
            f"sender ({handoff_from_persona}) has moved off this account. "
            f"You are a colleague picking it back up.\n\n"
            f"REQUIRED CONTENT — every element must appear:\n"
            f"  (a) HAND-OFF OPENER — 'Hey [name], picking this up from "
            f"{handoff_from_persona} who reached out about email and SMS "
            f"for [brand] a while back' (adapt the wording, keep it natural).\n"
            f"  (b) OFFER REFRESHER — restate the specific offer we made "
            f"them. Look CAREFULLY at the thread history for the exact "
            f"revenue number, time window, and mechanic we quoted (e.g. "
            f"'$25k/week in 14 days from email and SMS, or you're not "
            f"invoiced'). If a specific number appears in the thread, USE "
            f"IT VERBATIM. If not, describe the mechanic without inventing "
            f"a number: 'we guarantee new revenue attributed to email and "
            f"SMS inside 2 weeks, or you're not invoiced'. Never phrase as "
            f"a refund — 'not invoiced' is the right wording.\n"
            f"  (c) ONE CASE STUDY — name ONE Trendfeed client from the "
            f"canonical facts and give their concrete result verbatim as "
            f"social proof. Pick the case study whose vertical/model most "
            f"resembles the prospect's brand where possible; otherwise pick "
            f"the biggest transformation (Happy Head 122K→436K, Dream Frames "
            f"322% store lift, Frownies $202K in 60d, Oxygenetix 19.65%→"
            f"44.51%, Dermazen +$33K in 30d). Use only figures from the "
            f"canonical facts — never invent numbers. Keep the case study "
            f"mention to ONE line — this isn't a pitch.\n"
            f"  (d) THREAD REFERENCE — briefly reference the specific "
            f"concern or question the prospect left open (e.g. 'you'd asked "
            f"which brands we do this for', 'you'd confirmed the call but "
            f"it may not have gone ahead'). Skip this element only if the "
            f"thread history genuinely has no clear re-entry point.\n"
            f"  (e) PERMISSION-TO-SEND ASK — end with a soft permission "
            f"ask, not a hard 'book a call' push. The prospect has never "
            f"received from this mailbox before; asking permission to send "
            f"info builds trust and dodges the deliverability hit of a link "
            f"in a cold-domain first message. Example patterns to adapt "
            f"(do not paste verbatim):\n"
            f"     'OK if I send over a few more case studies and a time "
            f"to grab?'\n"
            f"     'Want me to send more info + a fresh calendar link?'\n"
            f"     'Happy to send over the details if it's still on your "
            f"radar — OK?'\n\n"
            f"PARAGRAPH LAYOUT — 3 short paragraphs by default, blank line "
            f"between each:\n"
            f"  ¶1: opener (element a)\n"
            f"  ¶2: offer refresher + case study proof (elements b + c)\n"
            f"  ¶3: thread reference + permission ask (elements d + e)\n\n"
            f"HARD DELIVERABILITY RULE: Zero URLs, zero calendar links, zero "
            f"deck links. Not even a bare domain. If your draft contains "
            f"'http', 'https', 'calendly', 'gamma', '.co/', or any other URL "
            f"fragment, rewrite it. This mailbox has no prior reputation "
            f"with the prospect's domain.\n\n"
            f"HARD FRAMING RULES: Do not pretend to be the original sender. "
            f"Do not say 'as I mentioned' — you didn't. Do not apologise "
            f"for the delay. Keep total length tight — 3 short paragraphs, "
            f"not more.\n\n"
            f"ORIGINAL PERSONA NAME: The system's best guess for the "
            f"previous sender's first name is '{handoff_from_persona}'. If "
            f"that looks like a real first name (Crystal, Mia, Jack), use "
            f"it. If it looks off — initials like 'Ml', a run-on like "
            f"'Owenalderton', or something clearly not a first name — "
            f"SCAN THE THREAD HISTORY for the sender's real sign-off name "
            f"and use that. If you can't find a real name anywhere, use "
            f"'my colleague' rather than '{handoff_from_persona}'."
        )
    elif followup_index == 1:
        intro_hint = (
            "This is the FIRST reactivation touch after a period of silence. "
            "Do not say 'circling back' — write like a busy operator with a "
            "specific reason to re-open the thread. Reference the last question "
            "or concern from the thread."
        )
    else:
        intro_hint = (
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
