"""
Reply drafter — writes the actual email body for a classified intent.

Only invoked when disposition == "draft". The drafter does NOT decide
whether to reply; that's the classifier + intel-block resolution upstream.

Move-1 vs move-2 timing (section 8) is decided by the drafter from the
thread history: the count of prior outbound messages from us on this thread
determines which move to use for intents that have both.

Intent 18 gets {company} interpolated; intent 26 gets {month} left as-is
for a human to fill (park intents).
"""
import logging
import os
from typing import Optional

import anthropic

from data.reply_agent_spec import (
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


def _format_intel(intel: Optional[dict]) -> str:
    """Render the intel block for the drafter's user prompt. Intel is
    reference data only — the drafter must never quote from it."""
    if not intel:
        return "(no Trendtrack resolution available — do not invent brand-specific detail)"

    lines = []
    lines.append(f"- Brand: {intel.get('name') or intel.get('domain') or 'unknown'}")
    lines.append(f"- Domain: {intel.get('domain') or 'unknown'}")
    lines.append(f"- Resolution: {intel.get('confidence_label') or 'unconfirmed'}")
    lines.append(f"- Monthly visits (Trendtrack): {intel.get('monthly_visits') or 0}")
    lines.append(f"- Meta ads active: {intel.get('active_ads') or 0}")
    geo = intel.get("geography") or {}
    lines.append(
        f"- Top country: {geo.get('label') or 'unknown'} "
        f"(verdict={geo.get('verdict') or 'unknown'})"
    )
    return "\n".join(lines)


def _count_prior_outbound(thread: list[dict], our_sending_domain: str = "") -> int:
    """
    Count how many messages in the thread came from our side. Used to
    decide move-1 vs move-2 for intents like 4, 5, 8, 16, 17.

    Thread entries look like:
      {"from": "prospect@brand.com" | "sender@ourdomain.help", ...}
    """
    if not thread:
        return 0
    our_domain = (our_sending_domain or "").lower()
    n = 0
    for msg in thread:
        sender = (msg.get("from") or "").lower()
        if not sender:
            continue
        if our_domain and our_domain in sender:
            n += 1
        elif sender.endswith(".help") or "trendfeed" in sender:
            n += 1
    return n


async def draft_reply(
    classification: dict,
    prospect_body: str,
    first_name: str,
    company_name: str,
    intel: Optional[dict] = None,
    thread: Optional[list[dict]] = None,
    our_sending_domain: str = "",
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
    prior_outbound = _count_prior_outbound(thread or [], our_sending_domain)
    move = 2 if prior_outbound >= 1 else 1

    intel_block = _format_intel(intel)

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

Intel block (REFERENCE ONLY — never quote any of this back to the prospect):
{intel_block}

Write the email body. Follow the playbook literally when it gives you exact copy — you may lightly adapt one or two words for flow if the prospect's wording demands it, but do not add sentences the playbook doesn't have and do not remove ones it does. Follow the VOICE and NON-NEGOTIABLE rules from the system prompt. Output the body only — no subject line, no markdown, no signature block beyond an optional first-name sign-off."""

    response = await _get_client().messages.create(
        model=MODEL,
        max_tokens=500,
        system=drafter_system_prompt(),
        messages=[{"role": "user", "content": user_prompt}],
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
