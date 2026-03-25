import json
import os
from pathlib import Path

import anthropic


_client: anthropic.AsyncAnthropic | None = None
_templates: dict | None = None

CALENDLY = "https://calendly.com/trendfeed-media/free-email-marketing"
AGENCY = "Trendfeed"
WEBSITE = "https://trendfeed.co.uk"
CONTACT_EMAIL = "Abdul.Ibrahim@trendfeed.co.uk"


def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        _client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    return _client


def _load_templates() -> dict:
    global _templates
    if _templates is None:
        path = Path(__file__).parent.parent / "data" / "templates.json"
        _templates = json.loads(path.read_text())
    return _templates


async def draft_response(
    reply_type: str,
    first_name: str,
    last_name: str,
    company_name: str,
    original_message: str,
    category: str = "other",
    client_references: list[str] | None = None,
    few_shot_examples: list[dict] | None = None,
) -> str:
    """
    Draft a reply to a cold email prospect using Claude.
    Returns the plain-text draft.
    """
    templates = _load_templates()
    type_meta = templates["reply_types"].get(reply_type, templates["reply_types"]["uncategorised"])

    # Build client reference line
    if client_references:
        if len(client_references) == 1:
            ref_line = f"We've worked with brands like {client_references[0]} in a similar space."
        else:
            names = ", ".join(client_references[:-1]) + f" and {client_references[-1]}"
            ref_line = f"We've worked with brands like {names} — all in the {category} space."
    else:
        ref_line = "We've worked with 40+ brands across a dozen industries."

    # Build few-shot block
    few_shot_block = ""
    if few_shot_examples:
        examples = "\n\n".join(
            f"Example reply type [{ex['reply_type']}]:\nProspect said: {ex['prospect_message']}\nWe sent: {ex['sent_response']}"
            for ex in few_shot_examples[:5]
        )
        few_shot_block = f"\n\nHere are recent approved responses for reference (match this tone and style):\n{examples}"

    system_prompt = f"""You write cold email replies for {AGENCY}, a boutique retention email marketing agency.

IDENTITY — NEVER BREAK THESE:
- You are {AGENCY} only. Never reference any other agency, person, or brand.
- Calendly: {CALENDLY}
- Sign off: "Trendfeed Team"

VOICE — study this carefully:
- Sound like a confident friend who happens to run a successful agency, not a salesperson
- Short sentences. Short paragraphs. One idea per paragraph.
- Never use corporate language: no "leverage", "synergy", "solutions", "reach out", "touch base"
- Never be pushy or desperate. You have 40+ clients. You're selective.
- Warm but brief. Get to the point fast.
- Max 1 exclamation mark in the whole email. Zero is fine too.

STRUCTURE (follow this):
1. Acknowledge what they said in one sentence (show you actually read their message)
2. Address their question/concern directly and honestly — 2–3 sentences max
3. One specific reason to take the next step (not generic hype)
4. Close with the Calendly link, framed as low-pressure

FORMATTING:
- Perfect grammar and spelling. Proofread before outputting.
- Never use em dashes (—), en dashes (–), or double hyphens (--). Use commas, full stops, or semicolons instead.
- Never use ellipsis (...) either.
- Write clean, simple punctuation only: commas, full stops, question marks, semicolons.

HARD RULES:
- 100 to 130 words maximum. Count carefully.
- Always use their first name in the opening line
- Always mention {company_name} at least once
- End every email with: {CALENDLY}
- Never fabricate specific numbers, percentages, or client results
- Never mention competitors"""

    user_prompt = f"""Write a reply to {first_name} at {company_name}.

What they said:
\"\"\"{original_message[:1500]}\"\"\"

How to handle this ({reply_type}):
{type_meta['template_hint']}

Client references (only use if relevant):
{ref_line}{few_shot_block}

Output the email body only. Plain text. No subject line. No markdown."""

    response = await _get_client().messages.create(
        model="claude-sonnet-4-6",
        max_tokens=400,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )

    return response.content[0].text.strip()


def compute_diff(original: str, edited: str) -> str:
    """Return a simple word-level diff summary."""
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
