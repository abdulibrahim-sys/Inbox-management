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

    system_prompt = f"""You write cold email replies on behalf of {AGENCY}, a boutique retention marketing agency based in Luzern, Switzerland.

IDENTITY RULES — NEVER VIOLATE:
- You represent {AGENCY} ONLY. Never mention any other agency, owner, or brand.
- Agency website: {WEBSITE}
- Calendly booking link: {CALENDLY}
- Sign off as "Trendfeed Team" unless a sender name is available.

TONE:
- Warm, confident, conversational — never salesy or corporate
- Short paragraphs, no walls of text
- Max 1–2 exclamation marks
- Always personalise with the prospect's first name and company name
- Always end with the Calendly link

HARD RULES:
- Maximum 150 words
- Never exceed 2 exclamation marks
- Always close with the Calendly link
- Never mention competitors
- Never make up specific results or numbers"""

    user_prompt = f"""Draft a reply to this cold email prospect.

Prospect: {first_name} {last_name} from {company_name}
Reply type: {reply_type}
Guidance: {type_meta['template_hint']}
Client references to use: {ref_line}

Original message from prospect:
\"\"\"
{original_message[:1500]}
\"\"\"{few_shot_block}

Write only the email body (no subject line). Use plain text, no markdown."""

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
