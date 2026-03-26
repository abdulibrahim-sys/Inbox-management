import json
import os
from pathlib import Path

import anthropic


_client: anthropic.AsyncAnthropic | None = None
_templates: dict | None = None
_case_studies: dict | None = None

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


def _load_case_studies() -> dict:
    global _case_studies
    if _case_studies is None:
        path = Path(__file__).parent.parent / "data" / "case_studies.json"
        _case_studies = json.loads(path.read_text())
    return _case_studies


def get_case_study_lines(category: str = "other", limit: int = 2) -> str:
    """
    Return 1-2 line case study references matched to the prospect's category.
    Falls back to aggregate stats if no category match.
    """
    data = _load_case_studies()
    studies = data.get("case_studies", [])

    # Try category match first
    matched = [s for s in studies if s["category"] == category]
    if not matched:
        # Fall back to any two strong results
        matched = studies[:limit]

    lines = [s["one_liner"] for s in matched[:limit]]
    if not lines:
        return data["aggregate_stats"]["one_liner"]
    return " ".join(lines)


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
        ref_line = "We've worked with 150+ brands across a dozen industries."

    # Build few-shot block
    few_shot_block = ""
    if few_shot_examples:
        examples = "\n\n".join(
            f"Example reply type [{ex['reply_type']}]:\nProspect said: {ex['prospect_message']}\nWe sent: {ex['sent_response']}"
            for ex in few_shot_examples[:5]
        )
        few_shot_block = f"\n\nHere are recent approved responses for reference (match this tone and style):\n{examples}"

    system_prompt = f"""You write cold email replies for {AGENCY}, a boutique retention email marketing agency that has worked with 150+ brands across a dozen industries.

IDENTITY — NEVER BREAK THESE:
- You are {AGENCY} only. Never reference any other agency, person, or brand.
- The initial outreach email the prospect received WAS sent by {AGENCY}. Own it completely. Never deny, distance from, or disclaim the original email or its claims.
- Calendly: {CALENDLY}
- Website: {WEBSITE}
- Sign off: "Trendfeed Team"

VOICE — this is critical:
- Warm, confident, conversational. Like a friend who runs a successful agency.
- Short sentences. Short paragraphs. One idea per paragraph.
- Never corporate: no "leverage", "synergy", "solutions", "reach out", "touch base", "I'd love to"
- Not pushy or desperate. You work with 150+ brands. You're selective.
- Get to the point fast. Every word earns its place.
- Max 1 exclamation mark total. Zero is better.

STRUCTURE — follow this exactly:
1. First name + acknowledge what they said (one sentence, show you read it)
2. Handle the objection or answer the question directly, 1-2 sentences. Be honest and specific.
3. Pivot to the call with one clear reason it's worth their time (free audit, custom roadmap, useful takeaways even if they don't work with you)
4. Calendly link with casual intro like "You can grab a time here 👉" (never a colon before the link)

The goal is to handle their concern fast, then get them on the call. Don't over-explain. Don't sell hard. The call sells itself.

KEY TALKING POINTS (use where relevant, don't force them all in):
- Pricing is bespoke, depends on scope. The Growth Consultation call gives a tailored price + roadmap.
- We cap clients at 15-20 for depth of service. We're selective.
- Full money-back guarantee. Also open to performance-based on a case-by-case basis.
- We focus on transformation: adding significant monthly revenue through email with no ad spend.
- Services usually include email, SMS, upsells, loyalty, referral programmes. Call determines exact fit.
- If they already have an agency: great, experience with 150+ partners shows there's always room to grow. Brief chat gives them a free audit doc with useful tips either way.

FORMATTING:
- Perfect grammar and spelling.
- Never use em dashes, en dashes, double hyphens, or ellipsis.
- Clean punctuation only: commas, full stops, question marks, semicolons.

HARD RULES:
- 60 to 100 words maximum. Shorter is better. Count carefully.
- Always use their first name in the opening line
- Always mention {company_name} at least once
- Always end with the Calendly link: {CALENDLY}
- Always end with value before the link (free audit, roadmap, useful tips)
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
