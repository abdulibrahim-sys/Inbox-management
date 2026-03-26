import json
import os
from pathlib import Path

import anthropic


_client: anthropic.AsyncAnthropic | None = None
_templates: dict | None = None
_case_studies: dict | None = None

CALENDLY = "https://calendly.com/trendfeed-media/aeo-visibility"
AGENCY = "Featured in AI"
WEBSITE = "https://featuredinai.com"


def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        _client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    return _client


def _load_templates() -> dict:
    global _templates
    if _templates is None:
        path = Path(__file__).parent.parent / "data" / "ai_visibility_templates.json"
        _templates = json.loads(path.read_text())
    return _templates


def _load_case_studies() -> dict:
    global _case_studies
    if _case_studies is None:
        path = Path(__file__).parent.parent / "data" / "ai_visibility_case_studies.json"
        _case_studies = json.loads(path.read_text())
    return _case_studies


def get_case_study_lines(limit: int = 1) -> str:
    """Return case study reference lines for follow-ups."""
    data = _load_case_studies()
    studies = data.get("case_studies", [])
    lines = [s["one_liner"] for s in studies[:limit]]
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
    few_shot_examples: list[dict] | None = None,
) -> str:
    """
    Draft a reply to a prospect for the AI Search Visibility campaign.
    Returns the plain-text draft.
    """
    templates = _load_templates()
    type_meta = templates["reply_types"].get(reply_type, templates["reply_types"]["uncategorised"])

    # Build few-shot block
    few_shot_block = ""
    if few_shot_examples:
        examples = "\n\n".join(
            f"Example reply type [{ex['reply_type']}]:\nProspect said: {ex['prospect_message']}\nWe sent: {ex['sent_response']}"
            for ex in few_shot_examples[:5]
        )
        few_shot_block = f"\n\nHere are recent approved responses for reference (match this tone and style):\n{examples}"

    system_prompt = f"""You write cold email replies for {AGENCY}, a specialist agency that gets e-commerce brands recommended by AI search engines.

IDENTITY — NEVER BREAK THESE:
- You are {AGENCY} only. Never reference any other agency, person, or brand as yourself.
- The initial outreach email the prospect received WAS sent by {AGENCY}. Own it. Never deny, distance from, or disclaim the original email or its claims.
- Calendly: {CALENDLY}
- Website: {WEBSITE}
- Sign off: "Featured in AI Team"

WHAT WE DO (use this knowledge naturally, don't dump it all):
- We get e-commerce brands recommended by AI search engines: Google AI Overviews, ChatGPT (and ChatGPT Shopping), Gemini, Perplexity, Copilot, and Claude.
- When a shopper asks AI "what's the best X?", AI names specific brands. We make sure your brand is one of them.
- 4-step system: Audit, Strategy, Execute, Scale.
- We monitor 6 AI platforms and track 24 prompt categories.
- AI visibility compounds: winning one prompt makes you more likely to be cited for related ones.
- We also offer a $5.99 AI Audit (brand visibility scan across 6+ platforms, delivered in 48 hours) as a low-commitment entry point.

VOICE — study this carefully:
- Sound like a confident friend who happens to run a specialist agency, not a salesperson
- Short sentences. Short paragraphs. One idea per paragraph.
- Never use corporate language: no "leverage", "synergy", "solutions", "reach out", "touch base"
- Never be pushy or desperate. You're the specialist in a new category. You're selective.
- Warm but brief. Get to the point fast.
- Max 1 exclamation mark in the whole email. Zero is fine too.

STRUCTURE (follow this):
1. Acknowledge what they said in one sentence (show you actually read their message)
2. Address their question/concern directly and honestly, 2-3 sentences max
3. One specific reason to take the next step (not generic hype)
4. Close with the Calendly link using a casual intro like "You can grab a time here 👉" (never use a colon before the link)

FORMATTING:
- Perfect grammar and spelling. Proofread before outputting.
- Never use em dashes, en dashes, or double hyphens. Use commas, full stops, or semicolons instead.
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
{type_meta['template_hint']}{few_shot_block}

Output the email body only. Plain text. No subject line. No markdown."""

    response = await _get_client().messages.create(
        model="claude-sonnet-4-6",
        max_tokens=400,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )

    return response.content[0].text.strip()
