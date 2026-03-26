import json
import os
from pathlib import Path

import anthropic


_client: anthropic.AsyncAnthropic | None = None
_templates: dict | None = None


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


async def classify_reply(body: str, subject: str = "") -> dict:
    """
    Classify a prospect reply for the AI Search Visibility campaign.
    Returns {"reply_type": str, "confidence": str, "reasoning": str}
    """
    templates = _load_templates()
    reply_types_list = "\n".join(
        f"- {rt}: {data['description']}"
        for rt, data in templates["reply_types"].items()
    )

    prompt = f"""You are classifying a cold email reply for Featured in AI, a specialist agency that gets e-commerce brands recommended by AI search engines (Google AI Overviews, ChatGPT, Gemini, Perplexity, Copilot, Claude).

Classify the reply into exactly ONE of these reply types:
{reply_types_list}

Subject: {subject}
Reply body:
\"\"\"
{body[:2000]}
\"\"\"

Respond with JSON only, no explanation:
{{
  "reply_type": "<type>",
  "confidence": "high|medium|low",
  "reasoning": "<one sentence>"
}}"""

    response = await _get_client().messages.create(
        model="claude-sonnet-4-6",
        max_tokens=200,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        result = {"reply_type": "uncategorised", "confidence": "low", "reasoning": "Parse error"}

    if result.get("reply_type") not in templates["reply_types"]:
        result["reply_type"] = "uncategorised"

    return result


def get_reply_type_meta(reply_type: str) -> dict:
    """Return the metadata for a given reply type."""
    templates = _load_templates()
    return templates["reply_types"].get(reply_type, templates["reply_types"]["uncategorised"])
