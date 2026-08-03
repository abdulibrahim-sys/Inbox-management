"""
Reply classifier — maps an inbound prospect reply to exactly one of the 32
intents in data/reply_agent_spec.INTENT_LIBRARY.

Returns:
  {
    "intent_n": int,        # 1..32, or 0 if we can't decide (→ escalate)
    "intent_name": str,
    "disposition": str,     # draft | escalate | disregard | park | stop | suppress | conditional
    "confidence": "high|medium|low",
    "reasoning": str,
    "escalation_reason": str,  # populated when the classifier decides to escalate
  }

Intent 24 has disposition="conditional" — the caller must resolve to
'draft' | 'disregard' | 'escalate' using the intel block per section 9.
"""
import json
import logging
import os

import anthropic

from data.reply_agent_spec import (
    ESCALATION_TRIGGERS,
    get_intent,
    intent_list_for_classifier,
)

log = logging.getLogger(__name__)

_client: anthropic.AsyncAnthropic | None = None
MODEL = "claude-sonnet-4-6"


def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        _client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    return _client


CLASSIFIER_SYSTEM = f"""You classify inbound cold-email replies for Trendfeed. Read the prospect's reply (plus optional thread history) and choose EXACTLY ONE intent from the library.

Intent library:
{intent_list_for_classifier()}

Rules:
- Pick the single best-fit intent. If the reply mixes 3 or more genuinely distinct intents, output intent_n=0 with escalation_reason="mixed intents".
- If you can't confidently place the reply into one of these 32 intents, output intent_n=0 with escalation_reason describing why.
- Some intents look similar — distinguish them carefully:
    * 4 (wants email, not a call) vs 16 (send me a proposal / strategy first). "Just answer me here" is 4. "Send me a plan / audit / teardown / strategy doc" is 16.
    * 5 (how does the guarantee work) vs 6 (what does 'you don't pay' mean) vs 11 (sounds too good to be true). 5 is a first-time factual question. 6 is a follow-up challenge specifically on the wording. 11 is broad skepticism.
    * 10 (case studies / proof / references, incl. live reference call requests) vs 11 (sounds too good to be true). 10 is "show me evidence" — including when the prospect specifically wants to speak 1-on-1 with an existing client BEFORE booking with us; that maps to intent 10 (VARIANT B in the playbook), not escalate. 11 is doubt about the offer itself with no specific proof ask.
    * 17 (already have an agency / already have flows) vs 27 (not interested with no question). 17 leaves room for a conversation. 27 is a hard no.
    * 24 (what ads did you see) is very specific — they are challenging the outreach hook that mentioned their ads.
    * 30 (only reply is 'we don't run ads') is different from 24 — 30 is a flat statement, no question about which ad.
    * 32 (unsubscribe / hostile / legal) is the only suppressing intent. "Not interested" alone is 27, not 32.

Also apply these hard escalation triggers on top of intent choice — if any of these are true, call the tool with intent_n=0 and describe which trigger fired in escalation_reason:
{ESCALATION_TRIGGERS}

You MUST call the classify_reply tool exactly once with the four fields. Do not respond with prose."""


# Anthropic tool-use schema — forces the model to return structured data.
# We used to ask for JSON in the system prompt and parse the text response.
# When the classifier hit an ambiguous case ("mixed intents"), the model
# sometimes returned plain-text prose ("The reply mixes several distinct
# intents...") and the parse failed. Tool use makes the JSON output
# structural, so that whole failure mode is gone.
_CLASSIFY_TOOL = {
    "name": "classify_reply",
    "description": (
        "Record the classification of a prospect reply. Must be called "
        "exactly once per invocation."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "intent_n": {
                "type": "integer",
                "minimum": 0,
                "maximum": 32,
                "description": "Intent number 1-32 from the library, or 0 for escalate/unknown.",
            },
            "confidence": {
                "type": "string",
                "enum": ["high", "medium", "low"],
                "description": "How confident you are in the classification.",
            },
            "reasoning": {
                "type": "string",
                "description": "One-sentence explanation.",
            },
            "escalation_reason": {
                "type": "string",
                "description": (
                    "Populated when intent_n=0. Which hard trigger fired "
                    "OR why the reply couldn't be placed in one of the 32 "
                    "intents (e.g. 'mixed intents', 'reference call request', "
                    "'qualification criteria disclosure'). Empty otherwise."
                ),
            },
        },
        "required": ["intent_n", "confidence", "reasoning"],
    },
}


async def classify_reply(body: str, subject: str = "", thread_context: str = "") -> dict:
    """
    Classify a prospect reply. `thread_context` is optional — a compact
    summary of prior exchanges on the same thread, used to distinguish
    move-1 vs move-2 style intents.

    Uses Anthropic tool use to force structured output, so free-form prose
    responses (which used to cause parse errors on ambiguous cases) can't
    happen. If the model still fails to call the tool, we escalate.
    """
    user_prompt = (
        f"Subject: {subject or '(none)'}\n\n"
        f"Reply body:\n\"\"\"\n{body[:3000]}\n\"\"\""
    )
    if thread_context:
        user_prompt += (
            f"\n\nPrior thread (older messages first):\n\"\"\"\n"
            f"{thread_context[:2000]}\n\"\"\""
        )

    try:
        response = await _get_client().messages.create(
            model=MODEL,
            max_tokens=400,
            system=CLASSIFIER_SYSTEM,
            tools=[_CLASSIFY_TOOL],
            tool_choice={"type": "tool", "name": "classify_reply"},
            messages=[{"role": "user", "content": user_prompt}],
        )
    except Exception as e:
        log.exception(f"Classifier LLM call failed: {e}")
        return _escalation_result("classifier error")

    # Extract the tool_use block — with tool_choice forced, this is
    # guaranteed to exist unless the API totally failed.
    result: dict | None = None
    for block in response.content:
        if getattr(block, "type", None) == "tool_use" and block.name == "classify_reply":
            result = block.input or {}
            break

    if not result:
        log.warning("Classifier did not call the tool; escalating.")
        return _escalation_result("classifier did not return structured output")

    intent_n = int(result.get("intent_n") or 0)
    intent = get_intent(intent_n)
    return {
        "intent_n": intent["n"],
        "intent_name": intent["name"],
        "disposition": intent["disposition"],
        "confidence": result.get("confidence") or "low",
        "reasoning": result.get("reasoning") or "",
        "escalation_reason": result.get("escalation_reason") or "",
    }


def _escalation_result(reason: str) -> dict:
    intent = get_intent(0)
    return {
        "intent_n": 0,
        "intent_name": intent["name"],
        "disposition": intent["disposition"],
        "confidence": "low",
        "reasoning": reason,
        "escalation_reason": reason,
    }


def resolve_intent_24(classification: dict) -> dict:
    """
    Section 9 intent 24 ("what ads did you see") now always escalates.

    Rationale: we no longer pull ad data. Answering with any specific ad
    would be inventing detail, and answering with 'your recent Meta ads'
    when the outreach hook cited something specific reads as evasive.
    A human decides.
    """
    if classification.get("intent_n") != 24:
        return classification
    out = dict(classification)
    out["disposition"] = "escalate"
    out["escalation_reason"] = (
        "Prospect asked which ads we saw. We no longer pull ad data, so "
        "any auto-reply would either invent an ad or concede the outreach "
        "premise. A human decides how to respond."
    )
    return out


# Countries hard-excluded via TLD heuristic. This replaces the previous
# Trendtrack-based top-country check. A .in / .pk email domain is the
# strongest cheap signal that a brand is based in an excluded market.
_EXCLUDED_TLDS = {"in", "pk"}


def check_tld_geography(prospect_email: str) -> str:
    """
    Return 'not_allowed' | 'unknown'. Never 'allowed' — we don't have
    enough signal from a TLD to affirmatively confirm a brand is in a
    supported country. .in / .pk are the only hard stops.
    """
    if not prospect_email or "@" not in prospect_email:
        return "unknown"
    domain = prospect_email.rsplit("@", 1)[-1].lower()
    # Trim to the last label (e.g. "abc.co.in" → "in", "abc.pk" → "pk").
    tld = domain.rsplit(".", 1)[-1] if "." in domain else ""
    if tld in _EXCLUDED_TLDS:
        return "not_allowed"
    return "unknown"


def promote_geo_stop(classification: dict, geography_verdict: str) -> dict:
    """
    Apply the geography gate on top of intent classification. A 'not_allowed'
    verdict overrides any intent → intent 31 (stop, no reply).
    """
    if geography_verdict == "not_allowed":
        stop_intent = get_intent(31)
        return {
            **classification,
            "intent_n": stop_intent["n"],
            "intent_name": stop_intent["name"],
            "disposition": stop_intent["disposition"],
            "reasoning": (
                (classification.get("reasoning") or "")
                + " | Geography gate: email TLD is in excluded set (.in / .pk)."
            ).strip(" |"),
        }
    return classification
