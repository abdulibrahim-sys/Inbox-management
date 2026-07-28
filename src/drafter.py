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
import base64
import logging
import os
from typing import Optional

import anthropic
import httpx

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


# ── Follow-up drafter ─────────────────────────────────────────────────────────

FOLLOWUP_SYSTEM = "\n\n".join([
    "You draft SHORT reactivation follow-up emails to prospects who went "
    "quiet after Trendfeed's previous reply. Your output is a plain-text "
    "email body only — no subject line, no markdown, no signature block.",
    NON_NEGOTIABLE,
    CANONICAL_FACTS,
    VOICE_GUIDE,
    """FOLLOW-UP RULES (on top of the voice + facts above):

1. 2 to 4 sentences. Shorter is better.
2. Lead with a real observation about the brand. If we have an ad image + ad
   copy, you may reference what's actually in it — the product, the hook,
   the offer, the aesthetic. NEVER embellish; if you're guessing, say
   'your recent Meta ads' instead of naming a product.
3. If we don't have an ad, reference something from the landing page /
   product context you're given.
4. Tie the observation to Trendfeed's service in ONE clause. Two examples:
     'Ads are doing the acquisition work — email keeps those buyers spending.'
     'That kind of promise (fast results) is what email flows are built for
      once the ad brings them in.'
5. Never re-explain the offer they've already been sent. Assume they read it.
6. ONE clear call to action. Either:
   (a) 'Worth 15 min?' + the Calendly link, OR
   (b) A single specific question that opens a reply.
7. No fake urgency. No 'just circling back'. No 'wanted to touch base'.
8. If the previous reply from us mentioned a specific number they were
   promised (e.g. $25k/week), DO NOT invent a different number now.
9. Do not open with 'Hi <name>' unless the previous thread also did so —
   personas maintain their voice. First name alone is fine.

Output the body only."""
])


async def _fetch_image_bytes(url: str) -> Optional[tuple[bytes, str]]:
    """Download an ad image for Claude's vision content block. Downscales
    with Pillow if the raw file exceeds Anthropic's per-image cap.

    Returns (bytes, media_type) or None if unreachable / unprocessable.
    Media type is always "image/jpeg" after downscaling; identical to the
    source suffix when no rescale was needed."""
    if not url:
        return None
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(url)
            r.raise_for_status()
            data = r.content
    except Exception as e:
        log.warning(f"followup: image fetch failed for {url}: {e}")
        return None

    ANTHROPIC_IMAGE_CAP = 4_500_000  # conservative headroom vs the 5MB doc'd cap

    # Sniff source media type from URL suffix.
    url_lc = url.lower()
    if url_lc.endswith(".jpg") or url_lc.endswith(".jpeg"):
        source_type = "image/jpeg"
    elif url_lc.endswith(".webp"):
        source_type = "image/webp"
    elif url_lc.endswith(".gif"):
        source_type = "image/gif"
    else:
        source_type = "image/png"

    if len(data) <= ANTHROPIC_IMAGE_CAP:
        return (data, source_type)

    # Downscale via Pillow — encode to JPEG at 85 quality, cap the long edge
    # at 1568 (Claude's ideal dimension) and iterate down if still too big.
    try:
        from io import BytesIO
        from PIL import Image
        img = Image.open(BytesIO(data))
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        long_edge = 1568
        for _ in range(4):
            w, h = img.size
            scale = min(1.0, long_edge / max(w, h))
            if scale < 1.0:
                nw, nh = int(w * scale), int(h * scale)
                resized = img.resize((nw, nh))
            else:
                resized = img
            buf = BytesIO()
            resized.save(buf, format="JPEG", quality=85, optimize=True)
            out = buf.getvalue()
            if len(out) <= ANTHROPIC_IMAGE_CAP:
                return (out, "image/jpeg")
            long_edge = int(long_edge * 0.7)
    except Exception as e:
        log.warning(f"followup: downscale failed for {url}: {e}")

    log.warning(f"followup: image still too big after downscale, skipping {url}")
    return None


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
    intel: dict,
    first_name: str,
    company_name: str,
    ad_context: Optional[dict] = None,
    landing_context: Optional[dict] = None,
    followup_index: int = 1,
    days_since_our_reply: int = 0,
    calendly_url: str = "https://calendly.com/trendfeed-media/email-marketing-audit",
) -> str:
    """
    Draft a reactivation follow-up.

    Args:
      thread: newest-first email thread rows (dicts with from/body/timestamp)
      intel: Trendtrack intel block (monthly_visits, active_ads, geography...)
      first_name: prospect first name (opener)
      company_name: prospect company (never quote intel to them)
      ad_context: {id, image_url, ad_copy, cta, landing_product, landing_url,
                   ad_library_url} — pass the top ad for this brand. Image is
                   fetched and shown to Claude via vision.
      landing_context: fallback if ad_context is None — {name, domain,
                       best_sellers[], main_category, top_products[]} etc.
      followup_index: 1 for reactivation, 2/3 for cadence. Only shapes the
                      "we've written before" framing.
      days_since_our_reply: for context, not quoted back to the prospect.
      calendly_url: prospect-facing Calendly. Never use the internal one.

    Returns the plain-text email body.
    """
    thread_str = _format_thread(thread)
    intel_str = (
        f"- Brand: {intel.get('name') or intel.get('domain') or 'unknown'}\n"
        f"- Domain: {intel.get('domain') or 'unknown'}\n"
        f"- Monthly visits: {intel.get('monthly_visits') or 0}\n"
        f"- Active Meta ads: {intel.get('active_ads') or 0}\n"
        f"- Top country: {(intel.get('geography') or {}).get('label') or 'unknown'}"
    )

    ad_block = "(no ad context available — use landing-page fallback below)"
    ad_image_b64: Optional[str] = None
    ad_media_type = "image/png"
    if ad_context:
        ad_block = (
            f"- Ad copy body: {ad_context.get('ad_copy') or '(none)'}\n"
            f"- CTA button: {ad_context.get('cta') or '(none)'}\n"
            f"- Landing product: {ad_context.get('landing_product') or '(none)'}\n"
            f"- Landing URL: {ad_context.get('landing_url') or '(none)'}\n"
            f"(Image of the ad is attached below — describe only what you can "
            f"actually SEE. Do not guess products, models, or details not "
            f"visible.)"
        )
        fetched = await _fetch_image_bytes(ad_context.get("image_url") or "")
        if fetched:
            img_bytes, ad_media_type = fetched
            ad_image_b64 = base64.standard_b64encode(img_bytes).decode()

    landing_block = ""
    if landing_context and not ad_context:
        parts = [f"- Brand name: {landing_context.get('name') or 'unknown'}"]
        if landing_context.get("domain"):
            parts.append(f"- Domain: {landing_context['domain']}")
        if landing_context.get("main_category"):
            parts.append(f"- Category: {landing_context['main_category']}")
        best = landing_context.get("best_sellers") or []
        if best:
            titles = ", ".join(str(b.get("title") or b) for b in best[:5])
            parts.append(f"- Best sellers: {titles}")
        landing_block = "LANDING / PRODUCT CONTEXT (no ads available):\n" + "\n".join(parts)

    landing_suffix = "\n" + landing_block if landing_block else ""

    intro_hint = (
        "This is the FIRST reactivation touch after a period of silence. "
        "Do not say 'circling back' — write like a busy operator with a "
        "genuine observation."
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

INTEL BLOCK (reference only, never quote back)
{intel_str}

THREAD HISTORY (newest first — read carefully so you don't repeat yourself)
\"\"\"
{thread_str}
\"\"\"

AD CONTEXT
{ad_block}
{landing_suffix}

Calendly for CTA (if you use it): {calendly_url}

Write the follow-up now. Body only, 2 to 4 sentences, one CTA."""

    content: list[dict] = [{"type": "text", "text": user_text}]
    if ad_image_b64:
        # Image FIRST so Claude sees it before the framing text.
        content = [
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": ad_media_type,
                    "data": ad_image_b64,
                },
            },
            {"type": "text", "text": user_text},
        ]

    response = await _get_client().messages.create(
        model=MODEL,
        max_tokens=400,
        system=FOLLOWUP_SYSTEM,
        messages=[{"role": "user", "content": content}],
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
