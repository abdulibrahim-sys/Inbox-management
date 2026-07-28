"""
Follow-up engine — Phase 1 (backlog reactivation).

Enumerates INTERESTED leads across the campaigns we care about, filters to
"we sent last, prospect ghosted", pulls Trendtrack intel + top static ad,
drafts a personalised reactivation follow-up, and posts a Slack review card.

Phase 2 (day-1/5/8 cadence) and Phase 3 (new-thread-from-different-mailbox)
will build on top of this module — the state store, drafter call, and
Slack post are already the right shape.

Approval flow (Phase 1):
  Slack "✅ Approve & send"  → send via plusvibe.send_reply (same thread)
  Slack "✏️ Edit"            → open modal → send edited
  Slack "🚫 Skip"             → mark skipped in Redis, do nothing

Concurrency + throttling:
  Backlog scan pages every campaign carefully (PlusVibe rate-limits at ~5
  req/sec workspace-wide). Between fetches we sleep 500ms.

  Slack cards are posted 5 at a time (`BATCH_SIZE`). The engine holds
  remaining candidates in Redis under `followup:queue` and advances the
  batch on demand — controlled from an admin endpoint or (later) after each
  approval.
"""
import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from src.drafter import draft_followup
from src.integrations.plusvibe import (
    get_email_thread,
    get_lead_data,
    list_received_emails_paginated,
)
from src.integrations.slack import post_followup_review
from src.integrations.trendtrack import (
    get_top_static_ads_for_brand,
    resolve_and_score,
)

log = logging.getLogger(__name__)

# Campaigns to include: everything except full-funnel-strategy + Ad Creative Offer.
BACKLOG_CAMPAIGNS: list[tuple[str, str]] = [
    ("6a4bb4325d0a8ff67b02b811", "Ai-ark-big brands - Copy"),
    ("6a60f7d25756c23899f6bbd2", "2 weeks - july"),
    ("69971f0aefa0db65892f6b37", "2 weeks"),
    ("69fb3fa29465cdb03f8c811f", "2 weeks - May [Outlook]"),
    ("6a15899d1bed093e5b656285", "2 weeks - May [Outlook] - Copy"),
    ("6a2033c867e914c9dffb36fd", "2 weeks - June[Outlook]"),
    ("6a300610824c59961b340989", "Ai-ark-big brands"),
]

# Same list the reply agent uses to skip our own sending accounts.
_OUR_DOMAIN_KEYWORDS = {
    "trendfeed", "trendsender", "trendconnect", "trendreach",
    "hiretrendfeed", "gettrendfeed", "jointrendfeed", "trytrendfeed",
    "trendfeedhub", "trendfeedscale", "trendscaleq", "trendscalx",
    "trendiox", "growtherio", "fundriumx",
}


def _is_our_send(from_email: str) -> bool:
    if not from_email:
        return False
    domain = from_email.split("@")[-1].lower()
    if domain.endswith(".help"):
        return True
    return any(k in domain for k in _OUR_DOMAIN_KEYWORDS)


def _is_from_prospect(from_email: str, prospect_email: str) -> bool:
    """More reliable than pattern-matching our sending personas: if the
    from-domain matches the prospect's domain (or is the prospect email),
    it's the prospect. Anything else in a lead thread is almost always us."""
    if not from_email or not prospect_email:
        return False
    a = from_email.lower().strip()
    b = prospect_email.lower().strip()
    if a == b:
        return True
    ad = a.split("@")[-1]
    bd = b.split("@")[-1]
    return ad and bd and ad == bd


@dataclass
class BacklogCandidate:
    """A single lead flagged for reactivation follow-up."""
    prospect_email: str
    first_name: str = ""
    last_name: str = ""
    company_name: str = ""
    company_website: str = ""
    campaign_id: str = ""
    campaign_name: str = ""
    interested_at: str = ""       # ISO ts of the prospect's INTERESTED message
    our_last_reply_at: str = ""   # ISO ts of the newest message from us
    reply_to_email_id: str = ""   # newest unibox email id to reply to
    reply_to_subject: str = ""    # subject of the newest message
    sending_mailbox: str = ""     # eaccount that sent our last reply
    days_since_our_reply: int = 0

    def key(self) -> str:
        return f"followup:pending:{self.prospect_email}"


async def scan_backlog(min_days_since_our_reply: int = 8) -> list[BacklogCandidate]:
    """
    Walk every INTERESTED lead in BACKLOG_CAMPAIGNS. Return the list of
    prospects where:
      - the latest message in the thread was from US, and
      - it was at least `min_days_since_our_reply` days ago, and
      - the prospect never came back to it.

    Deduped by prospect email (a lead in two campaigns counts once, keyed on
    whichever campaign's thread is newer).
    """
    cutoff_iso = (
        datetime.now(timezone.utc) - timedelta(days=90)
    ).isoformat().replace("+00:00", "Z")

    interested_by_email: dict[str, dict] = {}

    for cid, cname in BACKLOG_CAMPAIGNS:
        try:
            rows = await list_received_emails_paginated(
                cid, label="INTERESTED", stop_before_iso=cutoff_iso, max_pages=20
            )
        except Exception as e:
            log.warning(f"backlog scan: paginated list failed for {cid}: {e}")
            rows = []
        for r in rows:
            prospect_email = (r.get("from_address_email") or "").lower().strip()
            if not prospect_email or _is_our_send(prospect_email):
                continue
            ts = r.get("timestamp_created") or ""
            existing = interested_by_email.get(prospect_email)
            if existing and existing["ts"] >= ts:
                continue
            interested_by_email[prospect_email] = {
                "ts": ts,
                "campaign_id": cid,
                "campaign_name": cname,
                "subject": r.get("subject") or "",
            }
        # Rate-limit friendliness — PlusVibe 429s aggressively.
        await asyncio.sleep(0.5)

    log.info(
        f"backlog scan: {len(interested_by_email)} unique prospects flagged INTERESTED "
        f"across {len(BACKLOG_CAMPAIGNS)} campaigns"
    )

    now = datetime.now(timezone.utc)
    candidates: list[BacklogCandidate] = []
    for email, meta in interested_by_email.items():
        try:
            thread = await get_email_thread(email)
            await asyncio.sleep(0.3)
        except Exception as e:
            log.warning(f"backlog scan: thread fetch failed for {email}: {e}")
            continue
        if not thread:
            continue

        # PlusVibe returns newest first. Determine direction of the newest
        # message by comparing sender domain to prospect domain — reliable
        # regardless of which persona / mailbox sent our side.
        newest = thread[0]
        newest_sender = (newest.get("from") or "").lower()
        newest_ts = newest.get("timestamp") or ""

        if _is_from_prospect(newest_sender, email):
            # Prospect sent last → not a follow-up candidate; that's the
            # pending-initial-reply flow instead.
            continue
        # Newest is from us — the state we're looking for.

        # Age check.
        try:
            reply_dt = datetime.fromisoformat(newest_ts.replace("Z", "+00:00"))
        except Exception:
            continue
        days = (now - reply_dt).days
        if days < min_days_since_our_reply:
            continue

        # Also confirm the prospect did participate in this thread — reject
        # any thread that's entirely one-sided from our side.
        has_prospect_msg = any(
            _is_from_prospect((m.get("from") or ""), email) for m in thread
        )
        if not has_prospect_msg:
            continue

        # Enrich with lead_data
        try:
            ld = await get_lead_data(email, meta["campaign_id"]) or {}
        except Exception:
            ld = {}
        await asyncio.sleep(0.2)

        candidates.append(BacklogCandidate(
            prospect_email=email,
            first_name=ld.get("first_name") or "",
            last_name=ld.get("last_name") or "",
            company_name=ld.get("company_name") or "",
            company_website=ld.get("company_website") or "",
            campaign_id=meta["campaign_id"],
            campaign_name=meta["campaign_name"],
            interested_at=meta["ts"],
            our_last_reply_at=newest_ts,
            reply_to_email_id=str(newest.get("id") or ""),
            reply_to_subject=newest.get("subject") or meta["subject"],
            sending_mailbox=newest_sender,
            days_since_our_reply=days,
        ))

    # Newest-ghost first (they're warmer than 90-day-olds).
    candidates.sort(key=lambda c: c.our_last_reply_at, reverse=True)
    log.info(f"backlog scan: {len(candidates)} candidates after thread analysis")
    return candidates


@dataclass
class DraftedFollowup:
    candidate: BacklogCandidate
    draft: str
    ad_used: Optional[dict]
    intel: dict


async def draft_for_candidate(c: BacklogCandidate) -> Optional[DraftedFollowup]:
    """
    Build the full context for one candidate and produce the follow-up draft.

    Returns None if Trendtrack can't resolve the brand AND we have nothing
    else to say — the caller can decide to escalate manually instead of
    firing a generic follow-up.
    """
    # Intel
    domain = c.prospect_email.split("@")[-1] if "@" in c.prospect_email else ""
    intel = await resolve_and_score(
        email_domain=domain, brand_name_fallback=c.company_name or ""
    )

    # If Trendtrack says geography=not_allowed, don't follow up at all.
    geo = (intel.get("geography") or {}).get("verdict") or "unknown"
    if geo == "not_allowed":
        log.info(
            f"follow-up skipped for {c.prospect_email}: geo verdict not_allowed"
        )
        return None

    # Top static ad (may be empty for brands with no active ads or video-only).
    ad_context: Optional[dict] = None
    if intel.get("advertiser_id"):
        ads = await get_top_static_ads_for_brand(intel["advertiser_id"], limit=1)
        if ads:
            ad_context = ads[0]

    # Landing fallback — build from intel we already have. A dedicated landing
    # scrape can come later if this proves too generic.
    landing_context = None
    if not ad_context and intel.get("resolved"):
        landing_context = {
            "name": intel.get("name") or c.company_name,
            "domain": intel.get("domain") or domain,
        }

    # Thread — newest first, cap at 8 for the drafter.
    try:
        thread = await get_email_thread(c.prospect_email)
    except Exception:
        thread = []
    thread = thread[:8]

    draft = await draft_followup(
        thread=thread,
        intel=intel,
        first_name=c.first_name or "",
        company_name=c.company_name or intel.get("name") or "",
        ad_context=ad_context,
        landing_context=landing_context,
        followup_index=1,
        days_since_our_reply=c.days_since_our_reply,
    )
    return DraftedFollowup(
        candidate=c, draft=draft, ad_used=ad_context, intel=intel
    )


# ── Redis-backed queue + pending state ──────────────────────────────────────

def _get_redis():
    """Late import so a missing Upstash config doesn't crash the whole module."""
    try:
        from upstash_redis import Redis
        url = os.getenv("UPSTASH_REDIS_REST_URL")
        tok = os.getenv("UPSTASH_REDIS_REST_TOKEN")
        if not url or not tok:
            return None
        return Redis(url=url, token=tok)
    except Exception as e:
        log.warning(f"followup engine: redis init failed: {e}")
        return None


QUEUE_KEY = "followup:backlog:queue"
PENDING_PREFIX = "followup:pending:"
SKIP_PREFIX = "followup:skipped:"
QUEUE_TTL = 60 * 60 * 24 * 14  # 2 weeks — the whole backlog run should finish in that


def enqueue_candidates(candidates: list[BacklogCandidate]) -> int:
    """Persist a fresh scan to Redis. Overwrites any previous queue."""
    r = _get_redis()
    if not r:
        log.warning("enqueue_candidates: no redis; state won't survive restart")
        return 0
    payload = [json.dumps({
        "prospect_email": c.prospect_email,
        "first_name": c.first_name,
        "last_name": c.last_name,
        "company_name": c.company_name,
        "company_website": c.company_website,
        "campaign_id": c.campaign_id,
        "campaign_name": c.campaign_name,
        "interested_at": c.interested_at,
        "our_last_reply_at": c.our_last_reply_at,
        "reply_to_email_id": c.reply_to_email_id,
        "reply_to_subject": c.reply_to_subject,
        "sending_mailbox": c.sending_mailbox,
        "days_since_our_reply": c.days_since_our_reply,
    }) for c in candidates]
    r.delete(QUEUE_KEY)
    if payload:
        r.rpush(QUEUE_KEY, *payload)
        r.expire(QUEUE_KEY, QUEUE_TTL)
    return len(payload)


def peek_queue(n: int = 5) -> list[BacklogCandidate]:
    r = _get_redis()
    if not r:
        return []
    raw = r.lrange(QUEUE_KEY, 0, n - 1) or []
    out = []
    for j in raw:
        try:
            d = json.loads(j)
            out.append(BacklogCandidate(**d))
        except Exception:
            continue
    return out


def pop_queue(n: int) -> list[BacklogCandidate]:
    r = _get_redis()
    if not r:
        return []
    popped = []
    for _ in range(n):
        j = r.lpop(QUEUE_KEY)
        if not j:
            break
        try:
            d = json.loads(j)
            popped.append(BacklogCandidate(**d))
        except Exception:
            continue
    return popped


def queue_size() -> int:
    r = _get_redis()
    if not r:
        return 0
    return int(r.llen(QUEUE_KEY) or 0)


def store_pending_followup(record_id: str, payload: dict, ttl: int = 60 * 60 * 24 * 7) -> None:
    r = _get_redis()
    if not r:
        return
    r.set(f"{PENDING_PREFIX}{record_id}", json.dumps(payload), ex=ttl)


def get_pending_followup(record_id: str) -> Optional[dict]:
    r = _get_redis()
    if not r:
        return None
    raw = r.get(f"{PENDING_PREFIX}{record_id}")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def delete_pending_followup(record_id: str) -> None:
    r = _get_redis()
    if not r:
        return
    r.delete(f"{PENDING_PREFIX}{record_id}")


def mark_skipped(prospect_email: str, ttl: int = 60 * 60 * 24 * 90) -> None:
    r = _get_redis()
    if not r:
        return
    r.set(f"{SKIP_PREFIX}{prospect_email}", "1", ex=ttl)


def is_skipped(prospect_email: str) -> bool:
    r = _get_redis()
    if not r:
        return False
    return bool(r.get(f"{SKIP_PREFIX}{prospect_email}"))


# ── Post-a-batch orchestration ──────────────────────────────────────────────

BATCH_SIZE = 5


async def post_next_batch(batch_size: int = BATCH_SIZE) -> dict:
    """
    Pull up to `batch_size` candidates from the queue, draft each, and post
    Slack cards. Returns a summary dict.

    Called on demand from the admin endpoint. Approval / edit / skip is
    handled separately in main.py's Slack action handler.
    """
    picked = pop_queue(batch_size)
    if not picked:
        return {"posted": 0, "remaining": queue_size(), "detail": "queue empty"}

    posted = []
    skipped_geo = 0
    for c in picked:
        if is_skipped(c.prospect_email):
            log.info(f"backlog: {c.prospect_email} previously skipped, dropping")
            continue
        try:
            drafted = await draft_for_candidate(c)
        except Exception as e:
            log.exception(f"draft_for_candidate failed for {c.prospect_email}: {e}")
            drafted = None
        if drafted is None:
            skipped_geo += 1
            continue

        # Prior thread summary for the Slack card — one-liner per message.
        thread_summary = ""
        try:
            thread = await get_email_thread(c.prospect_email) or []
            lines = []
            for m in thread[:5]:
                sender = m.get("from") or "?"
                snippet = " ".join((m.get("body") or "").split())[:180]
                lines.append(f"• {sender}: {snippet}")
            thread_summary = "\n".join(lines) or "(no thread available)"
        except Exception:
            thread_summary = "(thread fetch failed)"

        ad_preview = None
        if drafted.ad_used:
            ad_preview = {
                "image_url": drafted.ad_used.get("image_url") or "",
                "landing_product": drafted.ad_used.get("landing_product") or "",
                "landing_url": drafted.ad_used.get("landing_url") or "",
            }

        # Post to Slack. record_id = the newest email id in the thread which
        # PlusVibe accepts as reply_to_id.
        record_id = c.reply_to_email_id or c.prospect_email
        try:
            slack_ts = post_followup_review(
                record_id=record_id,
                first_name=c.first_name,
                last_name=c.last_name,
                company_name=c.company_name or drafted.intel.get("name") or "",
                prospect_email=c.prospect_email,
                followup_index=1,
                days_since_our_reply=c.days_since_our_reply,
                prior_thread_summary=thread_summary,
                draft_followup_text=drafted.draft,
                ad_preview=ad_preview,
                intel=drafted.intel,
            )
        except Exception as e:
            log.exception(f"Slack post failed for {c.prospect_email}: {e}")
            continue

        # Persist pending so the approve handler can send it.
        store_pending_followup(record_id, {
            "type": "backlog_reactivation",
            "prospect_email": c.prospect_email,
            "reply_to_email_id": c.reply_to_email_id,
            "sending_mailbox": c.sending_mailbox,
            "subject": c.reply_to_subject,
            "draft": drafted.draft,
            "slack_ts": slack_ts,
            "candidate": {
                "campaign_id": c.campaign_id,
                "campaign_name": c.campaign_name,
                "first_name": c.first_name,
                "last_name": c.last_name,
                "company_name": c.company_name,
                "days_since_our_reply": c.days_since_our_reply,
            },
        })
        posted.append(c.prospect_email)

    return {
        "posted": len(posted),
        "posted_leads": posted,
        "skipped_geo_or_error": skipped_geo,
        "remaining": queue_size(),
    }
