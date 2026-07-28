"""
Never-replied engine — for the 76-lead cohort where the prospect wrote
in with an INTERESTED tag and we never got back to them.

Different flow from the follow-up engine:
  follow_up:      we replied, prospect ghosted → reactivation touch
  never_replied:  prospect replied, we ghosted → late INITIAL reply

Reuses the reply-agent's 32-intent classifier + drafter. When the original
sending mailbox is dead, drafter switches to hand-off mode (colleague
taking over) and defers any link to a follow-up message per the
deliverability rule.

Approve flow reuses the follow-up engine's Redis pending state (same key
prefix, same _send_followup handler in main.py) — the pending records
just carry a different `type` marker.
"""
import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from src.classifier import (
    check_tld_geography,
    classify_reply,
    promote_geo_stop,
    resolve_intent_24,
)
from src.drafter import draft_reply
from src.integrations.plusvibe import (
    get_email_thread,
    get_lead_data,
    get_lead_records,
    list_live_mailboxes,
    list_received_emails_paginated,
)
from src.integrations.slack import post_followup_review
from src.followup_engine import (
    ACTIVE_CAMPAIGN_IDS,
    BACKLOG_CAMPAIGNS,
    _get_redis,
    _is_from_prospect,
    _persona_first_name,
    is_skipped,
    mark_skipped,
    store_pending_followup,
)

log = logging.getLogger(__name__)


@dataclass
class NeverRepliedCandidate:
    prospect_email: str
    first_name: str = ""
    last_name: str = ""
    company_name: str = ""
    company_website: str = ""
    campaign_id: str = ""
    campaign_name: str = ""
    interested_at: str = ""
    # The prospect's newest message we haven't responded to
    prospect_msg_at: str = ""
    prospect_msg_body: str = ""
    prospect_msg_subject: str = ""
    prospect_msg_email_id: str = ""  # reply_to_id for send_reply
    # Original mailbox (from previous outbound in the thread, if any)
    prior_sending_mailbox: str = ""
    days_since_prospect_msg: int = 0


NEVER_REPLIED_QUEUE = "never_replied:backlog:queue"
NEVER_REPLIED_QUEUE_TTL = 60 * 60 * 24 * 14


async def scan_never_replied(
    min_days_since_prospect_msg: int = 8,
    max_lookback_days: int = 180,
) -> list[NeverRepliedCandidate]:
    """
    Walk every INTERESTED lead in BACKLOG_CAMPAIGNS. Return only prospects
    where the latest message is FROM THEM (i.e., we never got back).

    Deduped by prospect email.
    """
    cutoff_iso = (
        datetime.now(timezone.utc) - timedelta(days=max_lookback_days)
    ).isoformat().replace("+00:00", "Z")

    unique: dict[str, dict] = {}
    for cid, cname in BACKLOG_CAMPAIGNS:
        try:
            rows = await list_received_emails_paginated(
                cid, label="INTERESTED",
                stop_before_iso=cutoff_iso, max_pages=30,
            )
        except Exception as e:
            log.warning(f"never_replied scan: enum failed for {cname}: {e}")
            continue
        for r in rows:
            email = (r.get("from_address_email") or "").lower().strip()
            if not email or "@" not in email:
                continue
            ts = r.get("timestamp_created") or ""
            if not unique.get(email) or unique[email]["ts"] < ts:
                unique[email] = {
                    "ts": ts, "campaign_id": cid, "campaign_name": cname,
                    "subject": r.get("subject") or "",
                }
        await asyncio.sleep(0.5)

    now = datetime.now(timezone.utc)
    candidates: list[NeverRepliedCandidate] = []
    for email, meta in unique.items():
        try:
            thread = await get_email_thread(email)
            await asyncio.sleep(0.3)
        except Exception:
            continue
        if not thread:
            continue

        newest = thread[0]
        newest_sender = (newest.get("from") or "").lower()
        newest_ts = newest.get("timestamp") or ""

        # Only keep threads where the newest message is FROM the prospect
        if not _is_from_prospect(newest_sender, email):
            continue

        # Age check
        try:
            prospect_dt = datetime.fromisoformat(newest_ts.replace("Z", "+00:00"))
        except Exception:
            continue
        days = (now - prospect_dt).days
        if days < min_days_since_prospect_msg:
            continue

        # Find the last outbound message from us in the thread — that's the
        # mailbox we'd try to send from first.
        prior_mailbox = ""
        for m in thread:
            if not _is_from_prospect((m.get("from") or ""), email):
                prior_mailbox = (m.get("from") or "")
                break

        # Enrich with lead_data
        try:
            ld = await get_lead_data(email, meta["campaign_id"]) or {}
        except Exception:
            ld = {}
        await asyncio.sleep(0.2)

        candidates.append(NeverRepliedCandidate(
            prospect_email=email,
            first_name=ld.get("first_name") or "",
            last_name=ld.get("last_name") or "",
            company_name=ld.get("company_name") or "",
            company_website=ld.get("company_website") or "",
            campaign_id=meta["campaign_id"],
            campaign_name=meta["campaign_name"],
            interested_at=meta["ts"],
            prospect_msg_at=newest_ts,
            prospect_msg_body=newest.get("body") or "",
            prospect_msg_subject=newest.get("subject") or meta["subject"],
            prospect_msg_email_id=str(newest.get("id") or ""),
            prior_sending_mailbox=prior_mailbox,
            days_since_prospect_msg=days,
        ))

    # Freshest first
    candidates.sort(key=lambda c: c.prospect_msg_at, reverse=True)
    log.info(
        f"never_replied scan: {len(candidates)} candidates after thread analysis "
        f"(lookback={max_lookback_days}d, min_age={min_days_since_prospect_msg}d)"
    )
    return candidates


@dataclass
class DraftedInitialReply:
    candidate: NeverRepliedCandidate
    draft: str
    thread: list[dict] = field(default_factory=list)
    classification: dict = field(default_factory=dict)
    disposition: str = "draft"  # or escalate / disregard / stop / suppress / park
    send_mode: str = "reply"     # or "new_thread"
    send_from_mailbox: str = ""
    send_camp_id: str = ""
    send_lead_id: str = ""


async def draft_for_candidate(
    c: NeverRepliedCandidate,
    live_mailboxes: list[dict] | None = None,
    live_by_email: set[str] | None = None,
    picker_index: int = 0,
) -> Optional[DraftedInitialReply]:
    """
    Classify the prospect's message and produce an initial reply. Returns
    None when the disposition isn't 'draft' (escalate/disregard/etc.).

    Uses the reply-agent's full pipeline:
      TLD geo gate → classifier → resolve_intent_24 → geo promotion
    """
    if check_tld_geography(c.prospect_email) == "not_allowed":
        log.info(f"never_replied: {c.prospect_email} in excluded TLD, skipping")
        return None

    try:
        thread = await get_email_thread(c.prospect_email)
    except Exception:
        thread = []
    thread = thread[:8]

    # Classify prospect's most recent message
    thread_ctx = "\n---\n".join(
        f"from {m.get('from','')}: {(m.get('body') or '')[:400]}"
        for m in (thread or [])[:6]
    )
    classification = await classify_reply(
        body=c.prospect_msg_body,
        subject=c.prospect_msg_subject,
        thread_context=thread_ctx,
    )
    classification = resolve_intent_24(classification)
    classification = promote_geo_stop(
        classification, geography_verdict=check_tld_geography(c.prospect_email)
    )

    disposition = classification.get("disposition")
    if disposition != "draft":
        # Non-draft dispositions get returned so the caller can log/skip.
        return DraftedInitialReply(
            candidate=c, draft="",
            classification=classification,
            disposition=disposition or "escalate",
            thread=thread,
        )

    # Route: same-thread reply if original mailbox alive, else hand-off.
    prior_mb_lc = (c.prior_sending_mailbox or "").lower()
    original_is_live = (
        prior_mb_lc in (live_by_email or set())
        if live_by_email is not None else True
    )
    send_camp_id, send_lead_id = "", ""

    if original_is_live and c.prior_sending_mailbox:
        send_mode = "reply"
        send_from = c.prior_sending_mailbox
        handoff_from = ""
    else:
        if not live_mailboxes:
            log.warning(
                f"never_replied: no live mailboxes for {c.prospect_email}, "
                f"skipping"
            )
            return None
        picked = live_mailboxes[picker_index % len(live_mailboxes)]
        send_mode = "new_thread"
        send_from = picked["email"]
        handoff_from = _persona_first_name(c.prior_sending_mailbox) or "a colleague"

        # Resolve camp_id + lead_id via /lead/get, prefer an active campaign.
        records = await get_lead_records(c.prospect_email)
        active = [r for r in records if r["campaign_id"] in ACTIVE_CAMPAIGN_IDS]
        chosen = active[0] if active else (records[0] if records else None)
        if not chosen:
            log.warning(
                f"never_replied: no lead records for {c.prospect_email}"
            )
            return None
        send_camp_id = chosen["campaign_id"]
        send_lead_id = chosen["lead_id"]

    draft = await draft_reply(
        classification=classification,
        prospect_body=c.prospect_msg_body,
        first_name=c.first_name or "there",
        company_name=c.company_name or "",
        prospect_email=c.prospect_email,
        thread=thread,
        handoff_from_persona=handoff_from,
        days_since_their_message=c.days_since_prospect_msg,
    )

    return DraftedInitialReply(
        candidate=c,
        draft=draft,
        thread=thread,
        classification=classification,
        disposition="draft",
        send_mode=send_mode,
        send_from_mailbox=send_from,
        send_camp_id=send_camp_id,
        send_lead_id=send_lead_id,
    )


# ── Queue helpers (parallel to followup engine, separate key) ────────────

def enqueue_candidates(candidates: list[NeverRepliedCandidate]) -> int:
    r = _get_redis()
    if not r:
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
        "prospect_msg_at": c.prospect_msg_at,
        "prospect_msg_body": c.prospect_msg_body,
        "prospect_msg_subject": c.prospect_msg_subject,
        "prospect_msg_email_id": c.prospect_msg_email_id,
        "prior_sending_mailbox": c.prior_sending_mailbox,
        "days_since_prospect_msg": c.days_since_prospect_msg,
    }) for c in candidates]
    r.delete(NEVER_REPLIED_QUEUE)
    if payload:
        r.rpush(NEVER_REPLIED_QUEUE, *payload)
        r.expire(NEVER_REPLIED_QUEUE, NEVER_REPLIED_QUEUE_TTL)
    return len(payload)


def peek_queue(n: int = 10) -> list[NeverRepliedCandidate]:
    r = _get_redis()
    if not r:
        return []
    raw = r.lrange(NEVER_REPLIED_QUEUE, 0, n - 1) or []
    out = []
    for j in raw:
        try:
            out.append(NeverRepliedCandidate(**json.loads(j)))
        except Exception:
            continue
    return out


def pop_queue(n: int) -> list[NeverRepliedCandidate]:
    r = _get_redis()
    if not r:
        return []
    popped = []
    for _ in range(n):
        j = r.lpop(NEVER_REPLIED_QUEUE)
        if not j:
            break
        try:
            popped.append(NeverRepliedCandidate(**json.loads(j)))
        except Exception:
            continue
    return popped


def queue_size() -> int:
    r = _get_redis()
    if not r:
        return 0
    return int(r.llen(NEVER_REPLIED_QUEUE) or 0)


BATCH_SIZE = 5


async def post_next_batch(batch_size: int = BATCH_SIZE) -> dict:
    """
    Pull up to `batch_size` never-replied candidates, classify + draft,
    post Slack cards, persist pending.
    """
    picked = pop_queue(batch_size)
    if not picked:
        return {"posted": 0, "remaining": queue_size(), "detail": "queue empty"}

    live_mailboxes = await list_live_mailboxes(active_campaign_ids=ACTIVE_CAMPAIGN_IDS)
    live_by_email = {m["email"].lower() for m in live_mailboxes}
    log.info(f"never_replied: {len(live_mailboxes)} live mailboxes")

    posted, skipped_non_draft, errors = [], [], 0
    for i, c in enumerate(picked):
        if is_skipped(c.prospect_email):
            continue
        try:
            drafted = await draft_for_candidate(
                c, live_mailboxes=live_mailboxes,
                live_by_email=live_by_email, picker_index=i,
            )
        except Exception as e:
            log.exception(f"never_replied draft failed for {c.prospect_email}: {e}")
            errors += 1
            continue
        if drafted is None:
            continue
        if drafted.disposition != "draft":
            skipped_non_draft.append({
                "email": c.prospect_email,
                "disposition": drafted.disposition,
                "intent": drafted.classification.get("intent_n"),
                "reason": drafted.classification.get("escalation_reason") or
                          drafted.classification.get("reasoning", ""),
            })
            continue

        # Thread summary for the Slack card
        thread_lines = []
        for m in (drafted.thread or [])[:5]:
            sender = m.get("from") or "?"
            snippet = " ".join((m.get("body") or "").split())[:180]
            thread_lines.append(f"• {sender}: {snippet}")
        thread_summary = "\n".join(thread_lines) or "(no thread available)"

        if drafted.send_mode == "new_thread":
            route_note = (
                f"↳ *New thread* from `{drafted.send_from_mailbox}` — "
                f"original mailbox `{c.prior_sending_mailbox or 'unknown'}` "
                f"no longer connected. Intent "
                f"{drafted.classification.get('intent_n')}: "
                f"{drafted.classification.get('intent_name','?')}"
            )
        else:
            route_note = (
                f"↳ *Reply* on original thread from "
                f"`{drafted.send_from_mailbox}`. Intent "
                f"{drafted.classification.get('intent_n')}: "
                f"{drafted.classification.get('intent_name','?')}"
            )
        thread_summary = f"{route_note}\n\n{thread_summary}"

        # record_id = prospect_msg_email_id so PlusVibe threads correctly
        record_id = c.prospect_msg_email_id or c.prospect_email
        try:
            slack_ts = post_followup_review(
                record_id=record_id,
                first_name=c.first_name,
                last_name=c.last_name,
                company_name=c.company_name,
                prospect_email=c.prospect_email,
                followup_index=0,  # 0 = initial reply, not a reactivation
                days_since_our_reply=c.days_since_prospect_msg,
                prior_thread_summary=thread_summary,
                draft_followup_text=drafted.draft,
            )
        except Exception as e:
            log.exception(f"never_replied Slack post failed: {e}")
            errors += 1
            continue

        from src.integrations.slack import SLACK_CHANNEL_ID as _SCHAN
        store_pending_followup(record_id, {
            "type": "never_replied_initial",
            "prospect_email": c.prospect_email,
            "reply_to_email_id": c.prospect_msg_email_id,
            "sending_mailbox": drafted.send_from_mailbox,
            "original_sending_mailbox": c.prior_sending_mailbox,
            "send_mode": drafted.send_mode,
            "send_camp_id": drafted.send_camp_id,
            "send_lead_id": drafted.send_lead_id,
            "subject": c.prospect_msg_subject,
            "draft": drafted.draft,
            "slack_ts": slack_ts,
            "slack_channel": _SCHAN,
            "classification": {
                "intent_n": drafted.classification.get("intent_n"),
                "intent_name": drafted.classification.get("intent_name"),
            },
            "candidate": {
                "campaign_id": c.campaign_id,
                "campaign_name": c.campaign_name,
                "first_name": c.first_name,
                "last_name": c.last_name,
                "company_name": c.company_name,
                "days_since_prospect_msg": c.days_since_prospect_msg,
            },
        })
        posted.append(c.prospect_email)

    return {
        "posted": len(posted),
        "posted_leads": posted,
        "skipped_non_draft": skipped_non_draft,
        "errors": errors,
        "remaining": queue_size(),
    }
