"""
Trendfeed Inbox Reply Agent — FastAPI service.

Flow, per reply_agent_instructions.md:

  PlusVibe unibox → poller / webhook → _process_reply
    1. Lead status check (INTERESTED only for drafting)
    2. Beehiiv auto-subscribe (positive replies feed the newsletter)
    3. Trendtrack brand resolution (intel block)
    4. Classify intent (1..32)
    5. Resolve conditional intents (24) + geography gate
    6. Route by disposition:
         draft     → drafter → Slack review card
         disregard → Slack disregard notification (no send)
         escalate  → Slack "needs a human" (no send)
         park      → no draft, sequence paused (Slack note only)
         stop      → no reply, silent
         suppress  → Slack unsubscribe alert (no draft; PlusVibe handled manually)

  Manager approves in Slack → send via PlusVibe
  MEETING_BOOKED tag from PlusVibe → separate handler (Slack notification + Beehiiv)

The follow-up scheduler, Google Sheets CRM writes, weekly/monthly reports,
and @mention CRM commands have been removed from this version — they'll be
rebuilt against the new spec.
"""
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv

load_dotenv()

from fastapi import BackgroundTasks, FastAPI, Request, Response
from fastapi.responses import JSONResponse

from src.classifier import classify_reply, promote_geo_stop, resolve_intent_24
from src.drafter import draft_reply
from src.integrations.beehiiv import process_retry_queue, subscribe_to_newsletter
from src.integrations.plusvibe import (
    fetch_latest_email_id,
    get_email_thread,
    get_lead_data,
    list_received_emails,
    parse_webhook,
    send_reply,
)
from src.integrations.slack import (
    SLACK_CHANNEL_ID,
    open_edit_followup_send_modal,
    open_edit_modal,
    post_disregard_notification,
    post_escalation_message,
    post_review_message,
    post_unsubscribe_alert,
    update_message_approved,
    update_message_edited_sent,
    update_message_skipped,
    verify_slack_signature,
)
from src.followup_engine import (
    BATCH_SIZE,
    delete_pending_followup,
    enqueue_candidates,
    get_pending_followup,
    mark_skipped,
    peek_queue,
    post_next_batch,
    queue_size,
    scan_backlog,
    store_pending_followup,
)
from src.integrations.trendtrack import resolve_and_score

# ── Active campaigns (PlusVibe) ──────────────────────────────────────────────
# All replies route to SLACK_CHANNEL_ID (#inbox-agent-reply). Add / remove
# campaign IDs here as they're launched or paused in PlusVibe. The unibox
# poller iterates every entry in ACTIVE_CAMPAIGNS.
CAMPAIGN_AI_ARK_BIG_BRANDS = "6a4bb4325d0a8ff67b02b811"  # Ai-ark-big brands - Copy
CAMPAIGN_2_WEEKS_JULY      = "6a60f7d25756c23899f6bbd2"  # 2 weeks - july

ACTIVE_CAMPAIGNS: list[str] = [
    CAMPAIGN_AI_ARK_BIG_BRANDS,
    CAMPAIGN_2_WEEKS_JULY,
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ── Simple in-memory pending store (Redis-free) ─────────────────────────────
# Manager approvals arrive via Slack minutes after the draft was posted; a
# process crash between the two loses the pending draft. That's an acceptable
# tradeoff — Slack still holds the drafted text visibly, so a manager can copy
# it out and send manually.

_PENDING: dict[str, dict] = {}


def _store_pending(key: str, value: dict) -> None:
    _PENDING[key] = value


def _get_pending(key: str) -> dict | None:
    return _PENDING.get(key)


def _delete_pending(key: str) -> None:
    _PENDING.pop(key, None)


# ── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Inbox Management Agent starting up")
    asyncio.create_task(_beehiiv_retry_scheduler())
    asyncio.create_task(_unibox_poller())
    yield
    log.info("Inbox Management Agent shutting down")


app = FastAPI(title="Inbox Management Agent", lifespan=lifespan)


# ── Health check ─────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


# ── PlusVibe webhook ─────────────────────────────────────────────────────────

@app.post("/webhook/plusvibe")
async def plusvibe_webhook(request: Request, background: BackgroundTasks):
    """Receive events from PlusVibe — reply events + meeting-booked tag events."""
    body_bytes = await request.body()
    log.info(f"PlusVibe webhook raw: {body_bytes[:500]}")

    try:
        body = json.loads(body_bytes)
    except Exception:
        log.error(f"PlusVibe webhook: bad JSON: {body_bytes[:200]}")
        return JSONResponse({"status": "received"}, status_code=200)

    log.info(f"PlusVibe webhook parsed: {json.dumps(body)[:400]}")

    if _is_meeting_booked(body):
        background.add_task(_process_meeting_booked, body)
    else:
        background.add_task(_process_reply, body)

    return JSONResponse({"status": "received"}, status_code=200)


def _is_meeting_booked(payload: dict) -> bool:
    data = payload.get("data", payload)
    event_type = (payload.get("event_type") or payload.get("event") or "").lower()
    tag = (data.get("tag") or data.get("label") or data.get("tag_name") or "").lower()
    return (
        "meeting" in event_type
        or "booked" in event_type
        or tag == "meeting booked"
        or "meeting booked" in tag
    )


# ── Main reply processing ───────────────────────────────────────────────────

async def _process_reply(payload: dict) -> None:
    """Process one INTERESTED reply end to end."""
    try:
        reply = parse_webhook(payload)
        log.info(f"Processing reply from {reply.from_email} ({reply.company_name})")

        # 1. Beehiiv subscribe (positive-reply signal)
        if reply.from_email:
            try:
                await subscribe_to_newsletter(
                    email=reply.from_email,
                    first_name=reply.first_name or "",
                    last_name=reply.last_name or "",
                )
            except Exception:
                log.exception("Beehiiv subscribe failed (non-fatal)")

        # 2. Resolve email_id if the webhook didn't include one
        if not reply.email_id and reply.from_email:
            reply.email_id = await fetch_latest_email_id(reply.from_email) or ""
            log.info(f"Resolved email_id via unibox: {reply.email_id}")

        record_id = reply.email_id or reply.lead_id or reply.from_email
        if not record_id:
            log.error("No identifier for this reply, skipping")
            return

        # 3. Trendtrack brand resolution → intel block
        email_domain = reply.from_email.split("@")[-1] if reply.from_email else ""
        intel = await resolve_and_score(
            email_domain=email_domain,
            brand_name_fallback=reply.company_name or "",
        )
        log.info(
            f"Trendtrack: {intel.get('confidence_label')}, "
            f"{intel.get('monthly_visits')} visits, "
            f"{intel.get('active_ads')} ads, "
            f"geo={intel.get('geography', {}).get('verdict')}"
        )

        # 4. Fetch thread for classifier context + move-1/2 counting
        thread = []
        try:
            thread = await get_email_thread(reply.from_email) if reply.from_email else []
        except Exception:
            log.exception("Thread fetch failed (non-fatal)")
        thread_context = _format_thread_for_classifier(thread)

        # 5. Classify
        classification = await classify_reply(
            body=reply.body,
            subject=reply.subject,
            thread_context=thread_context,
        )
        log.info(
            f"Classified: {classification['intent_n']}. {classification['intent_name']} "
            f"({classification['confidence']}, disp={classification['disposition']})"
        )

        # 6. Resolve conditional intent 24 + geography gate
        classification = resolve_intent_24(
            classification,
            active_ads=intel.get("active_ads", 0),
            monthly_visits=intel.get("monthly_visits", 0),
        )
        classification = promote_geo_stop(
            classification,
            geography_verdict=(intel.get("geography") or {}).get("verdict") or "unknown",
        )

        # 7. Route by disposition
        await _route_by_disposition(
            classification=classification,
            reply=reply,
            record_id=record_id,
            intel=intel,
            thread=thread,
        )

    except Exception as e:
        log.exception(f"Error processing reply: {e}")


async def _route_by_disposition(
    classification: dict,
    reply,
    record_id: str,
    intel: dict,
    thread: list[dict],
) -> None:
    disposition = classification.get("disposition")
    intent_n = classification.get("intent_n", 0)
    intent_name = classification.get("intent_name", "unknown")

    common_kwargs = dict(
        first_name=reply.first_name or "",
        last_name=reply.last_name or "",
        company_name=reply.company_name or "",
        prospect_email=reply.from_email or "",
        intent_n=intent_n,
        intent_name=intent_name,
        original_message=reply.body,
        intel=intel,
    )

    if disposition == "draft":
        draft = await draft_reply(
            classification=classification,
            prospect_body=reply.body,
            first_name=reply.first_name or "there",
            company_name=reply.company_name or "",
            intel=intel,
            thread=thread,
            our_sending_domain=(reply.to_email.split("@")[-1] if reply.to_email else ""),
        )
        log.info(f"Draft created ({len(draft)} chars) for intent {intent_n}")

        slack_ts = post_review_message(
            email_id=record_id,
            draft_response=draft,
            **common_kwargs,
        )
        _store_pending(record_id, {
            "reply": reply.model_dump(),
            "intent_n": intent_n,
            "intent_name": intent_name,
            "ai_draft": draft,
            "slack_ts": slack_ts,
            "slack_channel": SLACK_CHANNEL_ID,
        })
        log.info(f"Posted draft to Slack ts={slack_ts}")

    elif disposition == "disregard":
        reason = classification.get("disregard_reason") or (
            "Prospect below qualifying floor per section 24 (no ads, <5k visits)."
        )
        ts = post_disregard_notification(reason=reason, **common_kwargs)
        log.info(f"Disregarded {record_id} — {reason} (ts={ts})")

    elif disposition == "escalate":
        reason = (
            classification.get("escalation_reason")
            or f"Intent {intent_n} routed to escalation per spec."
        )
        ts = post_escalation_message(reason=reason, **common_kwargs)
        log.info(f"Escalated {record_id} — {reason} (ts={ts})")

    elif disposition == "park":
        # OOO: silent. The sequence pause is handled by PlusVibe's OOO label;
        # we log for visibility but don't touch anything else.
        log.info(f"Parked {record_id} — intent {intent_n} (OOO or later-timing)")

    elif disposition == "stop":
        log.info(
            f"Stop-no-reply {record_id} — intent {intent_n} "
            f"({intent_name}). Marked seen, no send."
        )

    elif disposition == "suppress":
        post_unsubscribe_alert(
            first_name=reply.first_name or "",
            last_name=reply.last_name or "",
            company_name=reply.company_name or "",
            from_email=reply.from_email or "",
        )
        log.info(f"Suppression alert posted for {record_id}")

    else:
        # Unknown disposition — treat as escalate.
        ts = post_escalation_message(
            reason=f"Unknown disposition {disposition!r} — classifier bug or spec drift.",
            **common_kwargs,
        )
        log.warning(f"Unknown disposition for {record_id}: {disposition} (ts={ts})")


def _format_thread_for_classifier(thread: list[dict]) -> str:
    """Compact thread summary for the classifier's user prompt."""
    if not thread:
        return ""
    lines = []
    for msg in thread[:6]:  # cap at 6 most recent
        sender = msg.get("from") or "(unknown)"
        body = (msg.get("body") or "")[:400]
        ts = msg.get("timestamp") or ""
        lines.append(f"[{ts}] from {sender}: {body}")
    return "\n---\n".join(lines)


# ── Meeting booked handler ──────────────────────────────────────────────────

async def _process_meeting_booked(payload: dict) -> None:
    """
    Handle a 'Meeting booked' tag from PlusVibe.

    New scope: Slack notification + Beehiiv subscribe. The Google Sheets
    Call Log write from the previous version is gone — the CRM is being
    rebuilt separately.
    """
    try:
        data = payload.get("data", payload)
        email = (data.get("email") or data.get("from_address") or "").strip().lower()
        first_name = data.get("first_name") or ""
        last_name = data.get("last_name") or ""
        name = f"{first_name} {last_name}".strip() or email
        company = data.get("company_name") or ""
        campaign = data.get("campaign_name") or ""

        if not email:
            log.info("Meeting booked: no email in payload, skipping")
            return

        # Skip our own sending accounts.
        if _is_sending_account(email, name):
            log.info(f"Meeting booked: skipping sending account {email}")
            return

        from src.integrations.slack import post_call_booked_message
        post_call_booked_message(name=name, company=company, email=email, campaign=campaign)
        log.info(f"Meeting booked posted for {email}")

        try:
            await subscribe_to_newsletter(
                email=email, first_name=first_name, last_name=last_name
            )
        except Exception:
            log.exception("Beehiiv subscribe failed for meeting-booked lead")

    except Exception as e:
        log.exception(f"Meeting booked processing error: {e}")


# ── Sending-account filter ──────────────────────────────────────────────────

_SENDING_DOMAIN_KEYWORDS = {
    "trendfeed", "trendsender", "trendconnect", "trendreach",
    "hiretrendfeed", "gettrendfeed", "jointrendfeed", "trytrendfeed",
}
_SENDING_DOMAIN_EXACT = {"trendfeed.co.uk", "trendfeed.co"}
_SENDING_PERSONAS = {
    "elena clifford", "mina willard", "riley marchmain", "julia milton",
    "hanna raymond", "kendall hollinghurst", "crystal rosewood",
    "erin whitfield", "bethany cranston", "lacey northcott", "raymond hanna",
}


def _is_sending_account(email: str, name: str = "") -> bool:
    if not email:
        return False
    domain = email.split("@")[-1].lower()
    if domain in _SENDING_DOMAIN_EXACT:
        return True
    if domain.endswith(".help"):
        return True
    if any(kw in domain for kw in _SENDING_DOMAIN_KEYWORDS):
        return True
    if name.strip().lower() in _SENDING_PERSONAS:
        return True
    return False


# ── Slack interactivity ─────────────────────────────────────────────────────

@app.post("/webhook/slack/actions")
async def slack_actions(request: Request, background: BackgroundTasks):
    body_bytes = await request.body()
    timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
    signature = request.headers.get("X-Slack-Signature", "")
    if not verify_slack_signature(body_bytes, timestamp, signature):
        return Response(status_code=403)

    form_data = await request.form()
    payload = json.loads(form_data.get("payload", "{}"))
    payload_type = payload.get("type")

    if payload_type == "block_actions":
        action = payload["actions"][0]
        action_id = action["action_id"]
        action_value = action["value"]
        trigger_id = payload.get("trigger_id")
        manager = payload["user"]["name"]
        channel = payload["channel"]["id"]
        message_ts = payload["message"]["ts"]

        if action_id == "approve_reply":
            background.add_task(_handle_approve, action_value, manager, channel, message_ts)
        elif action_id == "deny_edit_reply":
            pending = _get_pending(action_value)
            if pending:
                open_edit_modal(trigger_id, action_value, pending["ai_draft"])

        # ── Follow-up (reactivation / cadence) actions ──
        elif action_id == "approve_followup_send":
            background.add_task(
                _handle_followup_approve, action_value, manager, channel, message_ts
            )
        elif action_id == "deny_edit_followup_send":
            pending = get_pending_followup(action_value)
            if pending:
                open_edit_followup_send_modal(trigger_id, action_value, pending["draft"])
        elif action_id == "skip_followup":
            background.add_task(
                _handle_followup_skip, action_value, manager, channel, message_ts
            )

    elif payload_type == "view_submission":
        manager = payload["user"]["name"]
        callback_id = payload["view"].get("callback_id", "")

        if callback_id == "edit_followup_send_modal":
            record_id = payload["view"]["private_metadata"]
            edited_text = (
                payload["view"]["state"]["values"]
                .get("edited_followup", {})
                .get("followup_text", {})
                .get("value", "")
            )
            background.add_task(
                _handle_followup_edit_send, record_id, edited_text, manager
            )
        else:
            # Initial-reply edit modal (existing flow)
            email_id = payload["view"]["private_metadata"]
            edited_text = (
                payload["view"]["state"]["values"]
                .get("edited_response", {})
                .get("response_text", {})
                .get("value", "")
            )
            background.add_task(_handle_edit_send, email_id, edited_text, manager)

    return Response(status_code=200)


async def _handle_approve(email_id: str, manager: str, channel: str, message_ts: str):
    pending = _get_pending(email_id)
    if not pending:
        log.warning(f"No pending data for email_id: {email_id}")
        return
    reply_data = pending["reply"]
    draft = pending["ai_draft"]
    try:
        await send_reply(
            reply_to_id=email_id,
            subject=reply_data["subject"],
            from_email=reply_data["to_email"],
            to_email=reply_data["from_email"],
            body=draft,
        )
        update_message_approved(channel, message_ts, manager)
        _delete_pending(email_id)
        log.info(f"Approved and sent reply for {email_id} by {manager}")
    except Exception as e:
        log.exception(f"Failed to send approved reply: {e}")


async def _handle_edit_send(email_id: str, edited_text: str, manager: str):
    pending = _get_pending(email_id)
    if not pending:
        log.warning(f"No pending data for email_id: {email_id}")
        return
    reply_data = pending["reply"]
    channel = pending["slack_channel"]
    message_ts = pending["slack_ts"]
    try:
        await send_reply(
            reply_to_id=email_id,
            subject=reply_data["subject"],
            from_email=reply_data["to_email"],
            to_email=reply_data["from_email"],
            body=edited_text,
        )
        update_message_edited_sent(channel, message_ts, manager)
        _delete_pending(email_id)
        log.info(f"Edited and sent reply for {email_id} by {manager}")
    except Exception as e:
        log.exception(f"Failed to send edited reply: {e}")


# ── Follow-up (backlog + cadence) handlers ──────────────────────────────────

async def _handle_followup_approve(
    record_id: str, manager: str, channel: str, message_ts: str
) -> None:
    """Send an approved follow-up as-drafted, same thread, same mailbox."""
    pending = get_pending_followup(record_id)
    if not pending:
        log.warning(f"No pending follow-up for {record_id}")
        return
    try:
        await send_reply(
            reply_to_id=pending["reply_to_email_id"] or record_id,
            subject=pending["subject"] or "Follow-up",
            from_email=pending["sending_mailbox"],
            to_email=pending["prospect_email"],
            body=pending["draft"],
        )
        update_message_approved(channel, message_ts, manager)
        delete_pending_followup(record_id)
        log.info(
            f"follow-up sent to {pending['prospect_email']} by {manager} "
            f"(reactivation, mailbox={pending['sending_mailbox']})"
        )
    except Exception as e:
        log.exception(f"Failed to send follow-up for {record_id}: {e}")


async def _handle_followup_edit_send(
    record_id: str, edited_text: str, manager: str
) -> None:
    pending = get_pending_followup(record_id)
    if not pending:
        log.warning(f"No pending follow-up for {record_id}")
        return
    try:
        await send_reply(
            reply_to_id=pending["reply_to_email_id"] or record_id,
            subject=pending["subject"] or "Follow-up",
            from_email=pending["sending_mailbox"],
            to_email=pending["prospect_email"],
            body=edited_text,
        )
        # The original Slack card's channel/ts are stored inside pending.
        # We look them up via the ts we posted:
        from src.integrations.slack import client as _sc
        try:
            _sc.chat_update(
                channel=SLACK_CHANNEL_ID,
                ts=pending["slack_ts"],
                text=f"✏️ Edited & sent by {manager}",
                blocks=[{
                    "type": "section",
                    "text": {"type": "mrkdwn",
                             "text": f"✏️ Edited & sent by {manager}"},
                }],
            )
        except Exception:
            pass
        delete_pending_followup(record_id)
        log.info(f"follow-up edited+sent to {pending['prospect_email']} by {manager}")
    except Exception as e:
        log.exception(f"Failed to send edited follow-up for {record_id}: {e}")


async def _handle_followup_skip(
    record_id: str, manager: str, channel: str, message_ts: str
) -> None:
    pending = get_pending_followup(record_id)
    if pending:
        mark_skipped(pending["prospect_email"])
        delete_pending_followup(record_id)
    update_message_skipped(channel, message_ts, manager)
    log.info(f"follow-up skipped for {record_id} by {manager}")


# ── Beehiiv retry scheduler ─────────────────────────────────────────────────

async def _beehiiv_retry_scheduler():
    log.info("Beehiiv retry scheduler started")
    while True:
        try:
            await asyncio.sleep(24 * 60 * 60)
            counts = await process_retry_queue()
            log.info(
                f"Beehiiv daily retry: {counts['retried']} retried, "
                f"{counts['succeeded']} succeeded, {counts['still_failing']} still failing"
            )
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.exception(f"Beehiiv retry scheduler error: {e}")


# ── Unibox poller ────────────────────────────────────────────────────────────

async def _unibox_poller():
    """
    Pull the INTERESTED + MEETING_BOOKED buckets from PlusVibe unibox every
    2 min and dispatch each new one. This is the primary trigger source — the
    webhook is a redundant path in case PlusVibe delivers events out of band.

    Each email's unibox `id` is the dedup key. Redis is not required in this
    version; a lightweight in-process set survives poll cycles, and PlusVibe
    itself only surfaces INTERESTED / MEETING_BOOKED labels a bounded number
    of times.
    """
    log.info("Unibox poller started")
    seen: set[str] = set()
    dispatch_labels = {"INTERESTED", "MEETING_BOOKED"}
    await asyncio.sleep(30)  # let other startup tasks settle

    while True:
        try:
            emails: list[tuple[str, dict]] = []
            seen_ids: set[str] = set()
            for cid in ACTIVE_CAMPAIGNS:
                for label in ("INTERESTED", "MEETING_BOOKED"):
                    for e in await list_received_emails(cid, label=label):
                        eid = str(e.get("id") or "")
                        if eid and eid not in seen_ids:
                            seen_ids.add(eid)
                            emails.append((cid, e))

            processed = 0
            skipped = 0
            for cid, e in emails:
                eid = str(e.get("id") or "")
                if not eid or eid in seen:
                    continue
                seen.add(eid)  # dedup regardless of dispatch outcome
                label = (e.get("label") or "").upper()
                if label and label not in dispatch_labels:
                    skipped += 1
                    continue

                payload = _unibox_to_webhook_payload(e)
                # Enrich with lead_data (name / company / website).
                try:
                    ld = await get_lead_data(payload["data"]["email"], cid) or {}
                    payload["data"].update({
                        "first_name": ld.get("first_name") or "",
                        "last_name": ld.get("last_name") or "",
                        "company_name": ld.get("company_name") or "",
                        "company_website": ld.get("company_website") or "",
                    })
                except Exception:
                    log.exception(
                        f"lead_data lookup failed for {payload['data'].get('email')}"
                    )

                if label == "MEETING_BOOKED":
                    asyncio.create_task(_process_meeting_booked(payload))
                else:
                    asyncio.create_task(_process_reply(payload))
                processed += 1

            if processed or skipped:
                log.info(
                    f"Unibox poller: dispatched {processed}, "
                    f"skipped {skipped} (already-seen filter separate)"
                )

            # Cap the seen set to a sane size — 30 day TTL isn't needed since
            # the poller only asks for the latest INTERESTED/MEETING_BOOKED
            # bucket (bounded).
            if len(seen) > 5000:
                seen = set(list(seen)[-2500:])

            await asyncio.sleep(120)
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.exception(f"Unibox poller error: {e}")
            await asyncio.sleep(120)


def _unibox_to_webhook_payload(email: dict) -> dict:
    """Map a PlusVibe unibox email row into the webhook shape parse_webhook expects."""
    body = email.get("body") or {}
    body_html = ""
    if isinstance(body, dict):
        body_html = body.get("html") or body.get("text") or ""
    body_text = body_html or email.get("content_preview") or ""

    to_list = email.get("to_address_email_list") or []
    to_email = (to_list[0] if to_list else "") or email.get("eaccount") or ""

    label = (email.get("label") or "").upper()
    return {
        "event_type": (
            "MEETING_BOOKED" if label == "MEETING_BOOKED"
            else "LEAD_MARKED_AS_INTERESTED"
        ),
        "data": {
            "email_id": email.get("id") or "",
            "lead_id": email.get("lead_id") or "",
            "email": email.get("from_address_email") or email.get("lead") or "",
            "actual_replied_from": to_email,
            "campaign_id": email.get("campaign_id") or "",
            "campaign_name": email.get("campaign_name") or "",
            "last_lead_reply_subject": email.get("subject") or "",
            "last_lead_reply": body_text,
        },
    }


# ── Slack Events (URL verification only — CRM commands removed) ────────────

@app.post("/webhook/slack/events")
async def slack_events(request: Request):
    """
    Slack Events endpoint. Only URL verification is honoured in this version;
    the @mention CRM commands have been removed and will be rebuilt against
    the new spec.
    """
    body_bytes = await request.body()
    body = json.loads(body_bytes)
    if body.get("type") == "url_verification":
        return JSONResponse({"challenge": body["challenge"]})
    return Response(status_code=200)


# ── Admin ────────────────────────────────────────────────────────────────────

@app.get("/admin/workspaces")
async def admin_get_workspaces():
    from src.integrations.plusvibe import get_workspaces
    return await get_workspaces()


@app.post("/admin/register-webhook")
async def admin_register_webhook(request: Request):
    from src.integrations.plusvibe import register_webhook
    body = await request.json()
    railway_url = body.get("url")
    if not railway_url:
        return JSONResponse({"error": "url required"}, status_code=400)
    return await register_webhook(f"{railway_url}/webhook/plusvibe")


@app.post("/admin/retry-beehiiv")
async def admin_retry_beehiiv(background: BackgroundTasks):
    background.add_task(process_retry_queue)
    return {"status": "triggered"}


@app.post("/admin/test-slack")
async def admin_test_slack():
    from slack_sdk import WebClient
    sc = WebClient(token=os.getenv("SLACK_BOT_TOKEN", ""))
    try:
        r = sc.chat_postMessage(
            channel=SLACK_CHANNEL_ID,
            text="✅ Test message — inbox agent is alive.",
        )
        return {"ok": True, "ts": r["ts"], "channel": SLACK_CHANNEL_ID}
    except Exception as e:
        return {"ok": False, "error": str(e), "channel": SLACK_CHANNEL_ID}


@app.get("/admin/trendtrack-lookup")
async def admin_trendtrack_lookup(request: Request):
    """Diagnostic: resolve a domain or brand name through Trendtrack."""
    q = request.query_params.get("q", "")
    if not q:
        return JSONResponse({"error": "?q=<domain-or-name> required"}, status_code=400)
    result = await resolve_and_score(email_domain=q, brand_name_fallback=q)
    return result


@app.post("/admin/reprocess-last-webhook")
async def admin_reprocess_last(background: BackgroundTasks, request: Request):
    """
    Diagnostic: reprocess an arbitrary webhook payload as if PlusVibe had
    just delivered it. POST body is the raw webhook JSON.
    """
    body = await request.json()
    background.add_task(_process_reply, body)
    return {"status": "queued"}


# ── Follow-up (backlog reactivation) admin endpoints ────────────────────────

_scan_task: asyncio.Task | None = None


@app.post("/admin/backlog/scan")
async def admin_backlog_scan(request: Request):
    """
    Kick off a fresh backlog scan across the include-campaigns. Runs
    in-process (not backgrounded) so the response returns the actual size.

    Optional JSON: {"min_days": 8}. Default is 8 (past all normal follow-up
    windows, so a single reactivation touch is safe to send).
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    min_days = int((body or {}).get("min_days") or 8)
    log.info(f"backlog scan requested (min_days={min_days})")

    candidates = await scan_backlog(min_days_since_our_reply=min_days)
    enqueued = enqueue_candidates(candidates)

    preview = [
        {
            "prospect_email": c.prospect_email,
            "company_name": c.company_name,
            "campaign_name": c.campaign_name,
            "days_since_our_reply": c.days_since_our_reply,
            "our_last_reply_at": c.our_last_reply_at,
        }
        for c in candidates[:20]
    ]

    return {
        "candidates_found": len(candidates),
        "enqueued": enqueued,
        "batch_size": BATCH_SIZE,
        "preview": preview,
    }


@app.get("/admin/backlog/queue")
async def admin_backlog_peek():
    """Peek at the next 10 candidates in the queue without popping."""
    peek = peek_queue(10)
    return {
        "queue_size": queue_size(),
        "next_up": [
            {
                "prospect_email": c.prospect_email,
                "company_name": c.company_name,
                "days_since_our_reply": c.days_since_our_reply,
                "campaign_name": c.campaign_name,
            }
            for c in peek
        ],
    }


@app.post("/admin/backlog/next-batch")
async def admin_backlog_next_batch(request: Request):
    """
    Draft + post the next batch of follow-ups to Slack.

    Optional JSON: {"batch_size": 5}. Default matches BATCH_SIZE (5).
    Returns the leads that were posted.

    Recommended: call once, review the 5 Slack cards (approve / edit / skip),
    then call again for the next 5.
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    batch_size = int((body or {}).get("batch_size") or BATCH_SIZE)
    result = await post_next_batch(batch_size)
    return result
