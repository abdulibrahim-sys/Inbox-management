import asyncio
import calendar as cal_module
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, timedelta
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, Request, Response, BackgroundTasks
from fastapi.responses import JSONResponse

from src.integrations.plusvibe import (
    parse_webhook, send_reply, fetch_latest_email_id,
    get_email_thread, save_draft,
)
from src.integrations.beehiiv import subscribe_to_newsletter, process_retry_queue
from src.integrations.sheets import (
    TAB_PIPELINE, TAB_CALL_LOG, TAB_FOLLOW_UP,
    find_row_by_email, append_row, update_row,
    today_str, date_str, increment_field,
)
from src.crm_commands import parse_slack_command, execute_crm_command, add_nurture_schedule
from src.reports import generate_weekly_report, generate_monthly_report
from src.deliverability import check_deliverability
from src.integrations.slack import (
    verify_slack_signature,
    post_review_message,
    open_edit_modal,
    update_message_approved,
    update_message_edited_sent,
    post_unsubscribe_alert,
    post_followup_review,
    open_followup_edit_modal,
    update_message_draft_saved,
    update_message_cancelled,
    post_call_booked_message,
    update_call_outcome_message,
    SLACK_CHANNEL_ID,
)
from src.classifier import classify_reply, get_reply_type_meta
from src.drafter import draft_response, compute_diff
from src.scraper import scrape_and_classify
from src.learning import (
    log_interaction,
    store_pending,
    get_pending,
    delete_pending,
    get_few_shot_examples,
)
from src.followup import (
    schedule_followups,
    cancel_followups,
    get_due_followups,
    advance_stage,
    draft_followup,
    format_thread_context,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

_followup_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _followup_task
    log.info("Inbox Management Agent starting up")
    _followup_task = asyncio.create_task(_followup_scheduler())
    asyncio.create_task(_beehiiv_retry_scheduler())
    asyncio.create_task(_reports_scheduler())
    yield
    _followup_task.cancel()
    log.info("Inbox Management Agent shutting down")


app = FastAPI(title="Inbox Management Agent", lifespan=lifespan)


# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


# ── PlusVibe webhook ──────────────────────────────────────────────────────────

@app.post("/webhook/plusvibe")
async def plusvibe_webhook(request: Request, background: BackgroundTasks):
    """Receive events from PlusVibe — reply events and meeting booked tags."""
    body = await request.json()
    log.info(f"PlusVibe webhook received: {json.dumps(body)[:200]}")

    if _is_meeting_booked(body):
        background.add_task(_process_meeting_booked, body)
    else:
        background.add_task(_process_reply, body)

    return JSONResponse({"status": "received"}, status_code=200)


def _is_meeting_booked(payload: dict) -> bool:
    """Detect a 'Meeting booked' tag event from PlusVibe."""
    data = payload.get("data", payload)
    event_type = (payload.get("event_type") or payload.get("event") or "").lower()
    tag = (data.get("tag") or data.get("label") or data.get("tag_name") or "").lower()
    return (
        "meeting" in event_type
        or "booked" in event_type
        or tag == "meeting booked"
        or "meeting booked" in tag
    )


async def _process_reply(payload: dict):
    try:
        reply = parse_webhook(payload)
        log.info(f"Processing reply from {reply.from_email} ({reply.company_name})")

        # 1. Subscribe to Beehiiv newsletter
        if reply.from_email:
            await subscribe_to_newsletter(
                email=reply.from_email,
                first_name=reply.first_name or "",
                last_name=reply.last_name or "",
            )

        # 2. Resolve email_id if not in webhook payload
        if not reply.email_id and reply.from_email:
            reply.email_id = await fetch_latest_email_id(reply.from_email) or ""
            log.info(f"Resolved email_id from unibox: {reply.email_id}")

        record_id = reply.email_id or reply.lead_id or reply.from_email
        if not record_id:
            log.error("No identifier available for this reply, skipping")
            return

        # 3. Classify
        classification = await classify_reply(reply.body, reply.subject)
        reply_type = classification["reply_type"]
        meta = get_reply_type_meta(reply_type)
        log.info(f"Classified as: {reply_type} (confidence: {classification['confidence']})")

        # 4. Handle no-draft types immediately
        if meta.get("no_draft"):
            if reply_type == "unsubscribe":
                post_unsubscribe_alert(
                    reply.first_name or "",
                    reply.last_name or "",
                    reply.company_name or "",
                    reply.from_email,
                )
            else:
                log.info(f"Skipping draft for reply type: {reply_type}")
            return

        # 5. Scrape website if needed
        category = "other"
        client_refs = []
        if meta.get("requires_scrape") or reply_type == "niche_experience":
            category, client_refs = await scrape_and_classify(reply.website or "")
            log.info(f"Scraped category: {category}, clients: {client_refs}")

        # 6. Get few-shot examples
        few_shots = get_few_shot_examples(reply_type)

        # 7. Draft response
        draft = await draft_response(
            reply_type=reply_type,
            first_name=reply.first_name or "there",
            last_name=reply.last_name or "",
            company_name=reply.company_name or "your company",
            original_message=reply.body,
            category=category,
            client_references=client_refs,
            few_shot_examples=few_shots,
        )
        log.info(f"Draft created ({len(draft)} chars)")

        # 8. Post to Slack for review
        flag = meta.get("flag", False)
        flag_reason = ""
        if reply_type == "referral":
            flag_reason = "Referral — manual handling required"
        elif reply_type == "hostile":
            flag_reason = "Hostile reply — flagged for manager"
        elif reply_type == "uncategorised":
            flag_reason = "Uncategorised — potential new template"

        slack_ts = post_review_message(
            email_id=record_id,
            first_name=reply.first_name or "",
            last_name=reply.last_name or "",
            company_name=reply.company_name or "",
            website=reply.website or "",
            category=category,
            reply_type=reply_type,
            original_message=reply.body,
            draft_response=draft,
            flag=flag,
            flag_reason=flag_reason,
        )
        log.info(f"Posted to Slack: ts={slack_ts}")

        # 9. Store pending state for when manager approves
        store_pending(record_id, {
            "reply": reply.model_dump(),
            "reply_type": reply_type,
            "category": category,
            "campaign": "email_marketing",
            "ai_draft": draft,
            "slack_ts": slack_ts,
            "slack_channel": SLACK_CHANNEL_ID,
        })

        # 9. Write to Google Sheets Pipeline (non-blocking, won't kill existing flow)
        try:
            await _write_to_pipeline(reply)
        except Exception as sheet_err:
            log.warning(f"Sheets Pipeline write failed (non-fatal): {sheet_err}")

    except Exception as e:
        log.exception(f"Error processing reply: {e}")


# ── Pipeline helper ───────────────────────────────────────────────────────────

_SENDING_DOMAIN_KEYWORDS = {"trendfeed", "trendsender", "trendconnect", "trendreach", "hiretrendfeed", "gettrendfeed", "jointrendfeed", "trytrendfeed"}
_SENDING_DOMAIN_EXACT = {"trendfeed.co.uk"}
_SENDING_PERSONAS = {"elena clifford", "mina willard", "riley marchmain", "julia milton", "hanna raymond", "kendall hollinghurst", "crystal rosewood", "erin whitfield", "bethany cranston", "lacey northcott", "raymond hanna"}


def _is_sending_account(email: str, name: str = "") -> bool:
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


async def _write_to_pipeline(reply) -> None:
    """Write or update a prospect in the Google Sheets Pipeline tab."""
    if not reply.from_email:
        return
    # Skip our own sending accounts
    full_name = f"{reply.first_name or ''} {reply.last_name or ''}".strip()
    if _is_sending_account(reply.from_email, full_name):
        log.info(f"Pipeline: skipping sending account {reply.from_email}")
        return
    # Skip if no company name (likely a sending account with unknown domain)
    if not reply.company_name:
        log.info(f"Pipeline: skipping {reply.from_email} — no company name")
        return

    today = today_str()
    next_followup = date_str(date.today() + timedelta(days=2))

    row_num, row_data = await find_row_by_email(TAB_PIPELINE, reply.from_email)

    if row_num is None:
        # New prospect — append row (columns A-R, leave formula cols empty)
        await append_row(TAB_PIPELINE, [
            reply.first_name or "" + " " + (reply.last_name or ""),  # A: Name
            reply.company_name or "",                                  # B: Company
            reply.from_email,                                          # C: Email
            today,                                                     # D: Date Added
            reply.campaign_name or "",                                 # E: Campaign
            "",                                                        # F: Industry (manual)
            "",                                                        # G: Company Size (manual)
            "Positive Reply",                                          # H: Stage
            "Positive",                                                # I: Sentiment
            today,                                                     # J: Reply Date
            "",                                                        # K: Deal Value
            "",                                                        # L: Assigned To
            today,                                                     # M: Last Touch
            next_followup,                                             # N: Next Follow-Up
            "1",                                                       # O: Follow-Up Count
            "",                                                        # P: Days in Stage (formula)
            "",                                                        # Q: Loss Reason
            "",                                                        # R: Notes
        ])
        log.info(f"Pipeline: new row added for {reply.from_email}")
    else:
        # Existing prospect — update stage, sentiment, dates, increment count
        current_count = int(row_data.get("followup_count", "0") or "0")
        await update_row(TAB_PIPELINE, row_num, {
            "stage": "Positive Reply",
            "sentiment": "Positive",
            "reply_date": today,
            "last_touch": today,
            "next_followup": next_followup,
            "followup_count": str(current_count + 1),
        })
        log.info(f"Pipeline: existing row updated for {reply.from_email}")


# ── PlusVibe meeting booked handler ──────────────────────────────────────────

async def _process_meeting_booked(payload: dict) -> None:
    """Handle a 'Meeting booked' tag event from PlusVibe."""
    try:
        data = payload.get("data", payload)
        email = (data.get("email") or data.get("from_address") or "").strip().lower()
        first_name = data.get("first_name") or ""
        last_name = data.get("last_name") or ""
        name = f"{first_name} {last_name}".strip() or email
        campaign = data.get("campaign_name") or ""

        if _is_sending_account(email, name):
            log.info(f"Meeting booked: skipping sending account {email}")
            return
        today = today_str()

        # Pull extra context from Pipeline row
        row_num, row_data = await find_row_by_email(TAB_PIPELINE, email)
        company = data.get("company_name") or (row_data.get("company") if row_data else "") or ""

        # Update Pipeline stage
        if row_num:
            await update_row(TAB_PIPELINE, row_num, {
                "stage": "Call Booked",
                "last_touch": today,
            })
            log.info(f"Pipeline: {email} → Call Booked")
        else:
            # Not in pipeline yet — add them
            await append_row(TAB_PIPELINE, [
                name, company, email, today, campaign,
                "", "", "Call Booked", "Positive", today,
                "", "", today, "", "0", "", "", "",
            ])
            log.info(f"Pipeline: new row added for {email} (meeting booked)")

        # Append Call Log row
        await append_row(TAB_CALL_LOG, [
            name, today, "", company, campaign,
            "Booked", "", "", "", "", "", "", email,
        ])

        # Post to Slack with outcome buttons
        post_call_booked_message(name=name, company=company, email=email, campaign=campaign)
        log.info(f"Meeting booked processed for {email}")

    except Exception as e:
        log.exception(f"Meeting booked processing error: {e}")


# ── Call outcome handler ─────────────────────────────────────────────────────

async def _handle_call_outcome(action_value: str, outcome: str, manager: str, channel: str, message_ts: str):
    """Handle Showed / No Show / Not Qualified button clicks."""
    try:
        data = json.loads(action_value)
        email = data["email"]
        name = data["name"]
        company = data["company"]
        today = today_str()

        # Update Call Log show_status
        call_rows = await read_all_rows(TAB_CALL_LOG)
        for r in reversed(call_rows):
            notes = r.get("notes", "").lower()
            if email.lower() in notes:
                updates = {"show_status": outcome, "next_step": "Follow up" if outcome == "Showed" else "Nurture"}
                await update_row(TAB_CALL_LOG, r["_row_number"], updates)
                break

        # Update Pipeline
        row_num, row_data = await find_row_by_email(TAB_PIPELINE, email)
        if row_num:
            if outcome == "Showed":
                await update_row(TAB_PIPELINE, row_num, {"stage": "Showed", "last_touch": today})
            elif outcome == "No Show":
                await update_row(TAB_PIPELINE, row_num, {"stage": "No Show", "last_touch": today})
                await add_nurture_schedule(email, name, company, "Call Booked", "No Show")
            elif outcome == "Not Qualified":
                await update_row(TAB_PIPELINE, row_num, {"stage": "Closed Lost", "loss_reason": "Not Qualified", "last_touch": today})

        # Update the Slack message
        update_call_outcome_message(channel, message_ts, name, company, outcome, manager)
        log.info(f"Call outcome '{outcome}' logged for {email} by {manager}")

    except Exception as e:
        log.exception(f"Call outcome handler error: {e}")


# ── Slack interactivity ───────────────────────────────────────────────────────

@app.post("/webhook/slack/actions")
async def slack_actions(request: Request, background: BackgroundTasks):
    """Handle Slack interactive component payloads (button clicks, modal submissions)."""
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

        # ── Initial reply actions ──
        if action_id == "approve_reply":
            background.add_task(_handle_approve, action_value, manager, channel, message_ts)

        elif action_id == "deny_edit_reply":
            pending = get_pending(action_value)
            if pending:
                open_edit_modal(trigger_id, action_value, pending["ai_draft"])

        # ── Call outcome actions ──
        elif action_id in ("call_showed", "call_no_show", "call_not_qualified"):
            outcome_map = {
                "call_showed": "Showed",
                "call_no_show": "No Show",
                "call_not_qualified": "Not Qualified",
            }
            outcome = outcome_map[action_id]
            background.add_task(_handle_call_outcome, action_value, outcome, manager, channel, message_ts)

        # ── Follow-up actions ──
        elif action_id == "approve_followup":
            background.add_task(_handle_followup_approve, action_value, manager, channel, message_ts)

        elif action_id == "deny_edit_followup":
            data = json.loads(action_value)
            pending = get_pending(f"followup:{data['lead_email']}:{data['stage']}")
            if pending:
                open_followup_edit_modal(trigger_id, data["lead_email"], data["stage"], pending["ai_draft"])

        elif action_id == "cancel_followup":
            cancel_followups(action_value)
            update_message_cancelled(channel, message_ts, manager)

    elif payload_type == "view_submission":
        callback_id = payload["view"].get("callback_id", "")
        manager = payload["user"]["name"]

        if callback_id == "edit_followup_modal":
            # Follow-up edit submission
            meta = json.loads(payload["view"]["private_metadata"])
            edited_text = (
                payload["view"]["state"]["values"]
                .get("edited_followup", {})
                .get("followup_text", {})
                .get("value", "")
            )
            background.add_task(
                _handle_followup_edit_approve,
                meta["lead_email"], meta["stage"], edited_text, manager,
            )
        else:
            # Initial reply edit submission
            email_id = payload["view"]["private_metadata"]
            edited_text = (
                payload["view"]["state"]["values"]
                .get("edited_response", {})
                .get("response_text", {})
                .get("value", "")
            )
            background.add_task(_handle_edit_send, email_id, edited_text, manager)

    return Response(status_code=200)


# ── Initial reply handlers ────────────────────────────────────────────────────

async def _handle_approve(email_id: str, manager: str, channel: str, message_ts: str):
    pending = get_pending(email_id)
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
        log_reply_type = pending["reply_type"]
        log_interaction(
            email_id=email_id,
            prospect_email=reply_data["from_email"],
            prospect_company=reply_data.get("company_name", ""),
            prospect_category=pending.get("category", "other"),
            reply_type=log_reply_type,
            ai_draft=draft,
            final_sent=draft,
            was_edited=False,
            manager=manager,
            slack_ts=pending["slack_ts"],
        )

        # Schedule follow-ups after successful send
        schedule_followups(
            record_id=email_id,
            lead_email=reply_data["from_email"],
            lead_first_name=reply_data.get("first_name") or "",
            lead_last_name=reply_data.get("last_name") or "",
            company_name=reply_data.get("company_name") or "",
            from_email=reply_data["to_email"],
            subject=reply_data["subject"],
            category=pending.get("category", "other"),
        )

        delete_pending(email_id)
        log.info(f"Approved and sent reply for {email_id} by {manager}")
    except Exception as e:
        log.exception(f"Failed to send approved reply: {e}")


async def _handle_edit_send(email_id: str, edited_text: str, manager: str):
    pending = get_pending(email_id)
    if not pending:
        log.warning(f"No pending data for email_id: {email_id}")
        return

    reply_data = pending["reply"]
    draft = pending["ai_draft"]
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
        is_aiv = pending.get("campaign") == "ai_visibility"
        log_reply_type = f"aiv:{pending['reply_type']}" if is_aiv else pending["reply_type"]
        log_interaction(
            email_id=email_id,
            prospect_email=reply_data["from_email"],
            prospect_company=reply_data.get("company_name", ""),
            prospect_category=pending.get("category", "other"),
            reply_type=log_reply_type,
            ai_draft=draft,
            final_sent=edited_text,
            was_edited=True,
            manager=manager,
            slack_ts=message_ts,
        )

        # Schedule follow-ups after successful send
        schedule_followups(
            record_id=email_id,
            lead_email=reply_data["from_email"],
            lead_first_name=reply_data.get("first_name") or "",
            lead_last_name=reply_data.get("last_name") or "",
            company_name=reply_data.get("company_name") or "",
            from_email=reply_data["to_email"],
            subject=reply_data["subject"],
            category=pending.get("category", "other"),
        )

        delete_pending(email_id)
        log.info(f"Edited and sent reply for {email_id} by {manager}")
    except Exception as e:
        log.exception(f"Failed to send edited reply: {e}")


# ── Follow-up handlers ────────────────────────────────────────────────────────

async def _handle_followup_approve(action_value: str, manager: str, channel: str, message_ts: str):
    """Approve a follow-up: save as draft in PlusVibe."""
    data = json.loads(action_value)
    lead_email = data["lead_email"]
    stage = data["stage"]

    pending = get_pending(f"followup:{lead_email}:{stage}")
    if not pending:
        log.warning(f"No pending follow-up data for {lead_email} stage {stage}")
        return

    try:
        await save_draft(
            parent_message_id=pending["parent_message_id"],
            from_email=pending["from_email"],
            subject=pending["subject"],
            body=pending["ai_draft"],
        )
        update_message_draft_saved(channel, message_ts, manager)
        advance_stage(lead_email)
        delete_pending(f"followup:{lead_email}:{stage}")
        log.info(f"Follow-up #{stage} draft saved for {lead_email} by {manager}")
    except Exception as e:
        log.exception(f"Failed to save follow-up draft: {e}")


async def _handle_followup_edit_approve(lead_email: str, stage: int, edited_text: str, manager: str):
    """Save an edited follow-up as draft in PlusVibe."""
    pending = get_pending(f"followup:{lead_email}:{stage}")
    if not pending:
        log.warning(f"No pending follow-up data for {lead_email} stage {stage}")
        return

    try:
        await save_draft(
            parent_message_id=pending["parent_message_id"],
            from_email=pending["from_email"],
            subject=pending["subject"],
            body=edited_text,
        )
        # Update original Slack message
        update_message_draft_saved(pending.get("slack_channel", SLACK_CHANNEL_ID), pending["slack_ts"], manager)
        advance_stage(lead_email)
        delete_pending(f"followup:{lead_email}:{stage}")
        log.info(f"Edited follow-up #{stage} draft saved for {lead_email} by {manager}")
    except Exception as e:
        log.exception(f"Failed to save edited follow-up draft: {e}")


# ── Follow-up scheduler ──────────────────────────────────────────────────────

async def _followup_scheduler():
    """Background task that checks for due follow-ups every 30 minutes."""
    log.info("Follow-up scheduler started")
    while True:
        try:
            await asyncio.sleep(30 * 60)  # Check every 30 minutes
            await _process_due_followups()
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.exception(f"Follow-up scheduler error: {e}")


async def _process_due_followups():
    """Check for and process all due follow-ups."""
    due = await get_due_followups()
    log.info(f"Follow-up check: {len(due)} due")

    for item in due:
        try:
            lead_email = item["lead_email"]
            stage = item["due_stage"]

            # Fetch email thread for context
            thread = await get_email_thread(lead_email)
            thread_context = format_thread_context(thread)

            # Find the parent message_id (most recent email in thread)
            parent_message_id = ""
            if thread:
                parent_message_id = thread[0].get("message_id") or thread[0].get("id") or ""

            # Draft the follow-up
            draft = await draft_followup(
                stage=stage,
                first_name=item["first_name"],
                company_name=item["company_name"],
                thread_context=thread_context,
                category=item.get("category", "other"),
            )
            log.info(f"Follow-up #{stage} drafted for {lead_email} ({len(draft)} chars)")

            # Post to Slack for review
            slack_ts = post_followup_review(
                lead_email=lead_email,
                first_name=item["first_name"],
                last_name=item["last_name"],
                company_name=item["company_name"],
                stage=stage,
                draft_followup=draft,
                thread_summary=thread_context[:800],
            )

            # Store pending follow-up for approval
            store_pending(f"followup:{lead_email}:{stage}", {
                "lead_email": lead_email,
                "stage": stage,
                "from_email": item["from_email"],
                "subject": item["subject"],
                "parent_message_id": parent_message_id,
                "ai_draft": draft,
                "slack_ts": slack_ts,
                "slack_channel": SLACK_CHANNEL_ID,
            })

        except Exception as e:
            log.exception(f"Failed to process follow-up for {item.get('lead_email')}: {e}")


# ── Beehiiv retry scheduler ───────────────────────────────────────────────────

async def _beehiiv_retry_scheduler():
    """Background task that retries failed Beehiiv subscriptions every 24 hours."""
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


# ── Reports scheduler ────────────────────────────────────────────────────────

async def _reports_scheduler():
    """Background task that fires weekly (Friday 7pm EST) and monthly (last day 7pm EST) reports."""
    log.info("Reports scheduler started")
    EST = ZoneInfo("America/New_York")
    while True:
        try:
            await asyncio.sleep(60 * 60)  # check once per hour
            now = date.today()
            import datetime as _dt
            now_dt = _dt.datetime.now(EST)

            # Weekly: Friday (weekday 4) at 19:xx
            if now_dt.weekday() == 4 and now_dt.hour == 19:
                await generate_weekly_report()

            # Deliverability check: Monday (weekday 0) at 09:xx
            if now_dt.weekday() == 0 and now_dt.hour == 9:
                await check_deliverability()

            # Monthly: last day of month at 19:xx
            last_day = cal_module.monthrange(now.year, now.month)[1]
            if now.day == last_day and now_dt.hour == 19:
                await generate_monthly_report()

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.exception(f"Reports scheduler error: {e}")


# ── Slack Events API (bot @mentions for CRM commands) ────────────────────────

@app.post("/webhook/slack/events")
async def slack_events(request: Request, background: BackgroundTasks):
    """Handle Slack Events API payloads (URL verification + app_mention)."""
    body_bytes = await request.body()
    body = json.loads(body_bytes)

    # Slack URL verification challenge (no sig check needed)
    if body.get("type") == "url_verification":
        return JSONResponse({"challenge": body["challenge"]})

    # Signature verification
    timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
    signature = request.headers.get("X-Slack-Signature", "")
    if not verify_slack_signature(body_bytes, timestamp, signature):
        return Response(status_code=403)

    event = body.get("event", {})
    if event.get("type") == "app_mention":
        background.add_task(_handle_crm_command, event)

    return Response(status_code=200)


async def _handle_crm_command(event: dict):
    """Parse and execute a CRM command from a Slack @mention."""
    text = event.get("text", "")
    channel = event.get("channel", "")
    thread_ts = event.get("thread_ts") or event.get("ts", "")

    try:
        parsed = await parse_slack_command(text)
        result = await execute_crm_command(parsed)

        from slack_sdk import WebClient
        slack = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))
        slack.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=result.get("message", "Done."),
        )
        log.info(f"CRM command executed: {parsed.get('intent')} — {result.get('message', '')[:80]}")
    except Exception as e:
        log.exception(f"CRM command handler error: {e}")


# ── Admin utilities ────────────────────────────────────────────────────────────

@app.get("/admin/workspaces")
async def admin_get_workspaces():
    """Utility endpoint to discover your PlusVibe workspace ID."""
    from src.integrations.plusvibe import get_workspaces
    data = await get_workspaces()
    return data


@app.post("/admin/register-webhook")
async def admin_register_webhook(request: Request):
    """Register this server's URL as a PlusVibe webhook."""
    from src.integrations.plusvibe import register_webhook
    body = await request.json()
    railway_url = body.get("url")
    if not railway_url:
        return JSONResponse({"error": "url required"}, status_code=400)
    result = await register_webhook(f"{railway_url}/webhook/plusvibe")
    return result


@app.post("/admin/check-followups")
async def admin_check_followups(background: BackgroundTasks):
    """Manually trigger a follow-up check (for testing)."""
    background.add_task(_process_due_followups)
    return {"status": "triggered"}


@app.post("/admin/check-deliverability")
async def admin_check_deliverability(background: BackgroundTasks):
    """Manually trigger a deliverability check."""
    background.add_task(check_deliverability)
    return {"status": "triggered"}


@app.post("/admin/retry-beehiiv")
async def admin_retry_beehiiv(background: BackgroundTasks):
    """Manually trigger a Beehiiv retry run."""
    background.add_task(process_retry_queue)
    return {"status": "triggered"}


@app.get("/admin/beehiiv-queue")
async def admin_beehiiv_queue():
    """Show leads currently in the Beehiiv retry queue."""
    from src.integrations.beehiiv import get_retry_queue
    queue = get_retry_queue()
    return {"count": len(queue), "leads": queue}
