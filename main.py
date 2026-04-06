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
from src.integrations.calendly import verify_calendly_signature, parse_calendly_event
from src.integrations.sheets import (
    TAB_PIPELINE, TAB_CALL_LOG, TAB_FOLLOW_UP,
    find_row_by_email, append_row, update_row,
    today_str, date_str, increment_field,
)
from src.crm_commands import parse_slack_command, execute_crm_command, add_nurture_schedule
from src.reports import generate_weekly_report, generate_monthly_report
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
    SLACK_CHANNEL_ID,
    SLACK_CHANNEL_AI_VISIBILITY,
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

# AI Visibility campaign modules
from src.ai_visibility_classifier import (
    classify_reply as aiv_classify_reply,
    get_reply_type_meta as aiv_get_reply_type_meta,
)
from src.ai_visibility_drafter import (
    draft_response as aiv_draft_response,
)

# Campaign name patterns for routing
AI_VISIBILITY_CAMPAIGNS = {"ai search visibility", "aeo", "featured in ai"}

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
    """Receive reply events from PlusVibe and trigger the processing pipeline."""
    body = await request.json()
    log.info(f"PlusVibe webhook received: {json.dumps(body)[:200]}")
    background.add_task(_process_reply, body)
    return JSONResponse({"status": "received"}, status_code=200)


def _is_ai_visibility_campaign(payload: dict) -> bool:
    """Check if the webhook payload belongs to an AI Search Visibility campaign."""
    data = payload.get("data", payload)
    campaign_name = (data.get("campaign_name") or "").lower().strip()
    return any(keyword in campaign_name for keyword in AI_VISIBILITY_CAMPAIGNS)


async def _process_reply(payload: dict):
    try:
        reply = parse_webhook(payload)
        is_aiv = _is_ai_visibility_campaign(payload)
        campaign_label = "AI Visibility" if is_aiv else "Email Marketing"
        target_channel = SLACK_CHANNEL_AI_VISIBILITY if is_aiv else SLACK_CHANNEL_ID
        log.info(f"[{campaign_label}] Processing reply from {reply.from_email} ({reply.company_name})")

        # 1. Subscribe to Beehiiv newsletter (fire and don't block)
        if reply.from_email:
            await subscribe_to_newsletter(
                email=reply.from_email,
                first_name=reply.first_name or "",
                last_name=reply.last_name or "",
            )

        # 2. Resolve email_id if not in webhook payload (needed to send reply)
        if not reply.email_id and reply.from_email:
            reply.email_id = await fetch_latest_email_id(reply.from_email) or ""
            log.info(f"Resolved email_id from unibox: {reply.email_id}")

        # Use lead_id as fallback identifier if email_id is still empty
        record_id = reply.email_id or reply.lead_id or reply.from_email
        if not record_id:
            log.error("No identifier available for this reply, skipping")
            return

        # 3. Classify (use campaign-specific classifier)
        if is_aiv:
            classification = await aiv_classify_reply(reply.body, reply.subject)
            reply_type = classification["reply_type"]
            meta = aiv_get_reply_type_meta(reply_type)
        else:
            classification = await classify_reply(reply.body, reply.subject)
            reply_type = classification["reply_type"]
            meta = get_reply_type_meta(reply_type)
        log.info(f"[{campaign_label}] Classified as: {reply_type} (confidence: {classification['confidence']})")

        # 3. Handle no-draft types immediately
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

        # 4. Scrape website if needed (email marketing only)
        category = "other"
        client_refs = []
        if not is_aiv and (meta.get("requires_scrape") or reply_type == "niche_experience"):
            category, client_refs = await scrape_and_classify(reply.website or "")
            log.info(f"Scraped category: {category}, clients: {client_refs}")

        # 5. Get few-shot examples
        few_shot_key = f"aiv:{reply_type}" if is_aiv else reply_type
        few_shots = get_few_shot_examples(few_shot_key)

        # 6. Draft response (use campaign-specific drafter)
        if is_aiv:
            draft = await aiv_draft_response(
                reply_type=reply_type,
                first_name=reply.first_name or "there",
                last_name=reply.last_name or "",
                company_name=reply.company_name or "your company",
                original_message=reply.body,
                few_shot_examples=few_shots,
            )
        else:
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
        log.info(f"[{campaign_label}] Draft created ({len(draft)} chars)")

        # 7. Post to Slack for review (campaign-specific channel)
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
            channel_override=target_channel,
        )
        log.info(f"[{campaign_label}] Posted to Slack: ts={slack_ts}")

        # 8. Store pending state for when manager approves
        store_pending(record_id, {
            "reply": reply.model_dump(),
            "reply_type": reply_type,
            "category": category,
            "campaign": "ai_visibility" if is_aiv else "email_marketing",
            "ai_draft": draft,
            "slack_ts": slack_ts,
            "slack_channel": target_channel,
        })

        # 9. Write to Google Sheets Pipeline (non-blocking, won't kill existing flow)
        try:
            await _write_to_pipeline(reply)
        except Exception as sheet_err:
            log.warning(f"Sheets Pipeline write failed (non-fatal): {sheet_err}")

    except Exception as e:
        log.exception(f"Error processing reply: {e}")


# ── Pipeline helper ───────────────────────────────────────────────────────────

async def _write_to_pipeline(reply) -> None:
    """Write or update a prospect in the Google Sheets Pipeline tab."""
    if not reply.from_email:
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


# ── Calendly webhook ──────────────────────────────────────────────────────────

@app.post("/webhook/calendly")
async def calendly_webhook(request: Request, background: BackgroundTasks):
    """Receive booking and cancellation events from Calendly."""
    body_bytes = await request.body()
    signature = request.headers.get("Calendly-Webhook-Signature", "")

    # URL verification challenge (Calendly sends this on first registration)
    try:
        body = json.loads(body_bytes)
    except Exception:
        return Response(status_code=400)

    if not verify_calendly_signature(body_bytes, signature):
        log.warning("Calendly signature verification failed")
        return Response(status_code=403)

    log.info(f"Calendly webhook received: {body.get('event')}")
    background.add_task(_process_calendly_event, body)
    return JSONResponse({"status": "received"}, status_code=200)


async def _process_calendly_event(payload: dict) -> None:
    try:
        event = parse_calendly_event(payload)
        if not event:
            return

        email = event["email"]
        name = event["name"]
        call_date = event["call_date"]
        call_time = event["call_time"]
        company = event["company"]

        from slack_sdk import WebClient
        slack = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))

        if event["event_type"] == "invitee.created":
            # Update Pipeline to Call Booked
            row_num, row_data = await find_row_by_email(TAB_PIPELINE, email)
            campaign = row_data.get("campaign", "") if row_data else ""
            company = company or (row_data.get("company", "") if row_data else "")

            if row_num:
                await update_row(TAB_PIPELINE, row_num, {
                    "stage": "Call Booked",
                    "last_touch": today_str(),
                    "next_followup": call_date,
                })

            # Append Call Log row (store email in Notes col M for cancellation lookup)
            await append_row(TAB_CALL_LOG, [
                name, call_date, call_time, company, campaign,
                "Booked", "", "", "", "", "", "", email,
            ])

            slack.chat_postMessage(
                channel=SLACK_CHANNEL_ID,
                text=f"Call booked: *{name}* ({company}) on {call_date} {call_time}",
            )
            log.info(f"Calendly: call booked for {email} on {call_date}")

        elif event["event_type"] == "invitee.canceled":
            today = today_str()
            next_followup = date_str(date.today() + timedelta(days=2))

            # Find and update Call Log row by email in Notes
            from src.integrations.sheets import read_all_rows
            call_rows = await read_all_rows(TAB_CALL_LOG)
            for r in reversed(call_rows):
                if email in r.get("notes", "").lower():
                    await update_row(TAB_CALL_LOG, r["_row_number"], {
                        "show_status": "Cancelled",
                        "next_step": "Reschedule",
                        "reschedule_date": today,
                    })
                    break

            # Revert Pipeline stage
            row_num, row_data = await find_row_by_email(TAB_PIPELINE, email)
            if row_num:
                company = company or (row_data.get("company", "") if row_data else "")
                await update_row(TAB_PIPELINE, row_num, {
                    "stage": "Positive Reply",
                    "last_touch": today,
                    "next_followup": next_followup,
                })

            # Add to Follow-Up Schedule
            await append_row(TAB_FOLLOW_UP, [
                name, company, email, "Call Booked", "Call Cancelled",
                "1", today, next_followup, "Personal Email", "Pending", "", "Pending", "",
            ])

            slack.chat_postMessage(
                channel=SLACK_CHANNEL_ID,
                text=f"Call cancelled: *{name}* ({company}) — follow-up scheduled for {next_followup}",
            )
            log.info(f"Calendly: call cancelled for {email}")

    except Exception as e:
        log.exception(f"Calendly event processing error: {e}")


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
        # Namespace few-shot keys by campaign
        is_aiv = pending.get("campaign") == "ai_visibility"
        log_reply_type = f"aiv:{pending['reply_type']}" if is_aiv else pending["reply_type"]
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
