import json
import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, Request, Response, BackgroundTasks
from fastapi.responses import JSONResponse

from src.integrations.plusvibe import parse_webhook, send_reply
from src.integrations.slack import (
    verify_slack_signature,
    post_review_message,
    open_edit_modal,
    update_message_approved,
    update_message_edited_sent,
    post_unsubscribe_alert,
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Inbox Management Agent starting up")
    yield
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


async def _process_reply(payload: dict):
    try:
        reply = parse_webhook(payload)
        log.info(f"Processing reply from {reply.from_email} ({reply.company_name})")

        # 1. Classify
        classification = await classify_reply(reply.body, reply.subject)
        reply_type = classification["reply_type"]
        meta = get_reply_type_meta(reply_type)
        log.info(f"Classified as: {reply_type} (confidence: {classification['confidence']})")

        # 2. Handle no-draft types immediately
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

        # 3. Scrape website if needed
        category = "other"
        client_refs = []
        if meta.get("requires_scrape") or reply_type == "niche_experience":
            category, client_refs = await scrape_and_classify(reply.website or "")
            log.info(f"Scraped category: {category}, clients: {client_refs}")

        # 4. Get few-shot examples
        few_shots = get_few_shot_examples(reply_type)

        # 5. Draft response
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

        # 6. Post to Slack for review
        flag = meta.get("flag", False)
        flag_reason = ""
        if reply_type == "referral":
            flag_reason = "Referral — manual handling required"
        elif reply_type == "hostile":
            flag_reason = "Hostile reply — flagged for manager"
        elif reply_type == "uncategorised":
            flag_reason = "Uncategorised — potential new template"

        slack_ts = post_review_message(
            email_id=reply.email_id,
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

        # 7. Store pending state for when manager approves
        store_pending(reply.email_id, {
            "reply": reply.model_dump(),
            "reply_type": reply_type,
            "category": category,
            "ai_draft": draft,
            "slack_ts": slack_ts,
            "slack_channel": SLACK_CHANNEL_ID,
        })

    except Exception as e:
        log.exception(f"Error processing reply: {e}")


# ── Slack interactivity ───────────────────────────────────────────────────────

@app.post("/webhook/slack/actions")
async def slack_actions(request: Request, background: BackgroundTasks):
    """Handle Slack interactive component payloads (button clicks, modal submissions)."""
    # Verify signature
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
        email_id = action["value"]
        trigger_id = payload.get("trigger_id")
        manager = payload["user"]["name"]
        channel = payload["channel"]["id"]
        message_ts = payload["message"]["ts"]

        if action_id == "approve_reply":
            background.add_task(_handle_approve, email_id, manager, channel, message_ts)

        elif action_id == "deny_edit_reply":
            pending = get_pending(email_id)
            if pending:
                open_edit_modal(trigger_id, email_id, pending["ai_draft"])

    elif payload_type == "view_submission":
        email_id = payload["view"]["private_metadata"]
        edited_text = (
            payload["view"]["state"]["values"]
            .get("edited_response", {})
            .get("response_text", {})
            .get("value", "")
        )
        manager = payload["user"]["name"]
        background.add_task(_handle_edit_send, email_id, edited_text, manager)

    return Response(status_code=200)


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
        log_interaction(
            email_id=email_id,
            prospect_email=reply_data["from_email"],
            prospect_company=reply_data.get("company_name", ""),
            prospect_category=pending.get("category", "other"),
            reply_type=pending["reply_type"],
            ai_draft=draft,
            final_sent=draft,
            was_edited=False,
            manager=manager,
            slack_ts=pending["slack_ts"],
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
        log_interaction(
            email_id=email_id,
            prospect_email=reply_data["from_email"],
            prospect_company=reply_data.get("company_name", ""),
            prospect_category=pending.get("category", "other"),
            reply_type=pending["reply_type"],
            ai_draft=draft,
            final_sent=edited_text,
            was_edited=True,
            manager=manager,
            slack_ts=message_ts,
        )
        delete_pending(email_id)
        log.info(f"Edited and sent reply for {email_id} by {manager}")
    except Exception as e:
        log.exception(f"Failed to send edited reply: {e}")


# ── Admin utilities ────────────────────────────────────────────────────────────

@app.get("/admin/workspaces")
async def get_workspaces():
    """Utility endpoint to discover your PlusVibe workspace ID."""
    from src.integrations.plusvibe import get_workspaces
    data = await get_workspaces()
    return data


@app.post("/admin/register-webhook")
async def register_webhook(request: Request):
    """Register this server's URL as a PlusVibe webhook."""
    from src.integrations.plusvibe import register_webhook
    body = await request.json()
    railway_url = body.get("url")
    if not railway_url:
        return JSONResponse({"error": "url required"}, status_code=400)
    result = await register_webhook(f"{railway_url}/webhook/plusvibe")
    return result
