import os
import hashlib
import hmac
import time
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

client = WebClient(token=SLACK_BOT_TOKEN)


def verify_slack_signature(body: bytes, timestamp: str, signature: str) -> bool:
    """Verify that an incoming request genuinely came from Slack."""
    if abs(time.time() - float(timestamp)) > 300:
        return False
    sig_basestring = f"v0:{timestamp}:{body.decode('utf-8')}"
    computed = "v0=" + hmac.new(
        SLACK_SIGNING_SECRET.encode(),
        sig_basestring.encode(),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(computed, signature)


def post_review_message(
    email_id: str,
    first_name: str,
    last_name: str,
    company_name: str,
    website: str,
    category: str,
    reply_type: str,
    original_message: str,
    draft_response: str,
    flag: bool = False,
    flag_reason: str = "",
) -> str:
    """
    Post a Block Kit review message to #inbox-review.
    Returns the message timestamp (ts) for later updates.
    """
    flag_note = f"\n\n:warning: *FLAG:* {flag_reason}" if flag else ""

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "📩 NEW REPLY — Requires Review"},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*👤 Prospect:*\n{first_name} {last_name}"},
                {"type": "mrkdwn", "text": f"*🏢 Company:*\n{company_name}"},
                {"type": "mrkdwn", "text": f"*🌐 Website:*\n{website or 'N/A'}"},
                {"type": "mrkdwn", "text": f"*🏷️ Category:*\n{category}"},
                {"type": "mrkdwn", "text": f"*📂 Reply Type:*\n{reply_type}"},
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*── Original Message ──*\n{original_message[:2000]}",
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*── Drafted Response ──*\n{draft_response}{flag_note}",
            },
        },
        {
            "type": "actions",
            "block_id": f"review_actions_{email_id}",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "✅ Approve"},
                    "style": "primary",
                    "action_id": "approve_reply",
                    "value": email_id,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "❌ Deny / Edit"},
                    "style": "danger",
                    "action_id": "deny_edit_reply",
                    "value": email_id,
                },
            ],
        },
    ]

    response = client.chat_postMessage(
        channel=SLACK_CHANNEL_ID,
        blocks=blocks,
        text=f"New reply from {first_name} {last_name} at {company_name} — requires review",
    )
    return response["ts"]


def open_edit_modal(trigger_id: str, email_id: str, current_draft: str):
    """Open a Slack modal for editing the draft before sending."""
    client.views_open(
        trigger_id=trigger_id,
        view={
            "type": "modal",
            "callback_id": f"edit_modal_{email_id}",
            "title": {"type": "plain_text", "text": "Edit Response"},
            "submit": {"type": "plain_text", "text": "Send"},
            "close": {"type": "plain_text", "text": "Cancel"},
            "private_metadata": email_id,
            "blocks": [
                {
                    "type": "input",
                    "block_id": "edited_response",
                    "label": {"type": "plain_text", "text": "Edit the response below:"},
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "response_text",
                        "multiline": True,
                        "initial_value": current_draft,
                    },
                }
            ],
        },
    )


def update_message_approved(channel: str, ts: str, manager: str):
    """Update the Slack message after approval."""
    _update_message_status(channel, ts, f"✅ Sent by {manager} at <!date^{int(time.time())}^{{time}}|now>")


def update_message_edited_sent(channel: str, ts: str, manager: str):
    """Update the Slack message after edit + send."""
    _update_message_status(channel, ts, f"✏️ Edited & Sent by {manager} at <!date^{int(time.time())}^{{time}}|now>")


def _update_message_status(channel: str, ts: str, status_text: str):
    try:
        client.chat_update(
            channel=channel,
            ts=ts,
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": status_text},
                }
            ],
            text=status_text,
        )
    except SlackApiError:
        pass


def post_unsubscribe_alert(first_name: str, last_name: str, company_name: str, from_email: str):
    """Post an urgent unsubscribe alert to Slack."""
    client.chat_postMessage(
        channel=SLACK_CHANNEL_ID,
        text=(
            f":rotating_light: *UNSUBSCRIBE REQUEST*\n"
            f"*{first_name} {last_name}* ({company_name}) — `{from_email}`\n"
            f"Process removal in PlusVibe immediately."
        ),
    )
