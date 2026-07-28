import json
import os
import hashlib
import hmac
import time
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")
SLACK_CHANNEL_AI_VISIBILITY = os.getenv("SLACK_CHANNEL_AI_VISIBILITY", "")

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


def _summary_line(prospect_email: str, company_name: str = "") -> str:
    """Slim one-line summary for the top of every review card."""
    brand_line = company_name.strip() or "(unknown brand)"
    domain = prospect_email.rsplit("@", 1)[-1] if "@" in prospect_email else ""
    return (
        f"*✉️* `{prospect_email}`\n"
        f"*🏬* {brand_line}"
        + (f"  _({domain})_" if domain and domain not in brand_line else "")
    )


def post_review_message(
    email_id: str,
    first_name: str,
    last_name: str,
    company_name: str,
    prospect_email: str,
    intent_n: int,
    intent_name: str,
    original_message: str,
    draft_response: str,
    channel_override: str = "",
) -> str:
    """
    Draft-for-approval Slack card. Simplified — no Trendtrack intel; the
    card shows only what we know from the PlusVibe lead record.

    Returns the message timestamp for later updates.
    """
    name_line = f"{first_name} {last_name}".strip() or "(unknown)"
    brand_title = company_name or "(unknown brand)"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text",
                     "text": f"💬 New reply to approve — {brand_title[:100]}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": _summary_line(prospect_email, company_name)},
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn",
                 "text": f"*👤 Prospect:* {name_line}   "
                         f"*🧠 Intent:* {intent_n}. {intent_name}"},
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Original message*\n{original_message[:2000]}",
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Drafted reply*\n{draft_response}",
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

    target_channel = channel_override or SLACK_CHANNEL_ID
    response = client.chat_postMessage(
        channel=target_channel,
        blocks=blocks,
        text=f"New reply from {name_line} at {brand_title} — requires review",
    )
    return response["ts"]


def post_disregard_notification(
    first_name: str,
    last_name: str,
    company_name: str,
    prospect_email: str,
    intent_n: int,
    intent_name: str,
    original_message: str,
    reason: str,
    channel_override: str = "",
) -> str:
    """Disregard notification — no reply drafted, tells the approver why."""
    name_line = f"{first_name} {last_name}".strip() or "(unknown)"
    brand_title = company_name or "(unknown brand)"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text",
                     "text": f"🚫 Disregarded, no reply drafted — {brand_title[:100]}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": _summary_line(prospect_email, company_name)},
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn",
                 "text": f"*👤 Prospect:* {name_line}   "
                         f"*🧠 Intent:* {intent_n}. {intent_name}"},
            ],
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": f"*They said*\n{original_message[:1500]}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"⚠️  *Why:* {reason}"},
        },
    ]
    target_channel = channel_override or SLACK_CHANNEL_ID
    response = client.chat_postMessage(
        channel=target_channel,
        blocks=blocks,
        text=f"Disregarded — {name_line} ({brand_title})",
    )
    return response["ts"]


def post_escalation_message(
    first_name: str,
    last_name: str,
    company_name: str,
    prospect_email: str,
    intent_n: int,
    intent_name: str,
    original_message: str,
    reason: str,
    channel_override: str = "",
) -> str:
    """Escalation — needs a human."""
    name_line = f"{first_name} {last_name}".strip() or "(unknown)"
    brand_title = company_name or "(unknown brand)"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text",
                     "text": f"🚨 Needs a human — {brand_title[:100]}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": _summary_line(prospect_email, company_name)},
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn",
                 "text": f"*👤 Prospect:* {name_line}   "
                         f"*🧠 Intent:* {intent_n}. {intent_name}"},
            ],
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": f"*They said*\n{original_message[:1500]}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": f"⚠️  *Why I stopped:* {reason}"},
        },
    ]
    target_channel = channel_override or SLACK_CHANNEL_ID
    response = client.chat_postMessage(
        channel=target_channel,
        blocks=blocks,
        text=f"Needs a human — {name_line} ({brand_title})",
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


def post_call_booked_message(name: str, company: str, email: str, campaign: str = "") -> str:
    """
    Post a call booked notification with Showed / No Show / Not Qualified buttons.
    Returns the message ts.
    """
    value = json.dumps({"email": email, "name": name, "company": company})
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "📅 Call Booked"},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*👤 Prospect:*\n{name}"},
                {"type": "mrkdwn", "text": f"*🏢 Company:*\n{company}"},
                {"type": "mrkdwn", "text": f"*📧 Email:*\n{email}"},
                {"type": "mrkdwn", "text": f"*📣 Campaign:*\n{campaign or '2 Weeks'}"},
            ],
        },
        {
            "type": "actions",
            "block_id": f"call_outcome_{email}",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "✅ Showed"},
                    "style": "primary",
                    "action_id": "call_showed",
                    "value": value,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "❌ No Show"},
                    "style": "danger",
                    "action_id": "call_no_show",
                    "value": value,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "🚫 Not Qualified"},
                    "action_id": "call_not_qualified",
                    "value": value,
                },
            ],
        },
    ]

    response = client.chat_postMessage(
        channel=SLACK_CHANNEL_ID,
        blocks=blocks,
        text=f"Call booked — {name} ({company})",
    )
    return response["ts"]


def update_call_outcome_message(channel: str, ts: str, name: str, company: str, outcome: str, manager: str):
    """Replace the call booked buttons with the recorded outcome."""
    icons = {"Showed": "✅", "No Show": "❌", "Not Qualified": "🚫"}
    icon = icons.get(outcome, "•")
    _update_message_status(
        channel, ts,
        f"{icon} *{outcome}* — {name} ({company}) — logged by {manager} at <!date^{int(time.time())}^{{time}}|now>"
    )


def post_followup_review(
    record_id: str,
    first_name: str,
    last_name: str,
    company_name: str,
    prospect_email: str,
    followup_index: int,
    days_since_our_reply: int,
    prior_thread_summary: str,
    draft_followup_text: str,
    channel_override: str = "",
) -> str:
    """
    Post a reactivation / cadence follow-up draft to Slack.

    record_id: stable key used by the approve/deny handler. For same-thread
      follow-ups this is the id of the newest email in the thread (which
      PlusVibe's /unibox/emails/reply accepts as reply_to_id).
    """
    name_line = f"{first_name} {last_name}".strip() or "(unknown)"
    brand_title = company_name or "(unknown brand)"

    header_text = (
        f"🔄 Reactivation follow-up — {brand_title[:100]}"
        if followup_index == 1
        else f"🔄 Follow-up #{followup_index} (day {days_since_our_reply}) — {brand_title[:100]}"
    )

    blocks: list[dict] = [
        {"type": "header", "text": {"type": "plain_text", "text": header_text}},
        {
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": _summary_line(prospect_email, company_name)},
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn",
                 "text": f"*👤 Prospect:* {name_line}   "
                         f"*⏱ Days since our last reply:* {days_since_our_reply}"},
            ],
        },
    ]

    blocks.extend([
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Thread context (most recent first)*\n{prior_thread_summary[:1800]}",
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Drafted follow-up*\n{draft_followup_text}",
            },
        },
        {
            "type": "actions",
            "block_id": f"followup_actions_{record_id}",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "✅ Approve & send"},
                    "style": "primary",
                    "action_id": "approve_followup_send",
                    "value": record_id,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "✏️ Edit"},
                    "action_id": "deny_edit_followup_send",
                    "value": record_id,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "🚫 Skip"},
                    "style": "danger",
                    "action_id": "skip_followup",
                    "value": record_id,
                },
            ],
        },
    ])

    target_channel = channel_override or SLACK_CHANNEL_ID
    response = client.chat_postMessage(
        channel=target_channel,
        blocks=blocks,
        text=f"Reactivation follow-up ready for {name_line} at {brand_title}",
    )
    return response["ts"]


def update_message_skipped(channel: str, ts: str, manager: str):
    _update_message_status(
        channel, ts,
        f"🚫 Skipped by {manager} at <!date^{int(time.time())}^{{time}}|now>"
    )


def post_send_failure_notice(
    channel: str,
    ts: str,
    prospect_email: str,
    error: str,
    manager: str = "",
):
    """Reply in-thread on the original review card so the approver knows the
    send failed and what happened. Keeps the buttons intact so they can retry
    after the underlying issue is fixed."""
    text = (
        f"⚠️  Send failed for `{prospect_email}` — {error[:400]}"
        + (f"\n(approved by {manager})" if manager else "")
    )
    try:
        client.chat_postMessage(
            channel=channel, thread_ts=ts, text=text,
        )
    except SlackApiError:
        pass


def open_edit_followup_send_modal(trigger_id: str, record_id: str, current_draft: str):
    """Edit modal for a follow-up draft. Same shape as open_edit_modal but
    routes the submission to the follow-up send handler."""
    client.views_open(
        trigger_id=trigger_id,
        view={
            "type": "modal",
            "callback_id": "edit_followup_send_modal",
            "title": {"type": "plain_text", "text": "Edit follow-up"},
            "submit": {"type": "plain_text", "text": "Send"},
            "close": {"type": "plain_text", "text": "Cancel"},
            "private_metadata": record_id,
            "blocks": [
                {
                    "type": "input",
                    "block_id": "edited_followup",
                    "label": {"type": "plain_text", "text": "Edit the follow-up:"},
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "followup_text",
                        "multiline": True,
                        "initial_value": current_draft,
                    },
                }
            ],
        },
    )


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
