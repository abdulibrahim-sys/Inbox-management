"""
CRM natural language command handler.

Listens to Slack bot messages, parses intent via Claude, executes
the corresponding Google Sheet update, and returns a confirmation.
"""

import json
import logging
import os
from datetime import date, timedelta

import anthropic

from src.integrations.sheets import (
    TAB_PIPELINE, TAB_CALL_LOG, TAB_FOLLOW_UP,
    find_row_by_email, append_row, update_row, read_all_rows,
    today_str, date_str, parse_date, increment_field,
)

log = logging.getLogger(__name__)

_client: anthropic.AsyncAnthropic | None = None


def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        _client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    return _client


# ── Auto-nurture cadences ─────────────────────────────────────────────────────

NURTURE_CADENCES = {
    "No Show":                  [1, 3, 7, 14, 30, 60, 90],
    "Closed Lost - timing":     [30, 60, 90, 180],
    "Closed Lost - competitor": [90, 180],
    "Ghosted After Call":       [3, 7, 14, 30, 60],
    "Not Right Now":            [30, 60, 90, 180, 270],
    "Stalled":                  [1, 7, 14],
}


async def add_nurture_schedule(
    email: str,
    name: str,
    company: str,
    original_stage: str,
    reason: str,
) -> None:
    """Append Follow-Up Schedule rows based on the nurture cadence for this reason."""
    cadence = NURTURE_CADENCES.get(reason, [3, 7, 14])
    today = date.today()
    for i, days in enumerate(cadence, 1):
        followup_date = date_str(today + timedelta(days=days))
        await append_row(TAB_FOLLOW_UP, [
            name, company, email, original_stage, reason,
            i, today_str(), followup_date,
            "Personal Email", "Pending", "", "Pending", "",
        ])
    log.info(f"Nurture schedule added for {email}: {len(cadence)} follow-ups ({reason})")


# ── Intent parsing via Claude ─────────────────────────────────────────────────

INTENT_PROMPT = """You are a CRM assistant for a cold email agency. Parse the user's message into a structured JSON action.

Available intents:
- UPDATE_SHOW_STATUS: prospect showed, no-showed, rescheduled, or cancelled a call
- CLOSE_DEAL: prospect signed / closed won
- BOOK_FOLLOW_UP: schedule a follow-up call or next touch
- MOVE_TO_NURTURE: move prospect to long-term nurture
- PROPOSAL_SENT: mark that a proposal was sent
- CLOSE_LOST: deal lost
- QUERY_STATS: asking for pipeline/revenue/call stats
- RESCHEDULE: reschedule an existing call
- ADD_NOTE: add a note to a prospect's record

Extract these fields where present:
- prospect_email: email address (most reliable identifier)
- prospect_name: full name
- company: company name
- show_status: "Showed" | "No Show" | "Rescheduled" | "Cancelled"
- deal_value: numeric value if mentioned (e.g. 3000 for "$3k/mo")
- loss_reason: "Price Too High" | "Not the Right Time" | "Went with Competitor" | "No Budget" | "No Decision Maker" | "Not Interested" | "Ghosted" | "Other"
- date: any date mentioned (ISO format YYYY-MM-DD if possible)
- note_text: the note content
- metric: what stat they're asking about
- time_period: "this week" | "this month" | "today" etc

Respond with JSON only:
{
  "intent": "<INTENT_NAME or null if unclear>",
  "prospect_email": "<email or null>",
  "prospect_name": "<name or null>",
  "company": "<company or null>",
  "show_status": "<status or null>",
  "deal_value": <number or null>,
  "loss_reason": "<reason or null>",
  "date": "<YYYY-MM-DD or null>",
  "note_text": "<note or null>",
  "metric": "<metric or null>",
  "time_period": "<period or null>",
  "needs_clarification": false,
  "clarification_question": "<question or null>"
}"""


async def parse_slack_command(message_text: str) -> dict:
    """Use Claude to parse a natural language Slack message into a structured intent."""
    # Strip bot mention (e.g. <@U12345>)
    import re
    text = re.sub(r"<@[A-Z0-9]+>", "", message_text).strip()

    try:
        response = await _get_client().messages.create(
            model="claude-sonnet-4-6",
            max_tokens=400,
            system=INTENT_PROMPT,
            messages=[{"role": "user", "content": text}],
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except Exception as e:
        log.exception(f"Intent parsing failed: {e}")
        return {"intent": None, "needs_clarification": True,
                "clarification_question": "Sorry, something went wrong parsing that. Please try again."}


# ── Intent executors ──────────────────────────────────────────────────────────

async def execute_crm_command(parsed: dict) -> str:
    """
    Execute the CRM action described by the parsed intent.
    Returns a confirmation string to post back to Slack.
    """
    intent = parsed.get("intent")

    if not intent:
        if parsed.get("needs_clarification"):
            return parsed.get("clarification_question", "I didn't understand that. Can you rephrase?")
        return "I didn't understand that command. Try mentioning a prospect email, name, and what you want to do."

    email = (parsed.get("prospect_email") or "").strip().lower()
    name = parsed.get("prospect_name") or ""
    company = parsed.get("company") or ""

    # Find the prospect in Pipeline if we have an email
    row_num = None
    row_data = None
    if email:
        row_num, row_data = await find_row_by_email(TAB_PIPELINE, email)
        if row_data:
            name = name or row_data.get("name", "")
            company = company or row_data.get("company", "")

    if intent == "UPDATE_SHOW_STATUS":
        return await _handle_show_status(parsed, email, name, company, row_num, row_data)

    elif intent == "CLOSE_DEAL":
        return await _handle_close_deal(parsed, email, name, company, row_num)

    elif intent == "CLOSE_LOST":
        return await _handle_close_lost(parsed, email, name, company, row_num, row_data)

    elif intent == "PROPOSAL_SENT":
        return await _handle_proposal_sent(email, name, company, row_num)

    elif intent == "MOVE_TO_NURTURE":
        return await _handle_move_to_nurture(parsed, email, name, company, row_num, row_data)

    elif intent == "ADD_NOTE":
        return await _handle_add_note(parsed, email, name, company, row_num, row_data)

    elif intent == "RESCHEDULE":
        return await _handle_reschedule(parsed, email, name, company, row_num)

    elif intent == "BOOK_FOLLOW_UP":
        return await _handle_book_followup(parsed, email, name, company, row_num)

    elif intent == "QUERY_STATS":
        return await _handle_query_stats(parsed)

    return f"I understood the intent ({intent}) but couldn't execute it. Please check the prospect email is correct."


async def _handle_show_status(parsed, email, name, company, row_num, row_data) -> str:
    show_status = parsed.get("show_status") or "No Show"
    today = today_str()

    if not row_num:
        return f"Couldn't find a prospect matching {email or name}. Please check the email address."

    stage = "No Show" if show_status == "No Show" else ("Showed" if show_status == "Showed" else row_data.get("stage", ""))
    await update_row(TAB_PIPELINE, row_num, {
        "stage": stage,
        "last_touch": today,
    })

    # Find and update Call Log row (email stored in notes col M)
    call_log_rows = await read_all_rows(TAB_CALL_LOG)
    call_row_num = None
    for r in reversed(call_log_rows):  # most recent first
        if email and email in r.get("notes", "").lower():
            call_row_num = r["_row_number"]
            break
        if name and name.lower() in r.get("name", "").lower():
            call_row_num = r["_row_number"]
            break

    if call_row_num:
        await update_row(TAB_CALL_LOG, call_row_num, {"show_status": show_status})

    # Auto-nurture on no-show
    if show_status == "No Show":
        original_stage = row_data.get("stage", "Call Booked") if row_data else "Call Booked"
        await add_nurture_schedule(email, name, company, original_stage, "No Show")
        next_touch = date_str(date.today() + timedelta(days=1))
        return (f"*No Show recorded*\n"
                f"• {name} ({company})\n"
                f"• Pipeline updated to No Show\n"
                f"• Nurture sequence started — next touch: {next_touch}")

    return f"*Show status updated*\n• {name} ({company}) — {show_status}"


async def _handle_close_deal(parsed, email, name, company, row_num) -> str:
    deal_value = parsed.get("deal_value")
    today = today_str()

    if not row_num:
        return f"Couldn't find prospect {email or name} in the pipeline."

    updates = {"stage": "Closed Won", "last_touch": today}
    if deal_value:
        updates["deal_value"] = str(deal_value)

    await update_row(TAB_PIPELINE, row_num, updates)

    # Update Call Log if match found
    call_log_rows = await read_all_rows(TAB_CALL_LOG)
    for r in reversed(call_log_rows):
        if email and email in r.get("notes", "").lower():
            await update_row(TAB_CALL_LOG, r["_row_number"], {
                "outcome": "Closed Won",
                "deal_value": str(deal_value or ""),
                "close_date": today,
            })
            break

    val_str = f"${deal_value:,}" if deal_value else "not specified"
    return (f"*Deal Closed*\n"
            f"• Prospect: {name} ({company})\n"
            f"• Value: {val_str}\n"
            f"• Pipeline stage → Closed Won")


async def _handle_close_lost(parsed, email, name, company, row_num, row_data) -> str:
    loss_reason = parsed.get("loss_reason") or "Other"
    today = today_str()

    if not row_num:
        return f"Couldn't find prospect {email or name} in the pipeline."

    await update_row(TAB_PIPELINE, row_num, {
        "stage": "Closed Lost",
        "loss_reason": loss_reason,
        "last_touch": today,
    })

    # Determine nurture cadence
    if "competitor" in loss_reason.lower():
        reason_key = "Closed Lost - competitor"
    else:
        reason_key = "Closed Lost - timing"

    original_stage = row_data.get("stage", "Proposal Sent") if row_data else "Proposal Sent"
    await add_nurture_schedule(email, name, company, original_stage, reason_key)

    return (f"*Deal Closed Lost*\n"
            f"• {name} ({company})\n"
            f"• Reason: {loss_reason}\n"
            f"• Nurture sequence started")


async def _handle_proposal_sent(email, name, company, row_num) -> str:
    if not row_num:
        return f"Couldn't find prospect {email or name} in the pipeline."

    await update_row(TAB_PIPELINE, row_num, {
        "stage": "Proposal Sent",
        "last_touch": today_str(),
    })
    return f"*Proposal Sent*\n• {name} ({company})\n• Pipeline updated to Proposal Sent"


async def _handle_move_to_nurture(parsed, email, name, company, row_num, row_data) -> str:
    if not row_num:
        return f"Couldn't find prospect {email or name} in the pipeline."

    today = today_str()
    next_touch = date_str(date.today() + timedelta(days=30))
    await update_row(TAB_PIPELINE, row_num, {
        "stage": "Nurture",
        "last_touch": today,
        "next_followup": next_touch,
    })

    original_stage = row_data.get("stage", "") if row_data else ""
    await add_nurture_schedule(email, name, company, original_stage, "Not Right Now")

    return (f"*Moved to Nurture*\n"
            f"• {name} ({company})\n"
            f"• Next follow-up: {next_touch}\n"
            f"• Follow-up sequence added")


async def _handle_add_note(parsed, email, name, company, row_num, row_data) -> str:
    note_text = parsed.get("note_text", "")
    if not note_text:
        return "What note would you like to add? Please include the note text in your message."
    if not row_num:
        return f"Couldn't find prospect {email or name}."

    existing = row_data.get("notes", "") if row_data else ""
    timestamp = date.today().strftime("%m/%d/%Y")
    separator = "\n" if existing else ""
    new_notes = f"{existing}{separator}[{timestamp}] {note_text}"

    await update_row(TAB_PIPELINE, row_num, {"notes": new_notes, "last_touch": today_str()})
    return f"*Note added*\n• {name} ({company})\n• {note_text}"


async def _handle_reschedule(parsed, email, name, company, row_num) -> str:
    new_date_raw = parsed.get("date")
    if not new_date_raw:
        return "What date should I reschedule to? Please include the new date."
    if not row_num:
        return f"Couldn't find prospect {email or name}."

    try:
        new_date = date_str(date.fromisoformat(new_date_raw))
    except Exception:
        new_date = new_date_raw

    await update_row(TAB_PIPELINE, row_num, {
        "next_followup": new_date,
        "last_touch": today_str(),
    })

    # Update Call Log reschedule date
    call_log_rows = await read_all_rows(TAB_CALL_LOG)
    for r in reversed(call_log_rows):
        if email and email in r.get("notes", "").lower():
            await update_row(TAB_CALL_LOG, r["_row_number"], {
                "show_status": "Rescheduled",
                "reschedule_date": new_date,
            })
            break

    return f"*Rescheduled*\n• {name} ({company})\n• New date: {new_date}"


async def _handle_book_followup(parsed, email, name, company, row_num) -> str:
    new_date_raw = parsed.get("date")
    today = today_str()

    if row_num:
        updates = {"stage": "Call Booked", "last_touch": today}
        if new_date_raw:
            try:
                updates["next_followup"] = date_str(date.fromisoformat(new_date_raw))
            except Exception:
                updates["next_followup"] = new_date_raw
        await update_row(TAB_PIPELINE, row_num, updates)

    date_display = updates.get("next_followup", "TBD") if row_num else new_date_raw or "TBD"
    return f"*Follow-up booked*\n• {name} ({company})\n• Date: {date_display}\n• Pipeline updated to Call Booked"


async def _handle_query_stats(parsed) -> str:
    """Read Pipeline and Call Log, return a formatted stats summary."""
    metric = (parsed.get("metric") or "").lower()
    period = (parsed.get("time_period") or "this month").lower()

    pipeline_rows = await read_all_rows(TAB_PIPELINE)
    call_log_rows = await read_all_rows(TAB_CALL_LOG)

    today = date.today()
    # Determine date filter
    if "week" in period:
        since = today - timedelta(days=today.weekday())
    elif "today" in period:
        since = today
    else:
        since = today.replace(day=1)  # start of month

    def in_period(date_str_val: str) -> bool:
        d = parse_date(date_str_val)
        return d is not None and d >= since

    # Stage counts
    from collections import Counter
    stage_counts = Counter(r.get("stage", "") for r in pipeline_rows if r.get("stage"))

    # Call stats from Call Log
    period_calls = [r for r in call_log_rows if in_period(r.get("call_date", ""))]
    shows = sum(1 for r in period_calls if r.get("show_status") == "Showed")
    no_shows = sum(1 for r in period_calls if r.get("show_status") == "No Show")

    # Revenue closed this period
    closed_rows = [r for r in pipeline_rows if r.get("stage") == "Closed Won" and in_period(r.get("last_touch", ""))]
    revenue = 0
    for r in closed_rows:
        try:
            revenue += float(str(r.get("deal_value", "0")).replace("$", "").replace(",", "") or 0)
        except Exception:
            pass

    # Overdue follow-ups
    overdue = [r for r in pipeline_rows if r.get("next_followup") and parse_date(r.get("next_followup", "")) and parse_date(r.get("next_followup", "")) < today]

    lines = [
        f"*Pipeline Stats ({period})*",
        f"• Positive Replies: {stage_counts.get('Positive Reply', 0)}",
        f"• Calls Booked: {stage_counts.get('Call Booked', 0)}",
        f"• Proposals Sent: {stage_counts.get('Proposal Sent', 0)}",
        f"• Closed Won: {stage_counts.get('Closed Won', 0)}",
        f"• Closed Lost: {stage_counts.get('Closed Lost', 0)}",
        f"• In Nurture: {stage_counts.get('Nurture', 0)}",
        "",
        f"*Calls ({period})*",
        f"• Showed: {shows}",
        f"• No Shows: {no_shows}",
        f"• Show Rate: {round(shows / max(shows + no_shows, 1) * 100)}%",
        "",
        f"*Revenue ({period})*: ${revenue:,.0f}",
        f"*Overdue Follow-Ups*: {len(overdue)} prospects need attention",
    ]

    return "\n".join(lines)
