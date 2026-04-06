"""
Weekly and monthly report generators.

Weekly  — every Friday at 7pm EST
Monthly — last day of every month at 7pm EST
"""

import calendar
import logging
import os
from collections import Counter
from datetime import date, timedelta

from slack_sdk import WebClient

from src.integrations.sheets import (
    TAB_PIPELINE, TAB_CALL_LOG, TAB_FOLLOW_UP,
    read_all_rows, parse_date,
)

log = logging.getLogger(__name__)

_slack: WebClient | None = None


def _get_slack() -> WebClient:
    global _slack
    if _slack is None:
        _slack = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))
    return _slack


def _channel() -> str:
    return os.getenv("SLACK_CHANNEL_ID", "")


def _week_range() -> tuple[date, date]:
    today = date.today()
    start = today - timedelta(days=today.weekday())  # Monday
    end = start + timedelta(days=6)  # Sunday
    return start, end


def _month_range() -> tuple[date, date]:
    today = date.today()
    start = today.replace(day=1)
    last_day = calendar.monthrange(today.year, today.month)[1]
    return start, today.replace(day=last_day)


def _in_range(date_str_val: str, since: date, until: date | None = None) -> bool:
    d = parse_date(date_str_val)
    if d is None:
        return False
    if until:
        return since <= d <= until
    return d >= since


def _sum_revenue(rows: list[dict]) -> float:
    total = 0.0
    for r in rows:
        try:
            total += float(str(r.get("deal_value", "0")).replace("$", "").replace(",", "") or 0)
        except Exception:
            pass
    return total


async def generate_weekly_report() -> None:
    """Generate and post the weekly performance report to Slack."""
    try:
        week_start, week_end = _week_range()
        pipeline_rows = await read_all_rows(TAB_PIPELINE)
        call_log_rows = await read_all_rows(TAB_CALL_LOG)
        followup_rows = await read_all_rows(TAB_FOLLOW_UP)
        today = date.today()

        # New prospects added this week
        new_this_week = [r for r in pipeline_rows if _in_range(r.get("date_added", ""), week_start, week_end)]

        # Stage counts (all time)
        stage_counts = Counter(r.get("stage", "") for r in pipeline_rows if r.get("stage"))

        # Calls this week
        calls_this_week = [r for r in call_log_rows if _in_range(r.get("call_date", ""), week_start, week_end)]
        shows = sum(1 for r in calls_this_week if r.get("show_status") == "Showed")
        no_shows = sum(1 for r in calls_this_week if r.get("show_status") == "No Show")
        total_calls = len(calls_this_week)
        show_rate = round(shows / max(total_calls, 1) * 100)

        # Deals closed this week
        closed_this_week = [r for r in pipeline_rows if r.get("stage") == "Closed Won" and _in_range(r.get("last_touch", ""), week_start, week_end)]
        revenue = _sum_revenue(closed_this_week)

        # Overdue follow-ups
        overdue = [r for r in pipeline_rows if r.get("next_followup") and parse_date(r.get("next_followup", "")) and parse_date(r.get("next_followup", "")) < today]

        # Follow-ups due next week
        next_week_start = week_end + timedelta(days=1)
        next_week_end = next_week_start + timedelta(days=6)
        due_next_week = [r for r in pipeline_rows if _in_range(r.get("next_followup", ""), next_week_start, next_week_end)]

        week_label = f"{week_start.strftime('%B %d')} – {week_end.strftime('%B %d, %Y')}"

        text = (
            f"*Weekly Performance Report*\n"
            f"_{week_label}_\n\n"
            f"*Pipeline*\n"
            f"• New prospects this week: {len(new_this_week)}\n"
            f"• Positive Replies (total): {stage_counts.get('Positive Reply', 0)}\n"
            f"• Calls Booked (total): {stage_counts.get('Call Booked', 0)}\n"
            f"• Proposals Out: {stage_counts.get('Proposal Sent', 0)}\n\n"
            f"*Calls This Week*\n"
            f"• Total: {total_calls} | Shows: {shows} | No Shows: {no_shows}\n"
            f"• Show Rate: {show_rate}%\n\n"
            f"*Revenue This Week*\n"
            f"• Deals Closed: {len(closed_this_week)}\n"
            f"• Revenue: ${revenue:,.0f}\n\n"
            f"*Follow-Ups*\n"
            f"• Due Next Week: {len(due_next_week)} prospects\n"
            f"• Overdue Now: {len(overdue)} prospects need attention"
        )

        _get_slack().chat_postMessage(channel=_channel(), text=text)
        log.info("Weekly report posted to Slack")

    except Exception as e:
        log.exception(f"Weekly report failed: {e}")


async def generate_monthly_report() -> None:
    """Generate and post the monthly performance report to Slack."""
    try:
        month_start, month_end = _month_range()
        today = date.today()
        month_label = today.strftime("%B %Y")

        pipeline_rows = await read_all_rows(TAB_PIPELINE)
        call_log_rows = await read_all_rows(TAB_CALL_LOG)

        # This month
        new_this_month = [r for r in pipeline_rows if _in_range(r.get("date_added", ""), month_start, month_end)]
        positive_this_month = [r for r in pipeline_rows if r.get("sentiment") == "Positive" and _in_range(r.get("reply_date", ""), month_start, month_end)]
        calls_booked = [r for r in pipeline_rows if _in_range(r.get("last_touch", ""), month_start, month_end) and r.get("stage") == "Call Booked"]
        calls_this_month = [r for r in call_log_rows if _in_range(r.get("call_date", ""), month_start, month_end)]
        shows = sum(1 for r in calls_this_month if r.get("show_status") == "Showed")
        no_shows = sum(1 for r in calls_this_month if r.get("show_status") == "No Show")
        closed_this_month = [r for r in pipeline_rows if r.get("stage") == "Closed Won" and _in_range(r.get("last_touch", ""), month_start, month_end)]
        lost_this_month = [r for r in pipeline_rows if r.get("stage") == "Closed Lost" and _in_range(r.get("last_touch", ""), month_start, month_end)]
        revenue = _sum_revenue(closed_this_month)

        total_calls = len(calls_this_month)
        show_rate = round(shows / max(total_calls, 1) * 100)
        close_rate = round(len(closed_this_month) / max(shows, 1) * 100)
        book_rate = round(len(calls_booked) / max(len(positive_this_month), 1) * 100)

        # Pipeline health
        active_stages = {"Positive Reply", "Call Booked", "Proposal Sent", "Showed"}
        active = [r for r in pipeline_rows if r.get("stage") in active_stages]
        stalled = [r for r in active if r.get("days_in_stage") and str(r.get("days_in_stage", "0")).isdigit() and int(r.get("days_in_stage", "0")) > 14]
        overdue = [r for r in pipeline_rows if r.get("next_followup") and parse_date(r.get("next_followup", "")) and parse_date(r.get("next_followup", "")) < today]

        # Loss reasons
        loss_reasons = Counter(r.get("loss_reason", "Other") for r in lost_this_month if r.get("loss_reason"))
        top_loss = loss_reasons.most_common(1)[0] if loss_reasons else ("None", 0)

        text = (
            f"*Monthly Performance Report*\n"
            f"_{month_label}_\n\n"
            f"*Outreach*\n"
            f"• New Prospects: {len(new_this_month)}\n"
            f"• Positive Replies: {len(positive_this_month)}\n\n"
            f"*Calls*\n"
            f"• Booked: {len(calls_booked)} | Book Rate: {book_rate}%\n"
            f"• Shows: {shows} | No Shows: {no_shows} | Show Rate: {show_rate}%\n\n"
            f"*Revenue*\n"
            f"• Deals Closed: {len(closed_this_month)} | Close Rate: {close_rate}%\n"
            f"• Revenue: ${revenue:,.0f}\n"
            f"• Deals Lost: {len(lost_this_month)}\n\n"
            f"*Pipeline Health*\n"
            f"• Active Prospects: {len(active)}\n"
            f"• Stalled (>14 days in stage): {len(stalled)}\n"
            f"• Overdue Follow-Ups: {len(overdue)}\n\n"
            f"*Loss Analysis*\n"
            f"• Top Loss Reason: {top_loss[0]} ({top_loss[1]} deals)"
        )

        _get_slack().chat_postMessage(channel=_channel(), text=text)
        log.info("Monthly report posted to Slack")

    except Exception as e:
        log.exception(f"Monthly report failed: {e}")
