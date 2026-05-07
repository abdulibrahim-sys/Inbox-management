"""
Daily and weekly send-report for the active outbound campaign.

Daily — fires the moment sending appears done for the day:
  - poll PlusVibe campaign stats every 15 min between 9am–11pm EST
  - first tick records baseline; on subsequent tick, "done" = today's
    sent_count > 0 AND unchanged since the prior tick (sending window quiet)
  - Redis flag prevents double-fires per (campaign, day)

Weekly — aggregates the 7-day window ending today; fires on day-6 of each
ramp week (counted from `ramp:start:<campaign_id>`), after the daily report.

Volume ramp — week 1 = 1 email/mailbox/day, week 2 = 2/day, etc. We DON'T
auto-update PlusVibe limits — the weekly report posts a Slack reminder.

Deliverability flag uses reply rate excl. OOO/auto-replies:
  ≥ 1.0%   🟢 healthy
  0.8–1.0% 🟡 warn
  < 0.8%   🔴 alert (likely deliverability issue)
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from typing import Optional

from slack_sdk import WebClient
from upstash_redis import Redis

from src.integrations.plusvibe import get_campaign_stats
from src.learning import get_booked_calls

log = logging.getLogger(__name__)

DEFAULT_CAMPAIGN_START = "2026-05-07"  # "2 weeks - May [Outlook]" launches today
SEND_REPORT_KEY_TTL = 60 * 60 * 24 * 30  # 30 days

_slack: Optional[WebClient] = None
_redis: Optional[Redis] = None


def _get_slack() -> WebClient:
    global _slack
    if _slack is None:
        _slack = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))
    return _slack


def _get_redis() -> Redis:
    global _redis
    if _redis is None:
        _redis = Redis(
            url=os.getenv("UPSTASH_REDIS_REST_URL"),
            token=os.getenv("UPSTASH_REDIS_REST_TOKEN"),
        )
    return _redis


def _channel() -> str:
    return os.getenv("SLACK_CHANNEL_ID", "")


# ── Ramp state ────────────────────────────────────────────────────────────────

def get_ramp_start(campaign_id: str) -> date:
    """Anchor date for the volume ramp; lazily seeded from DEFAULT_CAMPAIGN_START."""
    r = _get_redis()
    raw = r.get(f"ramp:start:{campaign_id}")
    if raw:
        try:
            return date.fromisoformat(raw)
        except Exception:
            pass
    d = date.fromisoformat(DEFAULT_CAMPAIGN_START)
    r.set(f"ramp:start:{campaign_id}", d.isoformat())
    return d


def set_ramp_start(campaign_id: str, d: date) -> None:
    _get_redis().set(f"ramp:start:{campaign_id}", d.isoformat())


def current_week_index(campaign_id: str, today: Optional[date] = None) -> int:
    """1-based ramp week. Week 1 = days 0..6 from start. Returns 0 if pre-start."""
    if today is None:
        today = date.today()
    days = (today - get_ramp_start(campaign_id)).days
    if days < 0:
        return 0
    return days // 7 + 1


def current_daily_limit(campaign_id: str, today: Optional[date] = None) -> int:
    """Expected per-mailbox daily limit per the ramp (week N → N emails/day)."""
    return max(current_week_index(campaign_id, today), 1)


# ── Trigger detection ────────────────────────────────────────────────────────

def _last_seen_key(campaign_id: str, day: str) -> str:
    return f"sendreport:lastseen:{campaign_id}:{day}"


def _daily_fired_key(campaign_id: str, day: str) -> str:
    return f"sendreport:fired:daily:{campaign_id}:{day}"


def _weekly_fired_key(campaign_id: str, week_start: date) -> str:
    return f"sendreport:fired:weekly:{campaign_id}:{week_start.isoformat()}"


def _set_with_ttl(key: str, value: str) -> None:
    _get_redis().set(key, value, ex=SEND_REPORT_KEY_TTL)


async def check_and_fire_daily(campaign_id: str) -> Optional[str]:
    """
    Poll PlusVibe; fire the daily report if today's sending is done.

    Returns the YYYY-MM-DD that was reported, or None.
    """
    today = date.today()
    day = today.isoformat()
    r = _get_redis()

    if r.get(_daily_fired_key(campaign_id, day)):
        return None

    stats = await get_campaign_stats(campaign_id, day, day) or {}
    sent_today = int(stats.get("sent_count") or 0)
    last_seen_raw = r.get(_last_seen_key(campaign_id, day))
    _set_with_ttl(_last_seen_key(campaign_id, day), str(sent_today))

    if last_seen_raw is None:
        return None  # first tick of the day, nothing to compare against
    if sent_today <= 0:
        return None
    if sent_today != int(last_seen_raw):
        return None  # still actively sending

    await generate_daily_send_report(campaign_id, today, stats)
    _set_with_ttl(_daily_fired_key(campaign_id, day), "1")
    await maybe_fire_weekly(campaign_id, today)
    return day


async def maybe_fire_weekly(campaign_id: str, today: date) -> None:
    """Fire weekly summary on day 7, 14, 21… of the ramp (after the daily fires)."""
    days = (today - get_ramp_start(campaign_id)).days
    if days < 6 or days % 7 != 6:
        return
    week_start = today - timedelta(days=6)
    if _get_redis().get(_weekly_fired_key(campaign_id, week_start)):
        return
    await generate_weekly_send_report(campaign_id, week_start, today)
    _set_with_ttl(_weekly_fired_key(campaign_id, week_start), "1")


# ── Reports ──────────────────────────────────────────────────────────────────

def _flag(reply_rate_pct: float, sent: int) -> str:
    if sent <= 0:
        return "n/a (nothing sent)"
    if reply_rate_pct >= 1.0:
        return "🟢 healthy"
    if reply_rate_pct >= 0.8:
        return "🟡 warn (under 1%)"
    return "🔴 alert — likely deliverability issue (under 0.8%)"


def _pct(num: int, denom: int) -> float:
    """Reply-rate style percent rounded to 1 decimal to match PlusVibe display."""
    return round((num / denom) * 100, 1) if denom > 0 else 0.0


async def generate_daily_send_report(campaign_id: str, day: date, stats: dict) -> None:
    """
    Post the daily send/reply summary to Slack.

    Reply rate matches PlusVibe (`replied_count / sent_count`, 1-decimal),
    so the headline number always agrees with the dashboard. The OOO-excluded
    rate is shown underneath as the engagement-quality signal.
    """
    try:
        sent = int(stats.get("sent_count") or 0)
        replied = int(stats.get("replied_count") or 0)
        positive = int(stats.get("positive_reply_count") or 0)
        bounced = int(stats.get("bounced_count") or 0)
        camp_name = stats.get("camp_name") or campaign_id

        reply_rate = _pct(replied, sent)
        positive_rate = _pct(positive, sent)
        bounce_rate = _pct(bounced, sent)

        booked = get_booked_calls(day.isoformat())
        if booked:
            booked_block = "\n".join(
                f"  • {b.get('name', '')} ({b.get('email', '')}) — {b.get('company', '')}"
                for b in booked
            )
            booked_section = f"*Calls booked today* ({len(booked)})\n{booked_block}"
        else:
            booked_section = "*Calls booked today*\n  • none"

        week = current_week_index(campaign_id, day)
        date_label = day.strftime("%A, %b %d %Y")

        text = (
            f"*Daily Send Report — {camp_name}*\n"
            f"_{date_label}_\n\n"
            f"*Volume*\n"
            f"  • Emails sent: *{sent}*\n"
            f"  • Ramp: week {week} → expected {current_daily_limit(campaign_id, day)}/mailbox/day\n\n"
            f"*Replies* _(PlusVibe campaign stats)_\n"
            f"  • Reply rate: *{reply_rate}%* ({replied} of {sent}) — {_flag(reply_rate, sent)}\n"
            f"  • Positive reply rate: *{positive_rate}%* ({positive} positive)\n"
            f"  • Bounces: {bounced} ({bounce_rate}%)\n\n"
            f"{booked_section}"
        )

        _get_slack().chat_postMessage(channel=_channel(), text=text)
        log.info(f"Daily send report posted (campaign={campaign_id}, day={day})")
    except Exception as e:
        log.exception(f"generate_daily_send_report failed: {e}")


async def generate_weekly_send_report(campaign_id: str, week_start: date, week_end: date) -> None:
    """Aggregate 7 days of stats + post the volume-bump reminder."""
    try:
        stats = await get_campaign_stats(
            campaign_id, week_start.isoformat(), week_end.isoformat()
        ) or {}
        sent = int(stats.get("sent_count") or 0)
        replied = int(stats.get("replied_count") or 0)
        positive = int(stats.get("positive_reply_count") or 0)
        bounced = int(stats.get("bounced_count") or 0)
        camp_name = stats.get("camp_name") or campaign_id

        reply_rate = _pct(replied, sent)
        positive_rate = _pct(positive, sent)
        bounce_rate = _pct(bounced, sent)

        current_week = current_week_index(campaign_id, week_end)
        next_limit = current_week + 1

        text = (
            f"*Weekly Send Report — {camp_name}*\n"
            f"_Week {current_week}: {week_start.strftime('%b %d')} – {week_end.strftime('%b %d, %Y')}_\n\n"
            f"*Totals (7 days)* _(PlusVibe campaign stats)_\n"
            f"  • Emails sent: {sent}\n"
            f"  • Reply rate: *{reply_rate}%* ({replied} of {sent}) — {_flag(reply_rate, sent)}\n"
            f"  • Positive reply rate: *{positive_rate}%* ({positive} positive)\n"
            f"  • Bounces: {bounced} ({bounce_rate}%)\n\n"
            f"*Volume ramp reminder*\n"
            f"  • Week {current_week} complete — current daily_limit was {current_week}/mailbox/day\n"
            f"  • Bump each mailbox's daily_limit to *{next_limit}/day* in PlusVibe to start week {current_week + 1}\n"
            f"  • Reply rate is {_flag(reply_rate, sent)} — hold the bump if anything is in the 🟡/🔴 zone"
        )

        _get_slack().chat_postMessage(channel=_channel(), text=text)
        log.info(f"Weekly send report posted (campaign={campaign_id}, week_end={week_end})")
    except Exception as e:
        log.exception(f"generate_weekly_send_report failed: {e}")
