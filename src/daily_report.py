"""
Daily and weekly send-report for the active outbound campaign.

Daily — fires once sending has been idle for 30 min:
  - poll PlusVibe campaign stats every 15 min between 9am–11pm EST
  - when sent_count stops moving, stamp `quiet_since`; fire once we've been
    quiet ≥ 30 min (matches user spec: report 30 min after campaign stops)
  - Redis flag prevents double-fires per (campaign, day)

Weekly — aggregates the 7-day window ending today; fires on day-6 of each
ramp week (counted from `ramp:start:<campaign_id>`), after the daily report.

Volume ramp — week 1 = 1 email/mailbox/day, week 2 = 2/day, etc. We DON'T
auto-update PlusVibe limits — the weekly report posts a Slack reminder.

Deliverability flag uses reply rate INCLUDING OOO/auto-replies (this is the
inbox-placement signal — OOO means we landed). Engagement-quality rate is
shown underneath as the excl-OOO number.
  ≥ 2.0%   🟢 healthy (inboxing fine)
  1.0–2.0% 🟡 warn — watch for deliverability decay
  < 1.0%   🔴 alert — likely deliverability issue
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from slack_sdk import WebClient
from upstash_redis import Redis

from src.integrations.plusvibe import get_campaign_stats, list_received_emails_paginated

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

QUIET_FIRE_SECONDS = 30 * 60  # report fires once sending has been idle this long


def _last_seen_key(campaign_id: str, day: str) -> str:
    return f"sendreport:lastseen:{campaign_id}:{day}"


def _quiet_since_key(campaign_id: str, day: str) -> str:
    return f"sendreport:quietsince:{campaign_id}:{day}"


def _daily_fired_key(campaign_id: str, day: str) -> str:
    return f"sendreport:fired:daily:{campaign_id}:{day}"


def _weekly_fired_key(campaign_id: str, week_start: date) -> str:
    return f"sendreport:fired:weekly:{campaign_id}:{week_start.isoformat()}"


def _set_with_ttl(key: str, value: str) -> None:
    _get_redis().set(key, value, ex=SEND_REPORT_KEY_TTL)


async def check_and_fire_daily(campaign_id: str) -> Optional[str]:
    """
    Poll PlusVibe; fire the daily report once today's sending has been idle
    for `QUIET_FIRE_SECONDS` (30 min). Returns the YYYY-MM-DD reported, else None.

    State machine per (campaign, day):
      sent_today changed since last tick → clear quiet_since, update last_seen
      sent_today unchanged               → set quiet_since (if unset); fire when
                                           now - quiet_since ≥ 30 min
    """
    today = date.today()
    day = today.isoformat()
    r = _get_redis()

    if r.get(_daily_fired_key(campaign_id, day)):
        return None

    stats = await get_campaign_stats(campaign_id, day, day) or {}
    sent_today = int(stats.get("sent_count") or 0)
    last_seen_raw = r.get(_last_seen_key(campaign_id, day))
    quiet_since_key = _quiet_since_key(campaign_id, day)
    now_ts = int(time.time())

    _set_with_ttl(_last_seen_key(campaign_id, day), str(sent_today))

    if sent_today <= 0:
        # Nothing sent yet — don't start the quiet timer
        r.delete(quiet_since_key)
        return None

    if last_seen_raw is None or sent_today != int(last_seen_raw):
        # First tick of the day, or sending advanced — reset quiet timer
        r.delete(quiet_since_key)
        return None

    # Sending has been idle since last tick. Stamp quiet_since if needed.
    quiet_since_raw = r.get(quiet_since_key)
    if quiet_since_raw is None:
        _set_with_ttl(quiet_since_key, str(now_ts))
        return None

    try:
        quiet_since = int(quiet_since_raw)
    except (TypeError, ValueError):
        _set_with_ttl(quiet_since_key, str(now_ts))
        return None

    if now_ts - quiet_since < QUIET_FIRE_SECONDS:
        return None

    await generate_daily_send_report(campaign_id, today, stats)
    _set_with_ttl(_daily_fired_key(campaign_id, day), "1")
    r.delete(quiet_since_key)
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

def _delivery_flag(reply_rate_incl_ooo_pct: float, sent: int) -> str:
    """
    Inbox-placement flag. Uses reply rate INCLUDING OOO/auto-replies because
    OOO is itself evidence we landed in the inbox. Anything under 2% suggests
    deliverability decay.
    """
    if sent <= 0:
        return "n/a (nothing sent)"
    if reply_rate_incl_ooo_pct >= 2.0:
        return "🟢 healthy (≥2% — landing in inbox)"
    if reply_rate_incl_ooo_pct >= 1.0:
        return "🟡 warn (1–2% — watch for deliverability decay)"
    return "🔴 alert — likely deliverability issue (under 1%)"


def _pct(num: int, denom: int) -> float:
    """Reply-rate style percent rounded to 1 decimal to match PlusVibe display."""
    return round((num / denom) * 100, 1) if denom > 0 else 0.0


def _day_bounds_utc_iso(day: date) -> tuple[str, str]:
    """EST day → UTC ISO strings for [start, end) — used to filter PlusVibe records."""
    from zoneinfo import ZoneInfo
    est = ZoneInfo("America/New_York")
    start_est = datetime.combine(day, datetime.min.time(), tzinfo=est)
    end_est = start_est + timedelta(days=1)
    return (
        start_est.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        end_est.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
    )


async def _count_label_today(campaign_id: str, label: str, day: date) -> int:
    """Count unibox emails with `label` whose timestamp_created falls within `day` (EST)."""
    start_iso, end_iso = _day_bounds_utc_iso(day)
    rows = await list_received_emails_paginated(
        campaign_id, label=label, stop_before_iso=start_iso
    )
    return sum(
        1 for r in rows
        if start_iso <= (r.get("timestamp_created") or "") < end_iso
    )


async def _booked_leads_today(campaign_id: str, day: date) -> list[dict]:
    """
    Distinct leads currently labeled MEETING_BOOKED in PlusVibe whose latest
    reply landed today (EST). Returns [{name, email, company}, ...].

    PlusVibe doesn't expose a "label-applied-at" timestamp, so we proxy with
    the lead's most recent inbound message — in practice the SDR labels the
    lead within hours of the booking reply, so this catches the typical flow.
    """
    start_iso, end_iso = _day_bounds_utc_iso(day)
    rows = await list_received_emails_paginated(
        campaign_id, label="MEETING_BOOKED", stop_before_iso=start_iso
    )

    by_lead: dict[str, dict] = {}
    for r in rows:
        ts = r.get("timestamp_created") or ""
        if not (start_iso <= ts < end_iso):
            continue
        lead_email = (r.get("from_address_email") or r.get("lead") or "").lower()
        if not lead_email:
            continue
        prev = by_lead.get(lead_email)
        if prev is None or ts > prev.get("ts", ""):
            from_json = r.get("from_address_json") or []
            display_name = ""
            if isinstance(from_json, list) and from_json:
                display_name = (from_json[0].get("name") or "").strip()
            by_lead[lead_email] = {
                "email": lead_email,
                "name": display_name,
                "ts": ts,
                "subject": r.get("subject") or "",
            }

    # Enrich with first/last name + company via lead/get (best-effort, parallel)
    from src.integrations.plusvibe import get_lead_data
    import asyncio
    emails = list(by_lead.keys())
    leads_data = await asyncio.gather(
        *(get_lead_data(e, campaign_id) for e in emails),
        return_exceptions=True,
    )
    for email, ld in zip(emails, leads_data):
        if isinstance(ld, dict):
            first = ld.get("first_name") or ""
            last = ld.get("last_name") or ""
            full = f"{first} {last}".strip()
            if full:
                by_lead[email]["name"] = full
            by_lead[email]["company"] = ld.get("company_name") or ""

    return sorted(by_lead.values(), key=lambda x: x.get("ts", ""))


async def generate_daily_send_report(campaign_id: str, day: date, stats: dict) -> None:
    """
    Post the daily send/reply summary to Slack.

    Headline reply rate INCLUDES OOO/auto-replies — that's the inbox-placement
    signal (OOO proves we landed). Engagement rate (excl OOO) sits underneath.
    Booked-calls list is sourced from PlusVibe `label=MEETING_BOOKED`.
    """
    try:
        sent = int(stats.get("sent_count") or 0)
        replied_incl_ooo = int(stats.get("replied_count") or 0)
        positive = int(stats.get("positive_reply_count") or 0)
        bounced = int(stats.get("bounced_count") or 0)
        camp_name = stats.get("camp_name") or campaign_id

        # OOO + auto-reply counts pulled directly from PlusVibe labels for today
        ooo = await _count_label_today(campaign_id, "OUT_OF_OFFICE", day)
        auto = await _count_label_today(campaign_id, "AUTOMATIC_REPLY", day)
        ooo_total = ooo + auto
        replied_excl_ooo = max(replied_incl_ooo - ooo_total, 0)

        reply_rate_incl = _pct(replied_incl_ooo, sent)   # deliverability signal — 2% floor
        reply_rate_excl = _pct(replied_excl_ooo, sent)   # engagement signal
        positive_rate = _pct(positive, replied_incl_ooo)
        bounce_rate = _pct(bounced, sent)

        booked = await _booked_leads_today(campaign_id, day)
        if booked:
            booked_block = "\n".join(
                f"  • {b.get('name') or b.get('email')} ({b.get('email', '')})"
                + (f" — {b['company']}" if b.get("company") else "")
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
            f"*Deliverability* _(reply rate incl. OOO — inbox-placement signal)_\n"
            f"  • Reply rate (incl. OOO): *{reply_rate_incl}%* ({replied_incl_ooo} of {sent}) — {_delivery_flag(reply_rate_incl, sent)}\n"
            f"  • Reply rate (excl. OOO): {reply_rate_excl}% ({replied_excl_ooo} of {sent}) — engagement quality\n"
            f"  • Auto-replies / OOO: {ooo_total} ({auto} auto · {ooo} OOO)\n"
            f"  • Positive replies: {positive} ({positive_rate}% of all replies)\n"
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
        replied_incl_ooo = int(stats.get("replied_count") or 0)
        positive = int(stats.get("positive_reply_count") or 0)
        bounced = int(stats.get("bounced_count") or 0)
        camp_name = stats.get("camp_name") or campaign_id

        # Sum daily OOO/auto-reply counts across the week (label endpoint takes
        # no date filter, so we iterate days — cheap, ~7 paginated calls).
        ooo_total = 0
        d = week_start
        while d <= week_end:
            ooo_total += await _count_label_today(campaign_id, "OUT_OF_OFFICE", d)
            ooo_total += await _count_label_today(campaign_id, "AUTOMATIC_REPLY", d)
            d = d + timedelta(days=1)
        replied_excl_ooo = max(replied_incl_ooo - ooo_total, 0)

        reply_rate_incl = _pct(replied_incl_ooo, sent)
        reply_rate_excl = _pct(replied_excl_ooo, sent)
        positive_rate = _pct(positive, replied_incl_ooo)
        bounce_rate = _pct(bounced, sent)

        current_week = current_week_index(campaign_id, week_end)
        next_limit = current_week + 1

        text = (
            f"*Weekly Send Report — {camp_name}*\n"
            f"_Week {current_week}: {week_start.strftime('%b %d')} – {week_end.strftime('%b %d, %Y')}_\n\n"
            f"*Totals (7 days)* _(PlusVibe campaign stats)_\n"
            f"  • Emails sent: {sent}\n"
            f"  • Reply rate (incl. OOO): *{reply_rate_incl}%* ({replied_incl_ooo} of {sent}) — {_delivery_flag(reply_rate_incl, sent)}\n"
            f"  • Reply rate (excl. OOO): {reply_rate_excl}% ({replied_excl_ooo} of {sent}) — engagement quality\n"
            f"  • Auto-replies / OOO: {ooo_total}\n"
            f"  • Positive replies: {positive} ({positive_rate}% of all replies)\n"
            f"  • Bounces: {bounced} ({bounce_rate}%)\n\n"
            f"*Volume ramp reminder*\n"
            f"  • Week {current_week} complete — current daily_limit was {current_week}/mailbox/day\n"
            f"  • Bump each mailbox's daily_limit to *{next_limit}/day* in PlusVibe to start week {current_week + 1}\n"
            f"  • Deliverability is {_delivery_flag(reply_rate_incl, sent)} — hold the bump if 🟡/🔴"
        )

        _get_slack().chat_postMessage(channel=_channel(), text=text)
        log.info(f"Weekly send report posted (campaign={campaign_id}, week_end={week_end})")
    except Exception as e:
        log.exception(f"generate_weekly_send_report failed: {e}")
