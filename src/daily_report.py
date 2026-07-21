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

# ── Benchmarks (from the 2-weeks May campaign, n=134,836 sends) ──────────────
# Frozen at analysis time — do not recompute from live data. These are the
# reference rates the daily report compares against.
BENCH_2WK_SENT = 134_836
BENCH_2WK_REPLIES = 742
BENCH_2WK_POSITIVE = 226
BENCH_2WK_POS_PER_SEND_PCT = round(BENCH_2WK_POSITIVE / BENCH_2WK_SENT * 100, 4)  # 0.1676%
BENCH_2WK_POS_OF_REPLY_PCT = round(BENCH_2WK_POSITIVE / BENCH_2WK_REPLIES * 100, 1)  # 30.5%
BENCH_2WK_C_POS_PER_SEND_PCT = 0.2270  # winning variation

# Success thresholds (from user spec)
TARGET_POS_OF_REPLY_PCT = 20.0     # positive-of-reply target
DELIVERABILITY_FLOOR_PCT = 2.0     # reply rate (incl. OOO) must stay ≥ this
PROJECTION_DAILY_SENDS = 10_000    # what if we sent 10K/day

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
      sent_today == 0 AND hour ≥ 20 EST  → fire an empty-day report anyway
                                           (guarantees a daily post on
                                           weekends / campaign-paused days)
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
        # Nothing sent — post an EOD placeholder after 8pm EST so the
        # channel still gets a daily heartbeat.
        from zoneinfo import ZoneInfo
        hour_est = datetime.now(ZoneInfo("America/New_York")).hour
        if hour_est >= 20:
            await generate_daily_send_report(campaign_id, today, stats)
            _set_with_ttl(_daily_fired_key(campaign_id, day), "1")
            await maybe_fire_weekly(campaign_id, today)
            return day
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


async def _interested_leads_today(campaign_id: str, day: date) -> list[dict]:
    """
    Distinct leads whose latest INTERESTED reply landed today (EST). These are
    prospects asking for a call, info, case studies, etc. — the positives that
    haven't converted to MEETING_BOOKED yet. Returns [{name,email,company,subject,body_snippet}].
    """
    start_iso, end_iso = _day_bounds_utc_iso(day)
    rows = await list_received_emails_paginated(
        campaign_id, label="INTERESTED", stop_before_iso=start_iso
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
            body_dict = r.get("body") or {}
            body_text = (body_dict.get("text") if isinstance(body_dict, dict) else "") \
                or r.get("content_preview") or ""
            body_snippet = " ".join(body_text.split())[:220]
            by_lead[lead_email] = {
                "email": lead_email,
                "name": display_name,
                "ts": ts,
                "subject": r.get("subject") or "",
                "body_snippet": body_snippet,
            }

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


def _projection_block(pos_per_send_pct: float, pos_of_reply_pct: float,
                      reply_rate_incl_pct: float) -> str:
    """
    Project today's observed rates out to PROJECTION_DAILY_SENDS sends/day.
    Reports what to expect at that scale under today's observed rates AND
    under the 2-weeks C-variation winning rate for reference.
    """
    per_day_today = PROJECTION_DAILY_SENDS * pos_per_send_pct / 100.0
    per_day_bench_c = PROJECTION_DAILY_SENDS * BENCH_2WK_C_POS_PER_SEND_PCT / 100.0
    per_day_bench_blended = PROJECTION_DAILY_SENDS * BENCH_2WK_POS_PER_SEND_PCT / 100.0
    replies_per_day = PROJECTION_DAILY_SENDS * reply_rate_incl_pct / 100.0
    return (
        f"*If we scale to {PROJECTION_DAILY_SENDS:,} sends/day*\n"
        f"  • At *today's* positive rate ({pos_per_send_pct:.4f}%): "
        f"~{per_day_today:.1f} positives/day → "
        f"~{per_day_today*7:.0f}/wk → ~{per_day_today*30:.0f}/mo\n"
        f"  • At the 2-weeks blended benchmark ({BENCH_2WK_POS_PER_SEND_PCT:.4f}%): "
        f"~{per_day_bench_blended:.1f}/day → ~{per_day_bench_blended*30:.0f}/mo\n"
        f"  • At the 2-weeks C winning rate ({BENCH_2WK_C_POS_PER_SEND_PCT:.4f}%): "
        f"~{per_day_bench_c:.1f}/day → ~{per_day_bench_c*30:.0f}/mo\n"
        f"  • Expected replies at 10K/day (incl. OOO): ~{replies_per_day:.0f}/day"
    )


def _benchmark_block(pos_per_send_pct: float, pos_of_reply_pct: float,
                     replied_incl: int, sent: int) -> str:
    """
    Compare today's observed rates against the 2-weeks winning campaign on an
    equal-sends basis. Rate-based comparison is scale-invariant.
    """
    # What 2-weeks would have produced on the SAME number of sends today
    expected_pos_at_2wk = int(round(sent * BENCH_2WK_POS_PER_SEND_PCT / 100.0))
    expected_replies_at_2wk = int(round(sent * (BENCH_2WK_REPLIES / BENCH_2WK_SENT * 100) / 100.0))

    def _delta_arrow(observed, benchmark):
        if observed >= benchmark: return "🟢"
        if observed >= benchmark * 0.75: return "🟡"
        return "🔴"

    pos_delta = _delta_arrow(pos_per_send_pct, BENCH_2WK_POS_PER_SEND_PCT)
    por_delta = _delta_arrow(pos_of_reply_pct, TARGET_POS_OF_REPLY_PCT)

    return (
        f"*Benchmark vs 2-weeks winner* _(on equal send volume)_\n"
        f"  • Positive per send: *{pos_per_send_pct:.4f}%* today vs "
        f"{BENCH_2WK_POS_PER_SEND_PCT:.4f}% (2-wk blended) / "
        f"{BENCH_2WK_C_POS_PER_SEND_PCT:.4f}% (2-wk C winner) — {pos_delta}\n"
        f"  • Positive of reply: *{pos_of_reply_pct:.1f}%* today vs "
        f"target {TARGET_POS_OF_REPLY_PCT:.0f}% (2-wk actual {BENCH_2WK_POS_OF_REPLY_PCT}%) — {por_delta}\n"
        f"  • On today's {sent} sends the 2-weeks winner would've produced "
        f"~{expected_pos_at_2wk} positives and ~{expected_replies_at_2wk} total replies"
    )


async def generate_daily_send_report(campaign_id: str, day: date, stats: dict) -> None:
    """
    Post the daily send/reply summary to Slack.

    Headline reply rate INCLUDES OOO/auto-replies — that's the inbox-placement
    signal (OOO proves we landed). Engagement rate (excl OOO) sits underneath.
    Adds: 2-weeks benchmark comparison, 10K/day projection, and today's
    positive-reply detail (calls booked + INTERESTED leads asking for a call).
    """
    try:
        sent = int(stats.get("sent_count") or 0)
        # PlusVibe's `replied_count` counts human labels only
        # (NOT_INTERESTED + INTERESTED + MEETING_BOOKED). OOO/auto-reply are
        # separate label buckets and must be added back for the true
        # inbox-placement signal.
        replied_excl_ooo = int(stats.get("replied_count") or 0)
        positive = int(stats.get("positive_reply_count") or 0)
        bounced = int(stats.get("bounced_count") or 0)
        camp_name = stats.get("camp_name") or campaign_id

        ooo = await _count_label_today(campaign_id, "OUT_OF_OFFICE", day)
        auto = await _count_label_today(campaign_id, "AUTOMATIC_REPLY", day)
        ooo_total = ooo + auto
        replied_incl_ooo = replied_excl_ooo + ooo_total

        reply_rate_incl = _pct(replied_incl_ooo, sent)   # deliverability signal — 2% floor
        reply_rate_excl = _pct(replied_excl_ooo, sent)   # engagement signal
        positive_of_reply = _pct(positive, replied_incl_ooo)
        positive_per_send = round((positive / sent) * 100, 4) if sent else 0.0
        bounce_rate = _pct(bounced, sent)

        # Booked calls today
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

        # Positive replies still awaiting booking (asking for call / info / case studies)
        interested = await _interested_leads_today(campaign_id, day)
        if interested:
            interested_block = "\n".join(
                f"  • {b.get('name') or b.get('email')} ({b.get('email', '')})"
                + (f" — {b['company']}" if b.get("company") else "")
                + (f"\n      _{b['body_snippet']}_" if b.get("body_snippet") else "")
                for b in interested
            )
            interested_section = (
                f"*Positive replies today — awaiting response* ({len(interested)})\n"
                f"{interested_block}"
            )
        else:
            interested_section = "*Positive replies today — awaiting response*\n  • none"

        # Alerts (surface at top of report)
        alerts = []
        if reply_rate_incl < DELIVERABILITY_FLOOR_PCT and sent > 200:
            alerts.append(
                f"🔴 Reply rate {reply_rate_incl}% is BELOW {DELIVERABILITY_FLOOR_PCT}% floor — deliverability check"
            )
        if bounce_rate >= 2.0 and sent > 200:
            alerts.append(f"🔴 Bounce rate {bounce_rate}% at/above 2% ceiling — list quality issue")
        elif bounce_rate >= 1.5:
            alerts.append(f"🟡 Bounce rate {bounce_rate}% approaching 2% ceiling")
        if replied_incl_ooo >= 10 and positive_of_reply < TARGET_POS_OF_REPLY_PCT:
            alerts.append(
                f"🟡 Positive-of-reply {positive_of_reply}% BELOW {TARGET_POS_OF_REPLY_PCT:.0f}% target"
                f" (2-weeks winner averaged {BENCH_2WK_POS_OF_REPLY_PCT}%)"
            )
        alerts_section = ("\n".join(alerts) + "\n\n") if alerts else ""

        week = current_week_index(campaign_id, day)
        date_label = day.strftime("%A, %b %d %Y")

        text = (
            f"*Daily Send Report — {camp_name}*\n"
            f"_{date_label}_\n\n"
            f"{alerts_section}"
            f"*Volume*\n"
            f"  • Emails sent: *{sent}*\n"
            f"  • Ramp: week {week} → expected {current_daily_limit(campaign_id, day)}/mailbox/day\n\n"
            f"*Deliverability* _(reply rate incl. OOO — inbox-placement signal)_\n"
            f"  • Reply rate (incl. OOO): *{reply_rate_incl}%* ({replied_incl_ooo} of {sent}) — {_delivery_flag(reply_rate_incl, sent)}\n"
            f"  • Reply rate (excl. OOO): {reply_rate_excl}% ({replied_excl_ooo} of {sent}) — engagement quality\n"
            f"  • Auto-replies / OOO: {ooo_total} ({auto} auto · {ooo} OOO)\n"
            f"  • Bounces: {bounced} ({bounce_rate}%)\n\n"
            f"*Positive performance*\n"
            f"  • Positive replies: *{positive}* ({positive_of_reply}% of all replies · {positive_per_send:.4f}% of sends)\n\n"
            f"{_benchmark_block(positive_per_send, positive_of_reply, replied_incl_ooo, sent)}\n\n"
            f"{_projection_block(positive_per_send, positive_of_reply, reply_rate_incl)}\n\n"
            f"{booked_section}\n\n"
            f"{interested_section}"
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
        # `replied_count` excludes OOO/auto (see generate_daily_send_report)
        replied_excl_ooo = int(stats.get("replied_count") or 0)
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
        replied_incl_ooo = replied_excl_ooo + ooo_total

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
