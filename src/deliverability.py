"""
Deliverability monitor — tracks weekly reply rate trends for the 2 Weeks campaign.

Runs every Monday morning. Pulls weekly stats from PlusVibe, detects:
  - Reply rate below 1.0% (warning)
  - Reply rate below 0.8% (critical)
  - Consecutive weeks declining
  - Positive reply rate trends (good/bad signal)

Posts a Slack alert with full trend breakdown.
"""

import logging
import os
from datetime import date, timedelta

import httpx
from slack_sdk import WebClient

log = logging.getLogger(__name__)

CAMPAIGN_ID = "69971f0aefa0db65892f6b37"  # 2 weeks campaign

THRESHOLD_WARNING  = 1.0   # % — below this is a warning
THRESHOLD_CRITICAL = 0.8   # % — below this is critical / deliverability risk


def _plusvibe_headers() -> dict:
    return {"x-api-key": os.getenv("PLUSVIBE_API_KEY", "")}


def _workspace_id() -> str:
    return os.getenv("PLUSVIBE_WORKSPACE_ID", "")


def _slack() -> WebClient:
    return WebClient(token=os.getenv("SLACK_BOT_TOKEN", ""))


def _channel() -> str:
    return os.getenv("SLACK_CHANNEL_ID", "")


async def _fetch_week_stats(start: date, end: date):
    """Fetch campaign stats for a given week from PlusVibe."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                "https://api.plusvibe.ai/api/v1/campaign/stats",
                headers=_plusvibe_headers(),
                params={
                    "workspace_id": _workspace_id(),
                    "start_date": start.isoformat(),
                    "end_date": end.isoformat(),
                },
            )
            r.raise_for_status()
            data = r.json()
            campaign = next((c for c in data if c["_id"] == CAMPAIGN_ID), None)
            if not campaign:
                return None
            sent     = campaign.get("sent_count", 0)
            replied  = campaign.get("replied_count", 0)
            positive = campaign.get("positive_reply_count", 0)
            return {
                "week_start": start,
                "week_end": end,
                "sent": sent,
                "replied": replied,
                "positive": positive,
                "reply_rate": round(replied / sent * 100, 2) if sent else 0.0,
                "positive_rate": round(positive / replied * 100, 1) if replied else 0.0,
            }
    except Exception as e:
        log.exception(f"Deliverability: failed to fetch stats for {start}: {e}")
        return None


def _trend_arrow(current: float, previous: float) -> str:
    diff = current - previous
    if diff > 0.05:
        return "↑"
    if diff < -0.05:
        return "↓"
    return "→"


async def check_deliverability() -> None:
    """
    Pull the last 4 complete weeks of stats, analyse trends, and post to Slack.
    Called every Monday morning by the scheduler.
    """
    today = date.today()
    # Build last 4 complete Mon-Sun weeks
    # Find last completed Sunday
    days_since_sunday = (today.weekday() + 1) % 7
    last_sunday = today - timedelta(days=days_since_sunday)

    weeks = []
    for i in range(3, -1, -1):  # 4 weeks, oldest first
        week_end   = last_sunday - timedelta(weeks=i)
        week_start = week_end - timedelta(days=6)
        weeks.append((week_start, week_end))

    # Also include current partial week
    current_week_start = last_sunday + timedelta(days=1)
    weeks.append((current_week_start, today))

    stats = []
    for start, end in weeks:
        s = await _fetch_week_stats(start, end)
        if s and s["sent"] > 0:
            stats.append(s)

    if not stats:
        log.warning("Deliverability: no stats available")
        return

    current = stats[-1]
    previous = stats[-2] if len(stats) >= 2 else None

    # Determine alert level
    rate = current["reply_rate"]
    if rate < THRESHOLD_CRITICAL:
        level = "critical"
    elif rate < THRESHOLD_WARNING:
        level = "warning"
    else:
        level = "healthy"

    # Check for consecutive declining weeks
    declining_weeks = 0
    for i in range(len(stats) - 1, 0, -1):
        if stats[i]["reply_rate"] < stats[i - 1]["reply_rate"]:
            declining_weeks += 1
        else:
            break

    # Build trend table
    trend_lines = []
    for i, s in enumerate(stats):
        label = s["week_start"].strftime("%-d %b") + " – " + s["week_end"].strftime("%-d %b")
        partial = " *(partial)*" if s["week_end"] >= today else ""
        arrow = _trend_arrow(s["reply_rate"], stats[i - 1]["reply_rate"]) if i > 0 else "–"
        pos_arrow = _trend_arrow(s["positive_rate"], stats[i - 1]["positive_rate"]) if i > 0 else "–"
        trend_lines.append(
            f"• {label}{partial}: *{s['reply_rate']}%* reply rate {arrow} | *{s['positive_rate']}%* positive {pos_arrow} | {s['sent']:,} sent"
        )

    trend_text = "\n".join(trend_lines)

    # Header and colour based on level
    if level == "critical":
        header = "🚨 *CRITICAL: Reply Rate Below 0.8% — Possible Deliverability Issue*"
        action = "• *Immediate action needed* — check domain health, warm-up status, and sending volume\n• Reduce daily send volume and monitor bounce rate"
    elif level == "warning":
        header = "⚠️ *Warning: Reply Rate Below 1.0%*"
        action = "• Monitor closely — reduce volume if rate continues dropping\n• Check for recent domain/IP changes"
    else:
        header = "✅ *Reply Rate Healthy*"
        action = "• No action needed — keep monitoring weekly"

    # Consecutive decline note
    decline_note = ""
    if declining_weeks >= 2:
        decline_note = f"\n• :chart_with_downwards_trend: *{declining_weeks} consecutive weeks declining* — trend needs attention"
    elif declining_weeks == 0 and len(stats) >= 2 and current["reply_rate"] > stats[-2]["reply_rate"]:
        decline_note = "\n• :chart_with_upwards_trend: Reply rate recovering from last week"

    # Positive rate insight
    pos_note = ""
    if previous:
        pos_diff = current["positive_rate"] - previous["positive_rate"]
        if pos_diff >= 2:
            pos_note = f"\n• :star: Positive reply rate up {pos_diff:.1f}pp — lead quality improving"
        elif pos_diff <= -2:
            pos_note = f"\n• :warning: Positive reply rate down {abs(pos_diff):.1f}pp — replies are less qualified"

    text = (
        f"{header}\n\n"
        f"*4-Week Trend (2 Weeks Campaign)*\n{trend_text}\n\n"
        f"*Analysis*\n{action}{decline_note}{pos_note}\n\n"
        f"*Thresholds:* Healthy ≥1.0% | Warning <1.0% | Critical <0.8%"
    )

    _slack().chat_postMessage(channel=_channel(), text=text)
    log.info(f"Deliverability check posted — level={level}, rate={rate}%, declining_weeks={declining_weeks}")


async def check_daily_recovery() -> None:
    """
    End-of-day recovery check-in. Compares today vs yesterday and posts a
    concise Slack update. Runs daily at 8pm EST.
    """
    today = date.today()
    yesterday = today - timedelta(days=1)

    today_stats    = await _fetch_week_stats(today, today)
    yesterday_stats = await _fetch_week_stats(yesterday, yesterday)

    # If no sends today yet, skip silently
    if not today_stats or today_stats["sent"] == 0:
        log.info("Daily recovery check: no sends recorded for today yet — skipping")
        return

    s = today_stats
    p = yesterday_stats

    def delta(curr, prev, field):
        if not prev:
            return ""
        d = curr[field] - prev[field]
        arrow = "↑" if d > 0 else ("↓" if d < 0 else "→")
        return f" ({arrow}{abs(round(d, 2))})"

    reply_d   = delta(s, p, "reply_rate")
    bounce_d  = delta(s, p, "bounce_rate") if p else ""
    pos_d     = delta(s, p, "positive_rate")
    sent_d    = delta(s, p, "sent")

    # Bounce rate assessment
    bounce_rate = s.get("bounce_rate", 0.0)
    if bounce_rate < 2.0:
        bounce_flag = "✅"
    elif bounce_rate < 4.0:
        bounce_flag = "⚠️"
    else:
        bounce_flag = "🚨"

    # Reply rate assessment
    rate = s["reply_rate"]
    if rate >= 1.0:
        rate_flag = "✅"
    elif rate >= 0.8:
        rate_flag = "⚠️"
    else:
        rate_flag = "🔴"

    # Overall signal
    if p:
        rate_improving   = s["reply_rate"] > p["reply_rate"]
        bounce_improving = s["bounce_rate"] <= p["bounce_rate"] if p.get("bounce_rate") else True
        if rate_improving and bounce_improving:
            signal = "📈 *Recovery trending in the right direction.*"
        elif rate_improving:
            signal = "📈 Reply rate improving — bounce rate still elevated, keep watching."
        elif bounce_improving:
            signal = "📉 Bounce rate easing — reply rate still low, give it more time."
        else:
            signal = "⚠️ Both reply rate and bounce rate moving the wrong way — review sending settings."
    else:
        signal = "No previous day to compare."

    date_label = today.strftime("%-d %b %Y")
    text = (
        f"📡 *End-of-Day Recovery Check-In — {date_label}*\n\n"
        f"*Today's Numbers (2 Weeks Campaign)*\n"
        f"• Sends: *{s['sent']:,}*{sent_d}\n"
        f"• Reply Rate: {rate_flag} *{s['reply_rate']}%*{reply_d}\n"
        f"• Bounce Rate: {bounce_flag} *{bounce_rate}%*{bounce_d}\n"
        f"• Positive Reply Rate: *{s['positive_rate']}%*{pos_d} _(of replies that were interested)_\n"
        f"• Replies Received: *{s['replied']}*\n\n"
        f"*Signal*\n{signal}\n\n"
        f"_Thresholds: Reply rate healthy ≥1.0% | Warning <1.0% | Critical <0.8% — Bounce healthy <2%_"
    )

    _slack().chat_postMessage(channel=_channel(), text=text)
    log.info(f"Daily recovery check posted — rate={rate}%, bounce={bounce_rate}%, sent={s['sent']}")


async def check_weekly_deliverability_summary() -> None:
    """
    Friday end-of-week deliverability summary. Pulls each day of the current
    Mon–Fri week, shows the daily table, week totals, and a recovery assessment.
    Runs every Friday at 7pm EST alongside the pipeline report.
    """
    today = date.today()

    # Build Mon–today for current week
    days_since_monday = today.weekday()  # Mon=0
    monday = today - timedelta(days=days_since_monday)

    daily = []
    for i in range(days_since_monday + 1):
        d = monday + timedelta(days=i)
        s = await _fetch_week_stats(d, d)
        if s and s["sent"] > 0:
            daily.append(s)

    if not daily:
        log.warning("Weekly deliverability summary: no data for this week")
        return

    # Also fetch last week's totals for week-over-week comparison
    last_monday = monday - timedelta(weeks=1)
    last_friday = last_monday + timedelta(days=4)
    last_week = await _fetch_week_stats(last_monday, last_friday)

    # Totals for this week
    total_sent    = sum(d["sent"] for d in daily)
    total_replied = sum(d["replied"] for d in daily)
    total_bounce  = sum(int(d["sent"] * d.get("bounce_rate", 0) / 100) for d in daily)
    week_reply_rate  = round(total_replied / total_sent * 100, 2) if total_sent else 0.0
    week_bounce_rate = round(total_bounce / total_sent * 100, 2) if total_sent else 0.0

    # Day-by-day table
    day_lines = []
    for i, s in enumerate(daily):
        label = s["week_start"].strftime("%a %-d %b")
        arrow = _trend_arrow(s["reply_rate"], daily[i - 1]["reply_rate"]) if i > 0 else "–"
        bounce = s.get("bounce_rate", 0.0)
        bounce_flag = "✅" if bounce < 2 else ("⚠️" if bounce < 4 else "🚨")
        day_lines.append(
            f"• {label}: *{s['reply_rate']}%* reply {arrow} | {bounce_flag} {bounce}% bounce | {s['sent']:,} sent | {s['replied']} replies"
        )
    day_table = "\n".join(day_lines)

    # Week-over-week
    wow_line = ""
    if last_week and last_week["sent"] > 0:
        wow_diff = round(week_reply_rate - last_week["reply_rate"], 2)
        wow_arrow = "↑" if wow_diff > 0 else ("↓" if wow_diff < 0 else "→")
        wow_line = (
            f"\n*Week-over-Week*\n"
            f"• This week avg reply rate: *{week_reply_rate}%* vs last week *{last_week['reply_rate']}%* "
            f"({wow_arrow}{abs(wow_diff)}pp)\n"
            f"• This week sends: *{total_sent:,}* vs last week *{last_week['sent']:,}*"
        )

    # Recovery assessment
    if len(daily) >= 3:
        recent = daily[-3:]
        improving = all(recent[i]["reply_rate"] >= recent[i-1]["reply_rate"] for i in range(1, len(recent)))
        if improving:
            assessment = "✅ *Reply rate has improved consistently over the last 3 days.* Recovery is on track."
        elif week_reply_rate >= 1.0:
            assessment = "✅ *Weekly average crossed 1.0%.* Deliverability is healthy — stay the course."
        elif week_bounce_rate < 2.0:
            assessment = "📈 *Bounce rate is under control.* Reply rate still recovering — expect improvement next week."
        else:
            assessment = "⚠️ *Bounce rate still elevated.* Avoid increasing volume until bounce falls below 2%."
    else:
        assessment = "Not enough daily data points this week to assess trend."

    week_label = monday.strftime("%-d %b") + " – " + today.strftime("%-d %b %Y")
    text = (
        f"📊 *Weekly Deliverability Summary — {week_label}*\n\n"
        f"*Day-by-Day Breakdown*\n{day_table}\n\n"
        f"*Week Totals*\n"
        f"• Total sent: *{total_sent:,}* | Total replies: *{total_replied}*\n"
        f"• Avg reply rate: *{week_reply_rate}%* | Avg bounce rate: *{week_bounce_rate}%*"
        f"{wow_line}\n\n"
        f"*Recovery Assessment*\n{assessment}\n\n"
        f"_Thresholds: Healthy ≥1.0% | Warning <1.0% | Critical <0.8% — Bounce healthy <2%_"
    )

    _slack().chat_postMessage(channel=_channel(), text=text)
    log.info(f"Weekly deliverability summary posted — week_reply_rate={week_reply_rate}%, sends={total_sent}")
