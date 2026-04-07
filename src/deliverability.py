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


async def _fetch_week_stats(start: date, end: date) -> dict | None:
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
