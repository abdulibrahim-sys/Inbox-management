"""
Campaign + agent metrics.

Two sources, reported side by side:

  PlusVibe campaign stats  → sent, replies, positive replies, bounces, unsubs
                             (source of truth for rates)
  Agent counters (Redis)   → what the agent did with positive replies:
                             posted to Slack, drafts approved / edited,
                             escalations, meetings booked, call outcomes

Counters live in Upstash Redis so they survive Railway restarts:
  metrics:{campaign_id}:total       hash of event → count (lifetime)
  metrics:{campaign_id}:{YYYY-MM-DD} hash of event → count (UTC day)

Rates follow the team's definitions:
  reply rate          = replies / sent
  positive reply rate = positive replies / total replies (NOT / sent)
  bounce rate         = bounced / sent (healthy under 2%)
"""
import logging
import os
from datetime import date, datetime, timedelta, timezone

from src.integrations.plusvibe import get_campaign_stats

log = logging.getLogger(__name__)

DAY_TTL = 60 * 60 * 24 * 120      # daily buckets kept ~4 months
SEEN_TTL = 60 * 60 * 24 * 90      # reply dedup keys kept 90 days
BOUNCE_ALERT_PCT = 2.0

# Event names — keep in sync with the report layout below.
POSITIVE_REPLY = "positive_reply"
MEETING_BOOKED = "meeting_booked"
DRAFT_POSTED = "draft_posted"
ESCALATED = "escalated"
DISREGARDED = "disregarded"
SUPPRESSED = "suppressed"
SILENT = "silent"                  # park / stop — no Slack post
SENT_APPROVED = "sent_approved"
SENT_EDITED = "sent_edited"
CALL_SHOWED = "call_showed"
CALL_NO_SHOW = "call_no_show"
CALL_NOT_QUALIFIED = "call_not_qualified"


_redis = None


def _get_redis():
    global _redis
    if _redis is not None:
        return _redis
    try:
        from upstash_redis import Redis
        url = os.getenv("UPSTASH_REDIS_REST_URL")
        tok = os.getenv("UPSTASH_REDIS_REST_TOKEN")
        if not url or not tok:
            log.warning("metrics: no Upstash config; counters disabled")
            return None
        _redis = Redis(url=url, token=tok)
        return _redis
    except Exception as e:
        log.warning(f"metrics: redis init failed: {e}")
        return None


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def record(event: str, campaign_id: str = "") -> None:
    """Increment an agent counter. Never raises — metrics must not break the pipeline."""
    r = _get_redis()
    if not r:
        return
    cid = campaign_id or "unknown"
    try:
        day_key = f"metrics:{cid}:{_today()}"
        r.hincrby(f"metrics:{cid}:total", event, 1)
        r.hincrby(day_key, event, 1)
        r.expire(day_key, DAY_TTL)
    except Exception as e:
        log.warning(f"metrics.record({event}) failed: {e}")


def claim_reply(record_id: str) -> bool:
    """
    Return True the first time a reply id is seen, False after.

    Shared by the unibox poller and the webhook so a reply is posted to Slack
    (and counted) once, including across restarts. Fails open if Redis is
    down: a duplicate Slack post beats a missed positive reply.
    """
    r = _get_redis()
    if not r or not record_id:
        return True
    try:
        return bool(r.set(f"seen:reply:{record_id}", "1", nx=True, ex=SEEN_TTL))
    except Exception as e:
        log.warning(f"metrics.claim_reply failed: {e}")
        return True


def get_counters(campaign_id: str, day: str | None = None) -> dict[str, int]:
    r = _get_redis()
    if not r:
        return {}
    key = f"metrics:{campaign_id}:{day}" if day else f"metrics:{campaign_id}:total"
    try:
        raw = r.hgetall(key) or {}
        return {k: int(v) for k, v in raw.items()}
    except Exception as e:
        log.warning(f"metrics.get_counters failed: {e}")
        return {}


def _pct(num: int, den: int) -> float | None:
    return round(100.0 * num / den, 2) if den else None


def _summarise_stats(row: dict | None) -> dict:
    row = row or {}
    sent = int(row.get("sent_count") or 0)
    replies = int(row.get("replied_count") or 0)
    positive = int(row.get("positive_reply_count") or 0)
    bounced = int(row.get("bounced_count") or 0)
    return {
        "sent": sent,
        "new_leads_contacted": int(row.get("new_lead_contacted_count") or 0),
        "replies": replies,
        "positive_replies": positive,
        "bounced": bounced,
        "unsubscribed": int(row.get("unsubscribed_count") or 0),
        "reply_rate_pct": _pct(replies, sent),
        "positive_reply_rate_pct": _pct(positive, replies),
        "bounce_rate_pct": _pct(bounced, sent),
    }


async def build_snapshot(campaign_id: str, day: date | None = None) -> dict:
    """
    Metrics for one campaign: the given UTC day (default yesterday),
    the trailing 7 days ending that day, and lifetime.
    """
    day = day or (datetime.now(timezone.utc).date() - timedelta(days=1))
    week_start = day - timedelta(days=6)
    today = datetime.now(timezone.utc).date()

    day_row = await get_campaign_stats(campaign_id, day.isoformat(), day.isoformat())
    week_row = await get_campaign_stats(campaign_id, week_start.isoformat(), day.isoformat())
    life_row = await get_campaign_stats(campaign_id, "2020-01-01", today.isoformat())

    week_counters: dict[str, int] = {}
    for i in range(7):
        d = (week_start + timedelta(days=i)).isoformat()
        for k, v in get_counters(campaign_id, d).items():
            week_counters[k] = week_counters.get(k, 0) + v

    return {
        "campaign_id": campaign_id,
        "campaign_name": (life_row or {}).get("camp_name") or campaign_id,
        "status": (life_row or {}).get("status") or "",
        "day": day.isoformat(),
        "plusvibe": {
            "day": _summarise_stats(day_row),
            "last_7_days": _summarise_stats(week_row),
            "lifetime": _summarise_stats(life_row),
        },
        "agent": {
            "day": get_counters(campaign_id, day.isoformat()),
            "last_7_days": week_counters,
            "lifetime": get_counters(campaign_id),
        },
    }
