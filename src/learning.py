import json
import os
import time
from typing import Optional

from upstash_redis import Redis


_redis: Redis | None = None

FEW_SHOT_LIMIT = 5
EDIT_RATE_THRESHOLD = 0.40
EDIT_RATE_REVIEW_MIN_SAMPLES = 10


def _get_redis() -> Redis:
    global _redis
    if _redis is None:
        _redis = Redis(
            url=os.getenv("UPSTASH_REDIS_REST_URL"),
            token=os.getenv("UPSTASH_REDIS_REST_TOKEN"),
        )
    return _redis


# ── Logging ──────────────────────────────────────────────────────────────────

def log_interaction(
    email_id: str,
    prospect_email: str,
    prospect_company: str,
    prospect_category: str,
    reply_type: str,
    ai_draft: str,
    final_sent: str,
    was_edited: bool,
    manager: str,
    slack_ts: str,
) -> None:
    """Persist a full interaction record to Redis."""
    r = _get_redis()
    record = {
        "email_id": email_id,
        "prospect_email": prospect_email,
        "prospect_company": prospect_company,
        "prospect_category": prospect_category,
        "reply_type": reply_type,
        "ai_draft": ai_draft,
        "final_sent": final_sent,
        "was_edited": was_edited,
        "edit_diff": _compute_diff(ai_draft, final_sent) if was_edited else "",
        "manager": manager,
        "slack_ts": slack_ts,
        "timestamp": int(time.time()),
    }

    key = f"interaction:{email_id}"
    r.set(key, json.dumps(record), ex=60 * 60 * 24 * 90)  # 90-day TTL

    # Track for few-shot examples (only approved / edited+sent = good quality)
    few_shot_key = f"fewshot:{reply_type}"
    r.lpush(few_shot_key, json.dumps({
        "reply_type": reply_type,
        "prospect_message": "",  # Not stored here to save space; use ai_draft context
        "sent_response": final_sent,
    }))
    r.ltrim(few_shot_key, 0, 19)  # Keep last 20

    # Update edit rate counter
    r.incr(f"stats:{reply_type}:total")
    if was_edited:
        r.incr(f"stats:{reply_type}:edited")


def store_pending(email_id: str, data: dict) -> None:
    """Store pending reply data while waiting for Slack approval."""
    r = _get_redis()
    r.set(f"pending:{email_id}", json.dumps(data), ex=60 * 60 * 48)  # 48h TTL


def get_pending(email_id: str) -> Optional[dict]:
    """Retrieve pending reply data by email_id."""
    r = _get_redis()
    raw = r.get(f"pending:{email_id}")
    if raw:
        return json.loads(raw)
    return None


def delete_pending(email_id: str) -> None:
    r = _get_redis()
    r.delete(f"pending:{email_id}")


# ── Few-shot retrieval ────────────────────────────────────────────────────────

def get_few_shot_examples(reply_type: str, limit: int = FEW_SHOT_LIMIT) -> list[dict]:
    """Return the most recent approved responses for a reply type."""
    r = _get_redis()
    items = r.lrange(f"fewshot:{reply_type}", 0, limit - 1)
    examples = []
    for item in items:
        try:
            examples.append(json.loads(item))
        except Exception:
            continue
    return examples


# ── Analytics ─────────────────────────────────────────────────────────────────

def get_edit_rate(reply_type: str) -> Optional[float]:
    """Return the edit rate (0.0–1.0) for a reply type, or None if insufficient data."""
    r = _get_redis()
    total = int(r.get(f"stats:{reply_type}:total") or 0)
    edited = int(r.get(f"stats:{reply_type}:edited") or 0)

    if total < EDIT_RATE_REVIEW_MIN_SAMPLES:
        return None

    return edited / total


def get_high_edit_rate_types() -> list[dict]:
    """Return reply types where edit rate exceeds the threshold."""
    r = _get_redis()
    # Scan for all reply type stat keys
    results = []
    cursor = 0
    seen = set()
    while True:
        cursor, keys = r.scan(cursor, match="stats:*:total", count=100)
        for key in keys:
            reply_type = key.split(":")[1]
            if reply_type in seen:
                continue
            seen.add(reply_type)
            rate = get_edit_rate(reply_type)
            if rate is not None and rate >= EDIT_RATE_THRESHOLD:
                results.append({"reply_type": reply_type, "edit_rate": round(rate, 2)})
        if cursor == 0:
            break
    return results


# ── Helpers ───────────────────────────────────────────────────────────────────

def _compute_diff(original: str, edited: str) -> str:
    orig_words = set(original.lower().split())
    edit_words = set(edited.lower().split())
    added = len(edit_words - orig_words)
    removed = len(orig_words - edit_words)
    return f"+{added}/-{removed} words"
