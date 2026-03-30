import json
import logging
import os
import time

import httpx
from upstash_redis import Redis

log = logging.getLogger(__name__)

BEEHIIV_API_URL = "https://api.beehiiv.com/v2"
RETRY_QUEUE_KEY = "beehiiv:retry_queue"

_redis: Redis | None = None


def _get_redis() -> Redis:
    global _redis
    if _redis is None:
        _redis = Redis(
            url=os.getenv("UPSTASH_REDIS_REST_URL"),
            token=os.getenv("UPSTASH_REDIS_REST_TOKEN"),
        )
    return _redis


def _pub_id() -> str:
    raw = os.getenv("BEEHIIV_PUBLICATION_ID", "")
    return raw if raw.startswith("pub_") else f"pub_{raw}"


async def _call_beehiiv(email: str, first_name: str, last_name: str) -> str:
    """
    Call the Beehiiv API. Returns 'ok', 'exists', or 'failed'.
    """
    api_key = os.getenv("BEEHIIV_API_KEY")
    pub_id = _pub_id()

    if not api_key or not pub_id:
        log.error("Beehiiv credentials not set")
        return "failed"

    payload: dict = {"email": email, "status": "active", "send_welcome_email": False}
    if first_name:
        payload["first_name"] = first_name
    if last_name:
        payload["last_name"] = last_name

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(
                f"{BEEHIIV_API_URL}/publications/{pub_id}/subscriptions",
                json=payload,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            )
        if r.status_code in (200, 201):
            return "ok"
        if r.status_code == 409:
            return "exists"
        log.warning(f"Beehiiv {r.status_code} for {email}: {r.text[:200]}")
        return "failed"
    except Exception as e:
        log.exception(f"Beehiiv request failed for {email}: {e}")
        return "failed"


async def subscribe_to_newsletter(
    email: str,
    first_name: str = "",
    last_name: str = "",
) -> bool:
    """
    Subscribe a lead. On failure, adds to Redis retry queue for the 24h scheduler.
    Returns True on success or already-subscribed, False on failure.
    """
    result = await _call_beehiiv(email, first_name, last_name)

    if result in ("ok", "exists"):
        log.info(f"Beehiiv: {'subscribed' if result == 'ok' else 'already subscribed'} {email}")
        # Remove from retry queue if it was previously queued
        _remove_from_retry(email)
        return True

    # Failed — add to retry queue
    log.warning(f"Beehiiv: subscription failed for {email}, adding to retry queue")
    _queue_for_retry(email, first_name, last_name)
    return False


def _queue_for_retry(email: str, first_name: str, last_name: str) -> None:
    try:
        r = _get_redis()
        entry = json.dumps({
            "email": email,
            "first_name": first_name,
            "last_name": last_name,
            "queued_at": int(time.time()),
        })
        r.hset(RETRY_QUEUE_KEY, email, entry)
    except Exception as e:
        log.exception(f"Failed to queue {email} for Beehiiv retry: {e}")


def _remove_from_retry(email: str) -> None:
    try:
        _get_redis().hdel(RETRY_QUEUE_KEY, email)
    except Exception:
        pass


def get_retry_queue() -> list[dict]:
    """Return all leads pending Beehiiv subscription retry."""
    try:
        r = _get_redis()
        entries = r.hvals(RETRY_QUEUE_KEY)
        return [json.loads(e) for e in entries]
    except Exception as e:
        log.exception(f"Failed to read Beehiiv retry queue: {e}")
        return []


async def process_retry_queue() -> dict:
    """
    Retry all failed Beehiiv subscriptions. Called by the 24h scheduler.
    Returns counts: {retried, succeeded, still_failing}.
    """
    leads = get_retry_queue()
    log.info(f"Beehiiv retry: {len(leads)} leads in queue")
    counts = {"retried": len(leads), "succeeded": 0, "still_failing": 0}

    for lead in leads:
        result = await _call_beehiiv(
            lead["email"], lead.get("first_name", ""), lead.get("last_name", "")
        )
        if result in ("ok", "exists"):
            _remove_from_retry(lead["email"])
            counts["succeeded"] += 1
            log.info(f"Beehiiv retry succeeded: {lead['email']}")
        else:
            counts["still_failing"] += 1
            log.warning(f"Beehiiv retry still failing: {lead['email']}")

    return counts
