import logging
import os

import httpx

log = logging.getLogger(__name__)

BEEHIIV_API_URL = "https://api.beehiiv.com/v2"


async def subscribe_to_newsletter(
    email: str,
    first_name: str = "",
    last_name: str = "",
) -> bool:
    """
    Subscribe a lead to the Beehiiv newsletter as an active subscriber.
    Returns True on success, False on failure.
    """
    api_key = os.getenv("BEEHIIV_API_KEY")
    raw_pub_id = os.getenv("BEEHIIV_PUBLICATION_ID", "")
    pub_id = raw_pub_id if raw_pub_id.startswith("pub_") else f"pub_{raw_pub_id}"

    if not api_key or not pub_id:
        log.error("Beehiiv credentials not set (BEEHIIV_API_KEY / BEEHIIV_PUBLICATION_ID)")
        return False

    payload = {
        "email": email,
        "status": "active",
        "send_welcome_email": False,
    }
    if first_name:
        payload["first_name"] = first_name
    if last_name:
        payload["last_name"] = last_name

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                f"{BEEHIIV_API_URL}/publications/{pub_id}/subscriptions",
                json=payload,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
            )
        if response.status_code in (200, 201):
            log.info(f"Beehiiv: subscribed {email}")
            return True
        elif response.status_code == 409:
            log.info(f"Beehiiv: {email} already subscribed")
            return True
        else:
            log.warning(f"Beehiiv: unexpected status {response.status_code} for {email}: {response.text[:200]}")
            return False
    except Exception as e:
        log.exception(f"Beehiiv: failed to subscribe {email}: {e}")
        return False
