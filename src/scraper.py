import json
import re
from pathlib import Path
from typing import Optional

import httpx
from bs4 import BeautifulSoup


_portfolio: dict | None = None


def _load_portfolio() -> dict:
    global _portfolio
    if _portfolio is None:
        path = Path(__file__).parent.parent / "data" / "client_portfolio.json"
        _portfolio = json.loads(path.read_text())
    return _portfolio


async def scrape_and_classify(website: str) -> tuple[str, list[str]]:
    """
    Scrape a prospect's website and return (category, matching_clients).
    Falls back to ('other', []) if scraping fails.
    """
    if not website:
        return "other", []

    url = website if website.startswith("http") else f"https://{website}"

    try:
        text = await _fetch_page_text(url)
    except Exception:
        return "other", []

    category = _classify_text(text)
    clients = _get_clients_for_category(category)
    return category, clients


async def _fetch_page_text(url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Remove script and style tags
    for tag in soup(["script", "style", "nav", "footer"]):
        tag.decompose()

    text = soup.get_text(separator=" ")
    text = re.sub(r"\s+", " ", text).strip()
    return text[:5000]  # Limit to 5k chars for classification


def _classify_text(text: str) -> str:
    """Match page text against category keywords. Returns best-match category name."""
    portfolio = _load_portfolio()
    text_lower = text.lower()

    scores: dict[str, int] = {}
    for category, data in portfolio["categories"].items():
        if category == "other":
            continue
        score = sum(1 for kw in data["keywords"] if kw in text_lower)
        if score > 0:
            scores[category] = score

    if not scores:
        return "other"

    return max(scores, key=scores.__getitem__)


def _get_clients_for_category(category: str) -> list[str]:
    portfolio = _load_portfolio()
    cats = portfolio["categories"]

    if category in cats and cats[category]["clients"]:
        return cats[category]["clients"][:3]

    return []


def format_client_reference(clients: list[str], category: str) -> str:
    """Format client references for inclusion in a drafted response."""
    portfolio = _load_portfolio()
    fallback = portfolio["fallback_line"]

    if not clients:
        return f"We've worked with {fallback}."

    if len(clients) == 1:
        return f"We've worked with brands like {clients[0]} in a similar space."

    names = ", ".join(clients[:-1]) + f", and {clients[-1]}"
    return f"We've worked with brands like {names} — all in the {category} space."
