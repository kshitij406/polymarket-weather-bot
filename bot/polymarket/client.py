import logging
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://gamma-api.polymarket.com"
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "polymarket-weather-bot/1.0"})


def _get(path: str, params: dict | None = None, max_retries: int = 5) -> Any:
    url = f"{BASE_URL}{path}"
    delay = 1.0
    for attempt in range(max_retries):
        try:
            resp = _SESSION.get(url, params=params, timeout=20)
            if resp.status_code == 429:
                time.sleep(delay)
                delay = min(delay * 2, 32)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as exc:
            if resp.status_code >= 500 and attempt < max_retries - 1:
                time.sleep(delay)
                delay = min(delay * 2, 32)
                continue
            raise
        except requests.exceptions.ConnectionError as exc:
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay = min(delay * 2, 32)
                continue
            raise
    raise RuntimeError(f"Max retries exceeded for {url}")


def get_markets(params: dict | None = None) -> list[dict]:
    results = []
    offset = 0
    limit = 100
    base_params = dict(params or {})
    base_params["limit"] = limit
    while True:
        base_params["offset"] = offset
        batch = _get("/markets", base_params)
        if not batch:
            break
        results.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return results


def get_events(params: dict | None = None) -> list[dict]:
    return _get("/events", params) or []


def search_markets(query: str) -> list[dict]:
    return _get("/public-search", {"q": query}) or []
