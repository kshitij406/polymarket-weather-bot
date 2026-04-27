import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

import requests

from ..config import WUNDERGROUND_API_KEY

logger = logging.getLogger(__name__)

_SCRAPED_KEY_CACHE: dict[str, str] = {}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


@dataclass
class WundergroundObservation:
    station_id: str
    date: date
    temp_high_c: Optional[float]
    temp_low_c: Optional[float]
    temp_avg_c: Optional[float]
    fetched_at: str
    raw_json: str


def _scrape_api_key(station_id: str, target_date: date) -> Optional[str]:
    """Extract Wunderground API key from page HTML/inline JS."""
    cache_key = "global"
    if cache_key in _SCRAPED_KEY_CACHE:
        return _SCRAPED_KEY_CACHE[cache_key]

    date_str = target_date.strftime("%Y-%m-%d")
    url = f"https://www.wunderground.com/history/daily/EGLC/date/{date_str}"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
        html = resp.text
        match = re.search(r'apiKey["\s:=]+([a-f0-9]{32})', html)
        if match:
            key = match.group(1)
            _SCRAPED_KEY_CACHE[cache_key] = key
            logger.info("Scraped Wunderground API key successfully")
            return key
        logger.warning("Could not find apiKey pattern in Wunderground page HTML (len=%d)", len(html))
    except Exception as exc:
        logger.warning("Failed to scrape Wunderground API key: %s", exc)
    return None


def _get_api_key(station_id: str, target_date: date) -> Optional[str]:
    if WUNDERGROUND_API_KEY:
        return WUNDERGROUND_API_KEY
    return _scrape_api_key(station_id, target_date)


def fetch(station_id: str, target_date: date) -> Optional[WundergroundObservation]:
    api_key = _get_api_key(station_id, target_date)
    if not api_key:
        logger.warning("No Wunderground API key available for %s %s", station_id, target_date)
        return None

    date_str = target_date.strftime("%Y%m%d")
    url = "https://api.weather.com/v2/pws/history/daily"
    params = {
        "stationId": station_id,
        "format": "json",
        "units": "m",
        "startDate": date_str,
        "endDate": date_str,
        "apiKey": api_key,
        "numericPrecision": "decimal",
    }
    fetched_at = datetime.now(timezone.utc).isoformat()

    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code in (401, 403):
            _SCRAPED_KEY_CACHE.clear()
            logger.warning("Wunderground API key rejected (HTTP %d), clearing cache", resp.status_code)
            return None
        if resp.status_code == 204 or not resp.text.strip():
            logger.info("No Wunderground data yet for %s %s (HTTP %d)", station_id, target_date, resp.status_code)
            return WundergroundObservation(
                station_id=station_id,
                date=target_date,
                temp_high_c=None,
                temp_low_c=None,
                temp_avg_c=None,
                fetched_at=fetched_at,
                raw_json="{}",
            )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("Wunderground fetch failed for %s %s: %s", station_id, target_date, exc)
        return None

    observations = data.get("observations", [])
    if not observations:
        logger.info("No Wunderground observations yet for %s %s", station_id, target_date)
        return WundergroundObservation(
            station_id=station_id,
            date=target_date,
            temp_high_c=None,
            temp_low_c=None,
            temp_avg_c=None,
            fetched_at=fetched_at,
            raw_json=json.dumps(data),
        )

    obs = observations[0]
    metric = obs.get("metric", obs.get("metric_si", {}))
    return WundergroundObservation(
        station_id=station_id,
        date=target_date,
        temp_high_c=metric.get("tempHigh"),
        temp_low_c=metric.get("tempLow"),
        temp_avg_c=metric.get("tempAvg"),
        fetched_at=fetched_at,
        raw_json=json.dumps(data),
    )
