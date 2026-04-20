import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

import requests

from ..config import Station

logger = logging.getLogger(__name__)


@dataclass
class NoaaForecast:
    source_name: str = "noaa"
    fetched_at: str = ""
    target_date: str = ""
    det_temp_max_c: Optional[float] = None
    det_temp_min_c: Optional[float] = None
    raw_json: str = ""


def fetch(station: Station, target_date: date) -> Optional[NoaaForecast]:
    """Fetch NWS gridded forecast. Returns None for non-US coordinates."""
    fetched_at = datetime.now(timezone.utc).isoformat()
    try:
        points_resp = requests.get(
            f"https://api.weather.gov/points/{station.lat},{station.lon}",
            headers={"User-Agent": "polymarket-weather-bot/1.0"},
            timeout=10,
        )
        if points_resp.status_code == 404:
            return None
        points_resp.raise_for_status()
        points_data = points_resp.json()
    except requests.exceptions.HTTPError:
        return None
    except Exception as exc:
        logger.warning("NOAA points fetch failed: %s", exc)
        return None

    forecast_url = points_data.get("properties", {}).get("forecast")
    if not forecast_url:
        return None

    try:
        fc_resp = requests.get(
            forecast_url,
            headers={"User-Agent": "polymarket-weather-bot/1.0"},
            timeout=10,
        )
        fc_resp.raise_for_status()
        fc_data = fc_resp.json()
    except Exception as exc:
        logger.warning("NOAA forecast fetch failed: %s", exc)
        return None

    target_str = str(target_date)
    periods = fc_data.get("properties", {}).get("periods", [])
    day_high = None
    day_low = None
    for period in periods:
        start = period.get("startTime", "")
        if start.startswith(target_str):
            temp_f = period.get("temperature")
            if temp_f is not None:
                temp_c = (float(temp_f) - 32) * 5 / 9
                if period.get("isDaytime"):
                    day_high = temp_c
                else:
                    day_low = temp_c

    if day_high is None and day_low is None:
        return None

    return NoaaForecast(
        fetched_at=fetched_at,
        target_date=target_str,
        det_temp_max_c=day_high,
        det_temp_min_c=day_low,
        raw_json=json.dumps(fc_data),
    )
