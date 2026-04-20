import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

import requests

from ..config import TOMORROW_IO_API_KEY, Station

logger = logging.getLogger(__name__)

BASE_URL = "https://api.tomorrow.io/v4/weather/forecast"


@dataclass
class TomorrowIoForecast:
    source_name: str = "tomorrow_io"
    fetched_at: str = ""
    target_date: str = ""
    det_temp_max_c: Optional[float] = None
    det_temp_min_c: Optional[float] = None
    raw_json: str = ""


def fetch(station: Station, target_date: date) -> Optional[TomorrowIoForecast]:
    if not TOMORROW_IO_API_KEY:
        logger.warning("TOMORROW_IO_API_KEY not set")
        return None

    params = {
        "location": f"{station.lat},{station.lon}",
        "timesteps": "1h",
        "units": "metric",
        "apikey": TOMORROW_IO_API_KEY,
    }
    fetched_at = datetime.now(timezone.utc).isoformat()
    try:
        resp = requests.get(BASE_URL, params=params, timeout=15)
        if resp.status_code == 429:
            logger.warning("Tomorrow.io rate limit exceeded")
            return None
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("Tomorrow.io fetch failed: %s", exc)
        return None

    target_str = str(target_date)
    hourly = data.get("timelines", {}).get("hourly", [])
    day_temps = [
        h["values"]["temperature"]
        for h in hourly
        if h.get("time", "").startswith(target_str) and "temperature" in h.get("values", {})
    ]

    if not day_temps:
        return None

    return TomorrowIoForecast(
        fetched_at=fetched_at,
        target_date=target_str,
        det_temp_max_c=max(day_temps),
        det_temp_min_c=min(day_temps),
        raw_json=json.dumps(data),
    )
