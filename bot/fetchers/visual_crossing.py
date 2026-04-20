import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

import requests

from ..config import VISUAL_CROSSING_API_KEY, Station

logger = logging.getLogger(__name__)

BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"


@dataclass
class VisualCrossingForecast:
    source_name: str = "visual_crossing"
    fetched_at: str = ""
    target_date: str = ""
    det_temp_max_c: Optional[float] = None
    det_temp_min_c: Optional[float] = None
    det_temp_avg_c: Optional[float] = None
    raw_json: str = ""


def fetch(station: Station, target_date: date) -> Optional[VisualCrossingForecast]:
    if not VISUAL_CROSSING_API_KEY:
        logger.warning("VISUAL_CROSSING_API_KEY not set")
        return None

    url = f"{BASE_URL}/{station.lat},{station.lon}/{target_date}"
    params = {
        "key": VISUAL_CROSSING_API_KEY,
        "unitGroup": "metric",
        "contentType": "json",
        "include": "days",
    }
    fetched_at = datetime.now(timezone.utc).isoformat()
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("Visual Crossing fetch failed: %s", exc)
        return None

    days = data.get("days", [])
    if not days:
        return None

    day = days[0]
    return VisualCrossingForecast(
        fetched_at=fetched_at,
        target_date=str(target_date),
        det_temp_max_c=day.get("tempmax"),
        det_temp_min_c=day.get("tempmin"),
        det_temp_avg_c=day.get("temp"),
        raw_json=json.dumps(data),
    )
