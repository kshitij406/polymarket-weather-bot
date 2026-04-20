import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://aviationweather.gov/api/data"


@dataclass
class MetarObservation:
    station_id: str
    observed_at: str
    temp_c: Optional[float]
    dewpoint_c: Optional[float]
    wind_kt: Optional[float]
    vis_m: Optional[float]
    raw_metar: str


@dataclass
class TafForecast:
    station_id: str
    issued_at: str
    raw_taf: str


def fetch_metar(icao: str) -> Optional[MetarObservation]:
    try:
        resp = requests.get(
            f"{BASE_URL}/metar",
            params={"ids": icao, "format": "json"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("METAR fetch failed for %s: %s", icao, exc)
        return None

    if not data:
        return None

    obs = data[0] if isinstance(data, list) else data
    temp = obs.get("temp") or obs.get("tmpf")
    if temp is not None and obs.get("temp") is None:
        temp = (float(temp) - 32) * 5 / 9

    dewp = obs.get("dewp") or obs.get("dwpf")
    if dewp is not None and obs.get("dewp") is None:
        dewp = (float(dewp) - 32) * 5 / 9

    raw = obs.get("rawOb") or obs.get("raw_text") or ""

    if temp is None and raw:
        m = re.search(r"(?<!\w)M?(\d{2})/M?(\d{2})(?!\w)", raw)
        if m:
            t_str = m.group(1)
            neg = raw[raw.index(m.group(0)) - 1] == "M" if raw.index(m.group(0)) > 0 else False
            temp = -float(t_str) if neg else float(t_str)

    return MetarObservation(
        station_id=icao,
        observed_at=obs.get("reportTime") or obs.get("observation_time") or datetime.now(timezone.utc).isoformat(),
        temp_c=float(temp) if temp is not None else None,
        dewpoint_c=float(dewp) if dewp is not None else None,
        wind_kt=obs.get("wspd") or obs.get("wind_speed_kt"),
        vis_m=obs.get("visib"),
        raw_metar=raw,
    )


def fetch_taf(icao: str) -> Optional[TafForecast]:
    try:
        resp = requests.get(
            f"{BASE_URL}/taf",
            params={"ids": icao, "format": "json"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("TAF fetch failed for %s: %s", icao, exc)
        return None

    if not data:
        return None

    taf = data[0] if isinstance(data, list) else data
    return TafForecast(
        station_id=icao,
        issued_at=taf.get("issueTime") or taf.get("issue_time") or "",
        raw_taf=taf.get("rawTAF") or taf.get("raw_text") or json.dumps(taf),
    )
