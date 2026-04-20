import json
import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Optional

import requests

from ..config import Station

logger = logging.getLogger(__name__)

BASE_ENSEMBLE = "https://ensemble-api.open-meteo.com/v1/ensemble"
BASE_ARCHIVE = "https://api.open-meteo.com/v1/archive"


@dataclass
class EnsembleForecast:
    source_name: str
    model: str
    fetched_at: str
    target_date: str
    member_daily_max_temps: list[float]
    raw_json: str


@dataclass
class DeterministicForecast:
    source_name: str
    fetched_at: str
    target_date: str
    det_temp_max_c: Optional[float]
    det_temp_min_c: Optional[float]
    raw_json: str


def _get_with_retry(url: str, params: dict, max_retries: int = 3) -> dict:
    delay = 1.0
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                time.sleep(delay)
                delay *= 2
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as exc:
            if attempt == max_retries - 1:
                raise
            time.sleep(delay)
            delay *= 2
    raise RuntimeError(f"Max retries exceeded for {url}")


def _fetch_ensemble(station: Station, model: str, source_name: str, target_date: date) -> Optional[EnsembleForecast]:
    params = {
        "latitude": station.lat,
        "longitude": station.lon,
        "hourly": "temperature_2m",
        "models": model,
        "start_date": str(target_date),
        "end_date": str(target_date),
        "timezone": station.timezone,
    }
    fetched_at = datetime.now(timezone.utc).isoformat()
    try:
        data = _get_with_retry(BASE_ENSEMBLE, params)
    except Exception as exc:
        logger.warning("Open-Meteo %s fetch failed: %s", source_name, exc)
        return None

    hourly = data.get("hourly", {})
    member_keys = [k for k in hourly if k.startswith("temperature_2m_member")]
    if not member_keys:
        logger.warning("No ensemble members in Open-Meteo %s response", source_name)
        return None

    if len(member_keys) < 10:
        logger.warning("Only %d members returned for %s", len(member_keys), source_name)

    member_max_temps = []
    for key in sorted(member_keys):
        vals = [v for v in hourly[key] if v is not None]
        if vals:
            member_max_temps.append(max(vals))

    return EnsembleForecast(
        source_name=source_name,
        model=model,
        fetched_at=fetched_at,
        target_date=str(target_date),
        member_daily_max_temps=member_max_temps,
        raw_json=json.dumps(data),
    )


def fetch_ecmwf_ensemble(station: Station, target_date: date) -> Optional[EnsembleForecast]:
    return _fetch_ensemble(station, "ecmwf_ifs025", "ecmwf_ensemble", target_date)


def fetch_gfs_ensemble(station: Station, target_date: date) -> Optional[EnsembleForecast]:
    return _fetch_ensemble(station, "gfs_seamless", "gfs_ensemble", target_date)


def fetch_ukmo(station: Station, target_date: date) -> Optional[EnsembleForecast]:
    return _fetch_ensemble(station, "ukmo_seamless", "ukmo", target_date)


def fetch_era5_climatology(station: Station, target_date: date, window_days: int = 7, years: int = 30) -> Optional[DeterministicForecast]:
    """Fetch ERA5 daily max temps for a ±window_days calendar window over the last `years` years."""
    from datetime import timedelta
    import calendar

    current_year = datetime.now().year
    start_year = current_year - years
    mm = target_date.month
    dd = target_date.day

    all_temps: list[float] = []
    raw_records = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for year in range(start_year, current_year):
        window_start = date(year, mm, dd) - timedelta(days=window_days)
        window_end = date(year, mm, dd) + timedelta(days=window_days)

        if window_start.year != year:
            window_start = date(year, 1, 1)
        if window_end.year != year:
            window_end = date(year, 12, 31)

        params = {
            "latitude": station.lat,
            "longitude": station.lon,
            "daily": "temperature_2m_max",
            "start_date": str(window_start),
            "end_date": str(window_end),
            "timezone": station.timezone,
        }
        try:
            data = _get_with_retry(BASE_ARCHIVE, params)
            temps = [v for v in data.get("daily", {}).get("temperature_2m_max", []) if v is not None]
            all_temps.extend(temps)
            raw_records.append({"year": year, "n": len(temps)})
        except Exception as exc:
            logger.warning("ERA5 fetch for year %d failed: %s", year, exc)

    if not all_temps:
        return None

    import numpy as np
    return DeterministicForecast(
        source_name="era5_climatology",
        fetched_at=fetched_at,
        target_date=str(target_date),
        det_temp_max_c=float(np.mean(all_temps)),
        det_temp_min_c=float(np.std(all_temps, ddof=1)),
        raw_json=json.dumps({"n_samples": len(all_temps), "records": raw_records, "all_temps": all_temps}),
    )
