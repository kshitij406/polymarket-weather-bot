import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Station:
    icao: str
    wunderground_id: str
    lat: float
    lon: float
    elevation_m: int
    timezone: str
    display_name: str
    polymarket_regex: str


STATIONS: list[Station] = [
    Station(
        icao="EGLC",
        wunderground_id="EGLC:9:GB",
        lat=51.5048,
        lon=-0.0495,
        elevation_m=4,
        timezone="Europe/London",
        display_name="London City Airport",
        polymarket_regex=r"(?i)(London|EGLC|City Airport)",
    ),
]

WUNDERGROUND_API_KEY: str = os.environ.get("WUNDERGROUND_API_KEY", "")
VISUAL_CROSSING_API_KEY: str = os.environ.get("VISUAL_CROSSING_API_KEY", "")
TOMORROW_IO_API_KEY: str = os.environ.get("TOMORROW_IO_API_KEY", "")
DISCORD_WEBHOOK_URL: str = os.environ.get("DISCORD_WEBHOOK_URL", "")

EDGE_THRESHOLD_PP: float = 10.0
HYPOTHETICAL_STAKE_USD: float = 10.0

DB_PATH: Path = Path(__file__).parent.parent / "data" / "weather_bot.db"

CLIMATOLOGY_WINDOW_DAYS: int = 7
CLIMATOLOGY_YEARS: int = 30
BIAS_CORRECTION_MIN_SAMPLES: int = 20
BIAS_CORRECTION_MAX_OFFSET_C: float = 3.0

SOURCE_WEIGHTS: dict[str, list[tuple[int, float]]] = {
    "ecmwf_ensemble":  [(1, 0.55), (3, 0.50), (5, 0.45), (7, 0.40), (10, 0.30)],
    "gfs_ensemble":    [(1, 0.20), (3, 0.20), (5, 0.20), (7, 0.20), (10, 0.20)],
    "ukmo":            [(1, 0.10), (3, 0.10), (5, 0.10), (7, 0.10), (10, 0.08)],
    "visual_crossing": [(1, 0.08), (3, 0.10), (5, 0.12), (7, 0.13), (10, 0.15)],
    "tomorrow_io":     [(1, 0.05), (3, 0.07), (5, 0.08), (7, 0.10), (10, 0.12)],
    "noaa":            [(1, 0.02), (3, 0.03), (5, 0.05), (7, 0.07), (10, 0.15)],
}
