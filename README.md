# polymarket-weather-bot

Automated tracker for Polymarket temperature range markets. Fetches multi-model weather forecasts, computes a calibrated probability distribution, and logs predictions with hypothetical edge vs. market prices. Resolves each prediction against Weather Underground historical data and tracks long-run performance.

No manual intervention required after setup. All state lives in `data/weather_bot.db` committed to this repo. A human-readable snapshot is written to `SUMMARY.md` on every run.

## How it works

1. **Forecast job** (every 6 hours) — fetches ECMWF, GFS, and UKMO ensembles from Open-Meteo plus Visual Crossing and Tomorrow.io deterministic forecasts. Blends them into a single Gaussian distribution using the law of total variance, adds an ERA5 climatological prior weighted by lead time, and applies a bias correction from historical verification pairs. Writes a `probability_snapshots` row per station per target date.

2. **Market check job** (every 30 minutes) — fetches active Polymarket weather markets via the Gamma API, matches them to our probability snapshots, and writes a `predictions` row whenever our probability exceeds the market price by ≥ 10 percentage points.

3. **Resolution job** (daily 08:00 + 14:00 UTC) — fetches the previous day's historical temperature from Weather Underground and scores all pending predictions as win/loss with hypothetical $10 stake payouts.

4. **Report job** (weekly, Sunday 06:00 UTC) — generates `REPORT.md` with win rate, ROI, edge-tier breakdown, and a calibration table.

After every job, `scripts/summarize_db.py` writes a current snapshot of all DB tables to `SUMMARY.md`.

## Repository files

| File | Description |
|------|-------------|
| `data/weather_bot.db` | SQLite database — all predictions, forecasts, market snapshots, resolutions |
| `SUMMARY.md` | Human-readable DB snapshot, updated on every workflow run |
| `REPORT.md` | Weekly performance report with win rate, ROI, calibration |

## Setup

### 1. Fork or clone this repo (make it public for unlimited Actions minutes)

### 2. Add GitHub Actions secrets

| Secret | Description |
|--------|-------------|
| `VISUAL_CROSSING_API_KEY` | [Visual Crossing](https://www.visualcrossing.com/) free tier (1,000 records/day) |
| `TOMORROW_IO_API_KEY` | [Tomorrow.io](https://www.tomorrow.io/) free tier (500 calls/day) |
| `WUNDERGROUND_API_KEY` | Optional — if omitted, the key is scraped dynamically from Wunderground |
| `DISCORD_WEBHOOK_URL` | Optional — Discord webhook URL for failure alerts |

### 3. Enable GitHub Actions

Workflows run automatically on their schedules. Trigger any workflow manually via **Actions → workflow → Run workflow** to test.

## Probability model

- **Sources:** ECMWF ensemble (51 members), GFS ensemble (31 members), UKMO (18 members), Visual Crossing, Tomorrow.io, NOAA (US stations only)
- **Blending:** Weighted Gaussian mixture via law of total variance. ECMWF-dominant at short lead times; weights shift toward climatology beyond ~9 days.
- **Climatological prior:** ERA5 daily max temps over ±7 calendar days × 30 years (~450 samples), blended in with weight `min(0.70, max(0, (lead_days − 2) / 12))`.
- **Bias correction:** Additive MOS-style offset = mean(actual − predicted) over last ≥ 20 resolved predictions, capped at ±3 °C.
- **Edge threshold:** Predictions flagged when our probability exceeds market YES price by ≥ 10 percentage points.

## Station configuration

Stations are defined in `bot/config.py`. Adding a new station requires one entry in the `STATIONS` list — no other code changes needed.

```python
Station(
    icao="EGLC",
    wunderground_id="EGLC:9:GB",
    lat=51.5048, lon=-0.0495, elevation_m=4,
    timezone="Europe/London",
    display_name="London City Airport",
    polymarket_regex=r"(?i)(London|EGLC|City Airport)",
)
```
