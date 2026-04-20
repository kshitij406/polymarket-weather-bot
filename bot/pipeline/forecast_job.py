"""
Forecast job — runs every 6 hours.
Fetches all model data for each configured station, blends into a probability
distribution, and writes results to the DB.
"""
import logging
import math
from datetime import datetime, timedelta, timezone

from ..alerts import send_alert
from ..config import DB_PATH, STATIONS
from ..database import (
    ForecastFetchRecord,
    ProbabilitySnapshotRecord,
    get_resolved_predictions_for_bias,
    init_db,
    insert_forecast_fetch,
    insert_probability_snapshot,
)
from ..fetchers import metar, noaa, open_meteo, tomorrow_io, visual_crossing
from ..models import bias_correction, climatology, ensemble_blend

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


def _lead_hours(fetch_time: datetime, target_date) -> float:
    import datetime as dt
    target_noon = dt.datetime(target_date.year, target_date.month, target_date.day, 12, 0, 0, tzinfo=timezone.utc)
    return max(0.0, (target_noon - fetch_time).total_seconds() / 3600)


def run_for_station(station, target_date, now):
    lead_h = _lead_hours(now, target_date)
    lead_d = lead_h / 24.0

    fetch_ids = []

    def _store_fetch(source, raw_json, member_temps=None, det_max=None, det_min=None,
                     ens_mean=None, ens_std=None, n_members=None, success=1, err=None):
        import numpy as np
        rec = ForecastFetchRecord(
            fetched_at=now.isoformat(),
            source=source,
            target_date=str(target_date),
            lead_hours=lead_h,
            station_id=station.icao,
            raw_json=raw_json,
            member_max_temps=member_temps,
            det_temp_max_c=det_max,
            det_temp_min_c=det_min,
            ensemble_mean_c=ens_mean,
            ensemble_std_c=ens_std,
            n_members=n_members,
            success=success,
            error_message=err,
        )
        fid = insert_forecast_fetch(rec)
        fetch_ids.append(fid)
        return fid

    import numpy as np
    import json

    ensemble_forecasts = []
    deterministic_forecasts = []

    ecmwf = open_meteo.fetch_ecmwf_ensemble(station, target_date)
    if ecmwf:
        arr = np.array(ecmwf.member_daily_max_temps)
        _store_fetch("ecmwf_ensemble", ecmwf.raw_json, member_temps=ecmwf.member_daily_max_temps,
                     ens_mean=float(np.mean(arr)), ens_std=float(np.std(arr, ddof=1)), n_members=len(arr))
        ensemble_forecasts.append(ecmwf)
    else:
        _store_fetch("ecmwf_ensemble", "{}", success=0, err="fetch returned None")

    gfs = open_meteo.fetch_gfs_ensemble(station, target_date)
    if gfs:
        arr = np.array(gfs.member_daily_max_temps)
        _store_fetch("gfs_ensemble", gfs.raw_json, member_temps=gfs.member_daily_max_temps,
                     ens_mean=float(np.mean(arr)), ens_std=float(np.std(arr, ddof=1)), n_members=len(arr))
        ensemble_forecasts.append(gfs)
    else:
        _store_fetch("gfs_ensemble", "{}", success=0, err="fetch returned None")

    ukmo = open_meteo.fetch_ukmo(station, target_date)
    if ukmo:
        arr = np.array(ukmo.member_daily_max_temps)
        _store_fetch("ukmo", ukmo.raw_json, member_temps=ukmo.member_daily_max_temps,
                     ens_mean=float(np.mean(arr)), ens_std=float(np.std(arr, ddof=1)), n_members=len(arr))
        ensemble_forecasts.append(ukmo)
    else:
        _store_fetch("ukmo", "{}", success=0, err="fetch returned None")

    vc = visual_crossing.fetch(station, target_date)
    if vc:
        _store_fetch("visual_crossing", vc.raw_json, det_max=vc.det_temp_max_c, det_min=vc.det_temp_min_c)
        deterministic_forecasts.append(vc)
    else:
        _store_fetch("visual_crossing", "{}", success=0, err="fetch returned None")

    tio = tomorrow_io.fetch(station, target_date)
    if tio:
        _store_fetch("tomorrow_io", tio.raw_json, det_max=tio.det_temp_max_c, det_min=tio.det_temp_min_c)
        deterministic_forecasts.append(tio)
    else:
        _store_fetch("tomorrow_io", "{}", success=0, err="fetch returned None")

    noaa_fc = noaa.fetch(station, target_date)
    if noaa_fc:
        _store_fetch("noaa", noaa_fc.raw_json, det_max=noaa_fc.det_temp_max_c, det_min=noaa_fc.det_temp_min_c)
        deterministic_forecasts.append(noaa_fc)

    era5 = open_meteo.fetch_era5_climatology(station, target_date)
    clim_prior = None
    if era5:
        _store_fetch("era5_climatology", era5.raw_json, det_max=era5.det_temp_max_c)
        clim_prior = climatology.compute(era5.raw_json)

    bias_pairs = get_resolved_predictions_for_bias(station.icao)
    bias_off, _ = bias_correction.compute(bias_pairs)

    bucket_boundaries = _default_bucket_boundaries(target_date)

    dist = ensemble_blend.compute_distribution(
        ensemble_forecasts=ensemble_forecasts,
        deterministic_forecasts=deterministic_forecasts,
        lead_days=lead_d,
        clim_prior=clim_prior,
        bias_offset=bias_off,
        bucket_boundaries=bucket_boundaries,
    )

    if dist is None:
        logger.warning("No distribution computed for %s %s", station.icao, target_date)
        return

    snap = ProbabilitySnapshotRecord(
        computed_at=now.isoformat(),
        target_date=str(target_date),
        station_id=station.icao,
        lead_hours=lead_h,
        mu_blended_c=dist.mu_blended,
        sigma_blended_c=dist.sigma_blended,
        mu_clim_c=dist.mu_clim,
        sigma_clim_c=dist.sigma_clim,
        clim_weight=dist.clim_weight,
        bias_offset_c=dist.bias_offset,
        mu_final_c=dist.mu_final,
        sigma_final_c=dist.sigma_final,
        bucket_probs=dist.bucket_probs,
        sources_used=dist.sources_used,
        forecast_fetch_ids=fetch_ids,
    )
    insert_probability_snapshot(snap)
    logger.info(
        "Forecast done: %s %s lead=%.1fd mu=%.1f±%.1f buckets=%d",
        station.icao, target_date, lead_d, dist.mu_final, dist.sigma_final, len(dist.bucket_probs),
    )


def _default_bucket_boundaries(target_date) -> dict:
    """Generate integer °C buckets from -10 to 40."""
    buckets = {}
    for k in range(-10, 41):
        buckets[str(k)] = (k - 0.5, k + 0.5)
    return buckets


def main():
    init_db()
    now = datetime.now(timezone.utc)
    target_dates = [
        (now + timedelta(days=d)).date()
        for d in range(1, 11)
    ]
    for station in STATIONS:
        for target_date in target_dates:
            try:
                run_for_station(station, target_date, now)
            except Exception as exc:
                logger.error("Forecast failed for %s %s: %s", station.icao, target_date, exc)
                send_alert(job="forecast_job", error=exc)


if __name__ == "__main__":
    main()
