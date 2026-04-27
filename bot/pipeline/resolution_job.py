"""
Resolution job — runs daily at 08:00 and 14:00 UTC.
Fetches Wunderground historical data and scores all pending predictions.
"""
import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from ..alerts import send_alert
from ..config import HYPOTHETICAL_STAKE_USD, STATIONS
from ..database import (
    get_unresolved_predictions,
    init_db,
    insert_resolution_observation,
    update_prediction_resolution,
)
from ..fetchers.visual_crossing import fetch as vc_fetch
from ..fetchers.wunderground import WundergroundObservation, fetch as wu_fetch

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

MAX_PENDING_DAYS = 7


def _find_actual_bucket(actual_temp_c: float, bucket_probs: dict) -> Optional[str]:
    """Find which bucket the actual temperature falls into."""
    import re
    for label in bucket_probs:
        if label == "unknown":
            continue
        range_m = re.match(r"^([-\d.]+)-([-\d.]+)$", label)
        if range_m:
            lo, hi = float(range_m.group(1)), float(range_m.group(2))
            if lo <= actual_temp_c < hi:
                return label
            continue
        atleast_m = re.match(r"^>=([-\d.]+)$", label)
        if atleast_m:
            lo = float(atleast_m.group(1)) - 0.5
            if actual_temp_c >= lo:
                return label
            continue
        try:
            center = float(label)
            if center - 0.5 <= actual_temp_c < center + 0.5:
                return label
        except ValueError:
            continue
    return None


def _score_prediction(pred: dict, obs: WundergroundObservation, resolved_at: str, obs_id: int) -> None:
    temp_metric = "max"
    if "min" in pred["market_description"].lower():
        temp_metric = "min"

    actual_temp = obs.temp_high_c if temp_metric == "max" else obs.temp_low_c
    if actual_temp is None:
        update_prediction_resolution(
            prediction_id=pred["id"],
            actual_temp_c=None,
            actual_bucket=None,
            win=0,
            hypothetical_payout_usd=0.0,
            net_pl_usd=-pred["hypothetical_stake_usd"],
            resolution_status="no_data",
            resolved_at=resolved_at,
            wunderground_obs_id=obs_id,
        )
        return

    bucket_probs = pred["bucket_probs"]
    actual_bucket = _find_actual_bucket(actual_temp, bucket_probs)
    if actual_bucket is None:
        logger.error("Could not match actual temp %.1f to any bucket for prediction %d", actual_temp, pred["id"])
        update_prediction_resolution(
            prediction_id=pred["id"],
            actual_temp_c=actual_temp,
            actual_bucket=None,
            win=0,
            hypothetical_payout_usd=0.0,
            net_pl_usd=-pred["hypothetical_stake_usd"],
            resolution_status="error",
            resolved_at=resolved_at,
            wunderground_obs_id=obs_id,
        )
        return

    stake = pred["hypothetical_stake_usd"]
    win = int(actual_bucket == pred["recommended_bucket"])
    yes_price = pred["market_yes_price_at_pred"]
    payout = (stake / yes_price) if (win and yes_price > 0) else 0.0
    net_pl = payout - stake

    update_prediction_resolution(
        prediction_id=pred["id"],
        actual_temp_c=actual_temp,
        actual_bucket=actual_bucket,
        win=win,
        hypothetical_payout_usd=payout,
        net_pl_usd=net_pl,
        resolution_status="resolved",
        resolved_at=resolved_at,
        wunderground_obs_id=obs_id,
    )
    logger.info(
        "Scored prediction %d: actual=%.1f°C bucket=%s win=%d net_pl=%.2f",
        pred["id"], actual_temp, actual_bucket, win, net_pl,
    )


def main():
    init_db()
    now = datetime.now(timezone.utc)
    resolved_at = now.isoformat()

    for station in STATIONS:
        for days_back in range(1, MAX_PENDING_DAYS + 1):
            target_date = (now - timedelta(days=days_back)).date()
            pending = get_unresolved_predictions(str(target_date))
            if not pending:
                continue

            logger.info("Resolving %d predictions for %s %s", len(pending), station.icao, target_date)

            try:
                obs = wu_fetch(station.wunderground_id, target_date)
            except Exception as exc:
                logger.error("Wunderground fetch error for %s %s: %s", station.icao, target_date, exc)
                send_alert(job="resolution_job", error=exc)
                continue

            if obs is None or obs.temp_high_c is None:
                logger.info("Wunderground has no data for %s %s, trying Visual Crossing", station.icao, target_date)
                try:
                    vc = vc_fetch(station, target_date)
                    if vc is not None and vc.det_temp_max_c is not None:
                        obs = WundergroundObservation(
                            station_id=station.icao,
                            date=target_date,
                            temp_high_c=vc.det_temp_max_c,
                            temp_low_c=vc.det_temp_min_c,
                            temp_avg_c=vc.det_temp_avg_c,
                            fetched_at=resolved_at,
                            raw_json=vc.raw_json,
                        )
                        logger.info("Visual Crossing fallback succeeded for %s %s: high=%.1f low=%.1f",
                                    station.icao, target_date, vc.det_temp_max_c, vc.det_temp_min_c or 0)
                except Exception as exc:
                    logger.warning("Visual Crossing fallback failed for %s %s: %s", station.icao, target_date, exc)

            data_available = int(obs is not None and obs.temp_high_c is not None)

            obs_id = insert_resolution_observation(
                fetched_at=resolved_at,
                target_date=str(target_date),
                station_id=station.icao,
                temp_high_c=obs.temp_high_c if obs else None,
                temp_low_c=obs.temp_low_c if obs else None,
                temp_avg_c=obs.temp_avg_c if obs else None,
                raw_json=obs.raw_json if obs else "{}",
                data_available=data_available,
            )

            if not data_available:
                logger.info("No observation data for %s %s from any source", station.icao, target_date)
                continue

            for pred in pending:
                if pred["station_id"] != station.icao:
                    continue
                try:
                    _score_prediction(pred, obs, resolved_at, obs_id)
                except Exception as exc:
                    logger.error("Score error prediction %d: %s", pred["id"], exc)
                    send_alert(job="resolution_job", error=exc)


if __name__ == "__main__":
    main()
