"""
Market check job — runs every 30 minutes.
Fetches active Polymarket weather markets, computes edge against our probability
snapshots, and writes predictions for markets with sufficient edge.
"""
import logging
from datetime import datetime, timezone

from ..alerts import send_alert
from ..config import EDGE_THRESHOLD_PP, HYPOTHETICAL_STAKE_USD, STATIONS
from ..database import (
    get_latest_probability_snapshot,
    get_recent_prediction_for_market,
    init_db,
    insert_market_snapshot,
    insert_prediction,
)
from ..polymarket import client as pm_client
from ..polymarket.parser import parse_temperature_markets

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

EDGE_THRESHOLD = EDGE_THRESHOLD_PP / 100.0


def main():
    init_db()
    now = datetime.now(timezone.utc)

    try:
        raw_markets = pm_client.get_markets({"category": "weather", "active": "true"})
        logger.info("Fetched %d raw weather markets from Polymarket", len(raw_markets))
    except Exception as exc:
        logger.error("Failed to fetch Polymarket markets: %s", exc)
        send_alert(job="market_job", error=exc)
        return

    for station in STATIONS:
        markets = parse_temperature_markets(raw_markets, station.polymarket_regex)
        logger.info("Station %s: %d matching temperature markets", station.icao, len(markets))

        if not markets:
            continue

        for market in markets:
            try:
                _process_market(market, station, now)
            except Exception as exc:
                logger.error("market_job error for market %s: %s", market.market_id, exc)
                send_alert(job="market_job", error=exc)


def _process_market(market, station, now):
    insert_market_snapshot(
        fetched_at=now.isoformat(),
        market_id=market.market_id,
        condition_id=market.condition_id,
        question=market.question,
        target_date=str(market.target_date) if market.target_date else "",
        station_id=station.icao,
        bucket_label=market.bucket_label,
        yes_price=market.yes_price,
        volume_usd=market.volume_usd,
        liquidity_usd=market.liquidity_usd,
        is_active=int(market.is_active),
        is_voided=int(market.is_voided),
    )

    if not market.target_date or not market.is_active or market.is_voided:
        return
    if market.temp_metric != "max":
        return

    snap = get_latest_probability_snapshot(str(market.target_date), station.icao)
    if snap is None:
        logger.debug("No probability snapshot for %s %s, skipping", station.icao, market.target_date)
        return

    bucket_probs = snap["bucket_probs"]
    market_prices = {market.bucket_label: market.yes_price}
    edges = {market.bucket_label: bucket_probs.get(market.bucket_label, 0.0) - market.yes_price}

    best_bucket = max(edges, key=lambda k: edges[k])
    best_edge = edges[best_bucket]

    if best_edge < EDGE_THRESHOLD:
        return

    recent = get_recent_prediction_for_market(market.market_id, str(market.target_date))
    if recent and recent["recommended_bucket"] == best_bucket:
        logger.debug("Dedup skip: already predicted market %s bucket %s", market.market_id, best_bucket)
        return

    insert_prediction(
        predicted_at=now.isoformat(),
        market_id=market.market_id,
        condition_id=market.condition_id,
        market_description=market.question,
        target_date=str(market.target_date),
        station_id=station.icao,
        probability_snapshot_id=snap["id"],
        bucket_probs=bucket_probs,
        market_prices=market_prices,
        edges=edges,
        recommended_bucket=best_bucket,
        recommended_edge=best_edge,
        market_yes_price_at_pred=market.yes_price,
        hypothetical_stake_usd=HYPOTHETICAL_STAKE_USD,
    )
    logger.info(
        "Prediction written: %s %s bucket=%s edge=%.3f mktP=%.3f ourP=%.3f",
        station.icao, market.target_date, best_bucket, best_edge,
        market.yes_price, bucket_probs.get(best_bucket, 0.0),
    )


if __name__ == "__main__":
    main()
