"""
Weekly report job — runs Sunday 06:00 UTC.
Generates REPORT.md from all DB data.
"""
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..config import DB_PATH, STATIONS
from ..database import get_all_predictions_for_report, init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

REPORT_PATH = Path(__file__).parent.parent.parent / "REPORT.md"


def _edge_tier(edge: float) -> str:
    if edge < 0.15:
        return "Low (10–15pp)"
    if edge < 0.20:
        return "Medium (15–20pp)"
    return "High (20+pp)"


def _calibration_bucket(yes_price: float) -> str:
    for lo in range(10, 80, 10):
        hi = lo + 10
        if lo / 100 <= yes_price < hi / 100:
            return f"{lo}–{hi}%"
    return "other"


def generate_report() -> str:
    now = datetime.now(timezone.utc)
    all_preds = get_all_predictions_for_report()

    resolved = [p for p in all_preds if p["resolution_status"] == "resolved"]
    wins = [p for p in resolved if p["win"] == 1]
    losses = [p for p in resolved if p["win"] == 0]
    voided = [p for p in all_preds if p["resolution_status"] in ("voided", "no_data")]
    pending = [p for p in all_preds if p["resolution_status"] == "pending"]

    total_staked = sum(p["hypothetical_stake_usd"] for p in resolved)
    total_returned = sum(p.get("hypothetical_payout_usd") or 0.0 for p in resolved)
    net_pl = total_returned - total_staked
    roi = (net_pl / total_staked * 100) if total_staked > 0 else 0.0
    win_rate = (len(wins) / len(resolved) * 100) if resolved else 0.0

    start_date = min((p["predicted_at"][:10] for p in all_preds), default="N/A")
    end_date = max((p["predicted_at"][:10] for p in all_preds), default="N/A")

    stations_str = ", ".join(f"{s.icao} ({s.display_name})" for s in STATIONS)

    lines = [
        "# Polymarket Weather Bot — Performance Report",
        f"Generated: {now.strftime('%Y-%m-%d %H:%M UTC')}",
        f"Stations: {stations_str}",
        f"Period: {start_date} to {end_date}",
        "",
        "## Summary",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Total predictions | {len(all_preds)} |",
        f"| Resolved | {len(resolved)} |",
        f"| Wins | {len(wins)} |",
        f"| Losses | {len(losses)} |",
        f"| Win rate | {win_rate:.1f}% |",
        f"| Total hypothetical staked | ${total_staked:.2f} |",
        f"| Total hypothetical returned | ${total_returned:.2f} |",
        f"| Net profit/loss | ${net_pl:+.2f} |",
        f"| ROI | {roi:+.1f}% |",
        f"| Pending | {len(pending)} |",
        f"| Voided/no-data (excluded) | {len(voided)} |",
        "",
    ]

    tiers = ["Low (10–15pp)", "Medium (15–20pp)", "High (20+pp)"]
    lines += [
        "## Performance by Edge Tier",
        "| Edge Tier | Predictions | Win Rate | Staked | Returned | Net P&L |",
        "|-----------|-------------|----------|--------|----------|---------|",
    ]
    for tier in tiers:
        tier_preds = [p for p in resolved if _edge_tier(p["recommended_edge"]) == tier]
        n = len(tier_preds)
        if n == 0:
            lines.append(f"| {tier} | 0 | — | $0.00 | $0.00 | $0.00 |")
            continue
        tw = [p for p in tier_preds if p["win"] == 1]
        wr = len(tw) / n * 100
        ts = sum(p["hypothetical_stake_usd"] for p in tier_preds)
        tr = sum(p.get("hypothetical_payout_usd") or 0.0 for p in tier_preds)
        tnp = tr - ts
        lines.append(f"| {tier} | {n} | {wr:.0f}% | ${ts:.2f} | ${tr:.2f} | ${tnp:+.2f} |")
    lines.append("")

    lines += [
        "## Calibration Check",
        "| Predicted Prob Bucket | Predictions | Actual Win Rate | Calibration Error |",
        "|----------------------|-------------|-----------------|-------------------|",
    ]
    cal_buckets = [f"{lo}–{lo+10}%" for lo in range(10, 80, 10)]
    for bucket_label in cal_buckets:
        bucket_preds = [p for p in resolved if _calibration_bucket(p["market_yes_price_at_pred"]) == bucket_label]
        n = len(bucket_preds)
        if n == 0:
            lines.append(f"| {bucket_label} | 0 | — | — |")
            continue
        actual_wr = sum(p["win"] for p in bucket_preds) / n
        lo = int(bucket_label.split("–")[0])
        midpoint = (lo + 5) / 100
        cal_err = midpoint - actual_wr
        lines.append(f"| {bucket_label} | {n} | {actual_wr*100:.1f}% | {cal_err*100:+.1f}pp |")
    lines.append("")
    lines.append("*Calibration error = predicted midpoint − actual win rate. Positive = overconfident.*")
    lines.append("")

    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()[:10]
    recent = [p for p in all_preds if p["predicted_at"][:10] >= cutoff]
    lines += [
        "## Recent Predictions (Last 30 Days)",
        "| Date | Market | Bucket | Our P% | Mkt P% | Edge | Win? | Net P&L |",
        "|------|--------|--------|--------|--------|------|------|---------|",
    ]
    for p in recent[:50]:
        our_p = p["bucket_probs"].get(p["recommended_bucket"], 0.0) * 100
        mkt_p = p["market_yes_price_at_pred"] * 100
        edge = p["recommended_edge"] * 100
        win_str = "✓" if p["win"] == 1 else ("✗" if p["win"] == 0 else "—")
        net_pl_str = f"${p['net_pl_usd']:+.2f}" if p.get("net_pl_usd") is not None else "—"
        market_short = p["market_description"][:40].replace("|", "/")
        lines.append(
            f"| {p['predicted_at'][:10]} | {market_short} | {p['recommended_bucket']} "
            f"| {our_p:.1f}% | {mkt_p:.1f}% | {edge:.1f}pp | {win_str} | {net_pl_str} |"
        )
    lines.append("")

    return "\n".join(lines) + "\n"


def main():
    init_db()
    report = generate_report()
    REPORT_PATH.write_text(report)
    logger.info("REPORT.md written (%d chars)", len(report))


if __name__ == "__main__":
    main()
