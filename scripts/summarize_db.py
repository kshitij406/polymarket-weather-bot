"""
Reads data/weather_bot.db and writes a human-readable SUMMARY.md to the repo root.
Run from any directory — paths are resolved relative to this script's location.
"""
import json
import sqlite3
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DB_PATH = REPO_ROOT / "data" / "weather_bot.db"
SUMMARY_PATH = REPO_ROOT / "SUMMARY.md"


def connect():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def section_probability_snapshots(conn) -> str:
    rows = conn.execute("""
        SELECT ps.*
        FROM probability_snapshots ps
        INNER JOIN (
            SELECT target_date, station_id, MAX(computed_at) AS latest
            FROM probability_snapshots
            GROUP BY target_date, station_id
        ) latest ON ps.target_date = latest.target_date
                   AND ps.station_id = latest.station_id
                   AND ps.computed_at = latest.latest
        ORDER BY ps.target_date, ps.station_id
    """).fetchall()

    if not rows:
        return "## Probability Snapshots\n\n_No data yet._\n"

    lines = [
        "## Probability Snapshots",
        "",
        "| Station | Target Date | Lead (h) | μ (°C) | σ (°C) | Top Buckets |",
        "|---------|-------------|----------|--------|--------|-------------|",
    ]
    for r in rows:
        probs = json.loads(r["bucket_probs_json"])
        top3 = sorted(probs.items(), key=lambda x: -x[1])[:3]
        top3_str = "  ".join(f"{k}°C {v*100:.1f}%" for k, v in top3)
        lines.append(
            f"| {r['station_id']} | {r['target_date']} | {r['lead_hours']:.1f} "
            f"| {r['mu_final_c']:.1f} | {r['sigma_final_c']:.1f} | {top3_str} |"
        )
    return "\n".join(lines) + "\n"


def section_market_snapshots(conn) -> str:
    rows = conn.execute("""
        SELECT ms.target_date, ms.station_id, ms.bucket_label, ms.yes_price
        FROM market_snapshots ms
        INNER JOIN (
            SELECT target_date, station_id, bucket_label, MAX(fetched_at) AS latest
            FROM market_snapshots
            GROUP BY target_date, station_id, bucket_label
        ) latest ON ms.target_date = latest.target_date
                   AND ms.station_id = latest.station_id
                   AND ms.bucket_label = latest.bucket_label
                   AND ms.fetched_at = latest.latest
        ORDER BY ms.target_date, ms.station_id,
                 CAST(REPLACE(ms.bucket_label, '>=', '') AS REAL)
    """).fetchall()

    if not rows:
        return "## Market Snapshots\n\n_No data yet._\n"

    lines = ["## Market Snapshots", ""]
    current_key = None
    for r in rows:
        key = (r["target_date"], r["station_id"])
        if key != current_key:
            if current_key is not None:
                lines.append("")
            lines.append(f"### {r['station_id']} — {r['target_date']}")
            lines.append("")
            lines.append("| Bucket (°C) | YES Price |")
            lines.append("|-------------|-----------|")
            current_key = key
        lines.append(f"| {r['bucket_label']} | {r['yes_price']*100:.1f}% |")

    return "\n".join(lines) + "\n"


def section_predictions(conn) -> str:
    rows = conn.execute("""
        SELECT predicted_at, target_date, station_id, recommended_bucket,
               recommended_edge, market_yes_price_at_pred,
               resolution_status, win, net_pl_usd
        FROM predictions
        ORDER BY predicted_at DESC
    """).fetchall()

    if not rows:
        return "## Predictions\n\n_No predictions yet._\n"

    lines = [
        "## Predictions",
        "",
        "| Predicted At | Date | Station | Bucket | Edge | Mkt Price | Status | Win | Net P&L |",
        "|--------------|------|---------|--------|------|-----------|--------|-----|---------|",
    ]
    for r in rows:
        win_str = "✓" if r["win"] == 1 else ("✗" if r["win"] == 0 else "—")
        pl_str = f"${r['net_pl_usd']:+.2f}" if r["net_pl_usd"] is not None else "—"
        lines.append(
            f"| {r['predicted_at'][:16]} | {r['target_date']} | {r['station_id']} "
            f"| {r['recommended_bucket']}°C | {r['recommended_edge']*100:.1f}pp "
            f"| {r['market_yes_price_at_pred']*100:.1f}% | {r['resolution_status']} "
            f"| {win_str} | {pl_str} |"
        )
    return "\n".join(lines) + "\n"


def section_performance(conn) -> str:
    rows = conn.execute("""
        SELECT resolution_status, win,
               hypothetical_stake_usd, hypothetical_payout_usd, net_pl_usd
        FROM predictions
    """).fetchall()

    if not rows:
        return "## Performance Summary\n\n_No predictions yet._\n"

    total = len(rows)
    pending = [r for r in rows if r["resolution_status"] == "pending"]
    resolved = [r for r in rows if r["resolution_status"] == "resolved"]
    wins = [r for r in resolved if r["win"] == 1]
    pending_staked = sum(r["hypothetical_stake_usd"] for r in pending)
    staked = sum(r["hypothetical_stake_usd"] for r in resolved)
    returned = sum(r["hypothetical_payout_usd"] or 0.0 for r in resolved)
    net_pl = returned - staked
    win_rate = len(wins) / len(resolved) * 100 if resolved else 0.0
    roi = net_pl / staked * 100 if staked else 0.0

    lines = [
        "## Performance Summary",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Total predictions | {total} |",
        f"| Pending | {len(pending)} (${pending_staked:.2f} at stake) |",
        f"| Resolved | {len(resolved)} |",
        f"| Wins | {len(wins)} |",
        f"| Losses | {len(resolved) - len(wins)} |",
        f"| Win rate | {win_rate:.1f}% |",
        f"| Total staked (resolved) | ${staked:.2f} |",
        f"| Total returned | ${returned:.2f} |",
        f"| Net P&L | ${net_pl:+.2f} |",
        f"| ROI | {roi:+.1f}% |",
    ]
    return "\n".join(lines) + "\n"


def main():
    if not DB_PATH.exists():
        SUMMARY_PATH.write_text("# Bot Summary\n\n_Database not found._\n")
        return

    conn = connect()
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    sections = [
        f"# Bot Summary\n\n_Generated: {now}_\n",
        section_probability_snapshots(conn),
        section_market_snapshots(conn),
        section_predictions(conn),
        section_performance(conn),
    ]
    conn.close()

    SUMMARY_PATH.write_text("\n".join(sections))
    print(f"SUMMARY.md written ({SUMMARY_PATH.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
