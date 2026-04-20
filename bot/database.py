import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from .config import DB_PATH


@contextmanager
def get_conn(db_path: Path = DB_PATH):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Path = DB_PATH) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with get_conn(db_path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS forecast_fetches (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                fetched_at       TEXT NOT NULL,
                source           TEXT NOT NULL,
                target_date      TEXT NOT NULL,
                lead_hours       REAL NOT NULL,
                station_id       TEXT NOT NULL,
                raw_json         TEXT NOT NULL,
                member_max_temps TEXT,
                det_temp_max_c   REAL,
                det_temp_min_c   REAL,
                ensemble_mean_c  REAL,
                ensemble_std_c   REAL,
                n_members        INTEGER,
                success          INTEGER NOT NULL DEFAULT 1,
                error_message    TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_ff_target_date
                ON forecast_fetches(target_date, source);

            CREATE TABLE IF NOT EXISTS probability_snapshots (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                computed_at        TEXT NOT NULL,
                target_date        TEXT NOT NULL,
                station_id         TEXT NOT NULL,
                lead_hours         REAL NOT NULL,
                mu_blended_c       REAL NOT NULL,
                sigma_blended_c    REAL NOT NULL,
                mu_clim_c          REAL NOT NULL,
                sigma_clim_c       REAL NOT NULL,
                clim_weight        REAL NOT NULL,
                bias_offset_c      REAL NOT NULL DEFAULT 0.0,
                mu_final_c         REAL NOT NULL,
                sigma_final_c      REAL NOT NULL,
                bucket_probs_json  TEXT NOT NULL,
                sources_used       TEXT NOT NULL,
                forecast_fetch_ids TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ps_target_date
                ON probability_snapshots(target_date);

            CREATE TABLE IF NOT EXISTS market_snapshots (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                fetched_at    TEXT NOT NULL,
                market_id     TEXT NOT NULL,
                condition_id  TEXT NOT NULL,
                question      TEXT NOT NULL,
                target_date   TEXT NOT NULL,
                station_id    TEXT NOT NULL,
                bucket_label  TEXT NOT NULL,
                yes_price     REAL NOT NULL,
                volume_usd    REAL,
                liquidity_usd REAL,
                is_active     INTEGER NOT NULL,
                is_voided     INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_ms_market_id
                ON market_snapshots(market_id, fetched_at);

            CREATE TABLE IF NOT EXISTS resolution_observations (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                fetched_at     TEXT NOT NULL,
                target_date    TEXT NOT NULL,
                station_id     TEXT NOT NULL,
                temp_high_c    REAL,
                temp_low_c     REAL,
                temp_avg_c     REAL,
                raw_json       TEXT NOT NULL,
                data_available INTEGER NOT NULL,
                data_revised   INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_ro_target_date
                ON resolution_observations(target_date);

            CREATE TABLE IF NOT EXISTS predictions (
                id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                predicted_at             TEXT NOT NULL,
                market_id                TEXT NOT NULL,
                condition_id             TEXT NOT NULL,
                market_description       TEXT NOT NULL,
                target_date              TEXT NOT NULL,
                station_id               TEXT NOT NULL,
                probability_snapshot_id  INTEGER REFERENCES probability_snapshots(id),
                bucket_probs_json        TEXT NOT NULL,
                market_prices_json       TEXT NOT NULL,
                edges_json               TEXT NOT NULL,
                recommended_bucket       TEXT NOT NULL,
                recommended_edge         REAL NOT NULL,
                market_yes_price_at_pred REAL NOT NULL,
                hypothetical_stake_usd   REAL NOT NULL DEFAULT 10.0,
                actual_temp_c            REAL,
                actual_bucket            TEXT,
                win                      INTEGER,
                hypothetical_payout_usd  REAL,
                net_pl_usd               REAL,
                resolution_status        TEXT DEFAULT 'pending',
                resolved_at              TEXT,
                wunderground_obs_id      INTEGER REFERENCES resolution_observations(id)
            );
            CREATE INDEX IF NOT EXISTS idx_pred_target_date
                ON predictions(target_date, resolution_status);
            CREATE INDEX IF NOT EXISTS idx_pred_market_id
                ON predictions(market_id);

            CREATE TABLE IF NOT EXISTS alerts_log (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                fired_at  TEXT NOT NULL,
                job       TEXT NOT NULL,
                severity  TEXT NOT NULL,
                message   TEXT NOT NULL,
                delivered INTEGER DEFAULT 0
            );
        """)


@dataclass
class ForecastFetchRecord:
    fetched_at: str
    source: str
    target_date: str
    lead_hours: float
    station_id: str
    raw_json: str
    member_max_temps: Optional[list[float]] = None
    det_temp_max_c: Optional[float] = None
    det_temp_min_c: Optional[float] = None
    ensemble_mean_c: Optional[float] = None
    ensemble_std_c: Optional[float] = None
    n_members: Optional[int] = None
    success: int = 1
    error_message: Optional[str] = None


def insert_forecast_fetch(record: ForecastFetchRecord, db_path: Path = DB_PATH) -> int:
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """INSERT INTO forecast_fetches
               (fetched_at, source, target_date, lead_hours, station_id, raw_json,
                member_max_temps, det_temp_max_c, det_temp_min_c,
                ensemble_mean_c, ensemble_std_c, n_members, success, error_message)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                record.fetched_at, record.source, record.target_date, record.lead_hours,
                record.station_id, record.raw_json,
                json.dumps(record.member_max_temps) if record.member_max_temps is not None else None,
                record.det_temp_max_c, record.det_temp_min_c,
                record.ensemble_mean_c, record.ensemble_std_c,
                record.n_members, record.success, record.error_message,
            ),
        )
        return cur.lastrowid


@dataclass
class ProbabilitySnapshotRecord:
    computed_at: str
    target_date: str
    station_id: str
    lead_hours: float
    mu_blended_c: float
    sigma_blended_c: float
    mu_clim_c: float
    sigma_clim_c: float
    clim_weight: float
    bias_offset_c: float
    mu_final_c: float
    sigma_final_c: float
    bucket_probs: dict
    sources_used: list[str]
    forecast_fetch_ids: list[int]


def insert_probability_snapshot(snap: ProbabilitySnapshotRecord, db_path: Path = DB_PATH) -> int:
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """INSERT INTO probability_snapshots
               (computed_at, target_date, station_id, lead_hours,
                mu_blended_c, sigma_blended_c, mu_clim_c, sigma_clim_c,
                clim_weight, bias_offset_c, mu_final_c, sigma_final_c,
                bucket_probs_json, sources_used, forecast_fetch_ids)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                snap.computed_at, snap.target_date, snap.station_id, snap.lead_hours,
                snap.mu_blended_c, snap.sigma_blended_c, snap.mu_clim_c, snap.sigma_clim_c,
                snap.clim_weight, snap.bias_offset_c, snap.mu_final_c, snap.sigma_final_c,
                json.dumps(snap.bucket_probs),
                json.dumps(snap.sources_used),
                json.dumps(snap.forecast_fetch_ids),
            ),
        )
        return cur.lastrowid


def get_latest_probability_snapshot(
    target_date: str, station_id: str, db_path: Path = DB_PATH
) -> Optional[dict]:
    with get_conn(db_path) as conn:
        row = conn.execute(
            """SELECT * FROM probability_snapshots
               WHERE target_date=? AND station_id=?
               ORDER BY computed_at DESC LIMIT 1""",
            (target_date, station_id),
        ).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["bucket_probs"] = json.loads(d["bucket_probs_json"])
        d["sources_used"] = json.loads(d["sources_used"])
        d["forecast_fetch_ids"] = json.loads(d["forecast_fetch_ids"])
        return d


def insert_market_snapshot(
    fetched_at: str, market_id: str, condition_id: str, question: str,
    target_date: str, station_id: str, bucket_label: str, yes_price: float,
    volume_usd: Optional[float], liquidity_usd: Optional[float],
    is_active: int, is_voided: int, db_path: Path = DB_PATH,
) -> int:
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """INSERT INTO market_snapshots
               (fetched_at, market_id, condition_id, question, target_date, station_id,
                bucket_label, yes_price, volume_usd, liquidity_usd, is_active, is_voided)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (fetched_at, market_id, condition_id, question, target_date, station_id,
             bucket_label, yes_price, volume_usd, liquidity_usd, is_active, is_voided),
        )
        return cur.lastrowid


def get_recent_prediction_for_market(
    market_id: str, target_date: str, db_path: Path = DB_PATH
) -> Optional[dict]:
    with get_conn(db_path) as conn:
        row = conn.execute(
            """SELECT * FROM predictions
               WHERE market_id=? AND target_date=?
               ORDER BY predicted_at DESC LIMIT 1""",
            (market_id, target_date),
        ).fetchone()
        return dict(row) if row else None


def insert_prediction(
    predicted_at: str, market_id: str, condition_id: str, market_description: str,
    target_date: str, station_id: str, probability_snapshot_id: int,
    bucket_probs: dict, market_prices: dict, edges: dict,
    recommended_bucket: str, recommended_edge: float, market_yes_price_at_pred: float,
    hypothetical_stake_usd: float = 10.0, db_path: Path = DB_PATH,
) -> int:
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """INSERT INTO predictions
               (predicted_at, market_id, condition_id, market_description,
                target_date, station_id, probability_snapshot_id,
                bucket_probs_json, market_prices_json, edges_json,
                recommended_bucket, recommended_edge, market_yes_price_at_pred,
                hypothetical_stake_usd)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                predicted_at, market_id, condition_id, market_description,
                target_date, station_id, probability_snapshot_id,
                json.dumps(bucket_probs), json.dumps(market_prices), json.dumps(edges),
                recommended_bucket, recommended_edge, market_yes_price_at_pred,
                hypothetical_stake_usd,
            ),
        )
        return cur.lastrowid


def get_unresolved_predictions(target_date: str, db_path: Path = DB_PATH) -> list[dict]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT * FROM predictions
               WHERE target_date=? AND resolution_status='pending'""",
            (target_date,),
        ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d["bucket_probs"] = json.loads(d["bucket_probs_json"])
            d["market_prices"] = json.loads(d["market_prices_json"])
            d["edges"] = json.loads(d["edges_json"])
            result.append(d)
        return result


def update_prediction_resolution(
    prediction_id: int, actual_temp_c: float, actual_bucket: str,
    win: int, hypothetical_payout_usd: float, net_pl_usd: float,
    resolution_status: str, resolved_at: str, wunderground_obs_id: int,
    db_path: Path = DB_PATH,
) -> None:
    with get_conn(db_path) as conn:
        conn.execute(
            """UPDATE predictions SET
               actual_temp_c=?, actual_bucket=?, win=?,
               hypothetical_payout_usd=?, net_pl_usd=?,
               resolution_status=?, resolved_at=?, wunderground_obs_id=?
               WHERE id=?""",
            (actual_temp_c, actual_bucket, win, hypothetical_payout_usd, net_pl_usd,
             resolution_status, resolved_at, wunderground_obs_id, prediction_id),
        )


def insert_resolution_observation(
    fetched_at: str, target_date: str, station_id: str,
    temp_high_c: Optional[float], temp_low_c: Optional[float], temp_avg_c: Optional[float],
    raw_json: str, data_available: int, data_revised: int = 0,
    db_path: Path = DB_PATH,
) -> int:
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """INSERT INTO resolution_observations
               (fetched_at, target_date, station_id,
                temp_high_c, temp_low_c, temp_avg_c,
                raw_json, data_available, data_revised)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (fetched_at, target_date, station_id,
             temp_high_c, temp_low_c, temp_avg_c,
             raw_json, data_available, data_revised),
        )
        return cur.lastrowid


def get_resolved_predictions_for_bias(
    station_id: str, db_path: Path = DB_PATH
) -> list[tuple[float, float]]:
    """Return (predicted_mu, actual_temp) pairs for resolved predictions."""
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT p.market_yes_price_at_pred, p.actual_temp_c,
                      ps.mu_final_c
               FROM predictions p
               JOIN probability_snapshots ps ON p.probability_snapshot_id = ps.id
               WHERE p.station_id=? AND p.resolution_status='resolved'
                 AND p.actual_temp_c IS NOT NULL""",
            (station_id,),
        ).fetchall()
        return [(row["mu_final_c"], row["actual_temp_c"]) for row in rows]


def get_all_predictions_for_report(db_path: Path = DB_PATH) -> list[dict]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT p.*, ps.mu_final_c, ps.sigma_final_c
               FROM predictions p
               LEFT JOIN probability_snapshots ps ON p.probability_snapshot_id = ps.id
               ORDER BY p.predicted_at DESC"""
        ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d["bucket_probs"] = json.loads(d["bucket_probs_json"])
            d["market_prices"] = json.loads(d["market_prices_json"])
            d["edges"] = json.loads(d["edges_json"])
            result.append(d)
        return result


def log_alert(job: str, severity: str, message: str, delivered: int = 0, db_path: Path = DB_PATH) -> None:
    from datetime import timezone
    fired_at = datetime.now(timezone.utc).isoformat()
    with get_conn(db_path) as conn:
        conn.execute(
            "INSERT INTO alerts_log (fired_at, job, severity, message, delivered) VALUES (?,?,?,?,?)",
            (fired_at, job, severity, message, delivered),
        )
