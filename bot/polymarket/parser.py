import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)

_TEMP_RANGE_RE = re.compile(r"\d+\s*°\s*[CcFf]|max.*temp|min.*temp|temperature", re.I)
_DEGREE_RE = re.compile(r"([-\d.]+)\s*°\s*([CcFf])")
_RANGE_RE = re.compile(r"between\s+([-\d.]+)\s*(?:°\s*[CcFf])?\s*and\s*([-\d.]+)\s*°\s*[CcFf]", re.I)
_ATLEAST_RE = re.compile(r"(?:at least|>=|≥)\s*([-\d.]+)\s*°\s*[CcFf]", re.I)
_EXACTLY_RE = re.compile(r"(?:exactly|be)\s*([-\d.]+)\s*°\s*[CcFf]", re.I)


@dataclass
class TemperatureMarket:
    market_id: str
    condition_id: str
    question: str
    description: str
    target_date: Optional[date]
    station_name: str
    outcome_labels: list[str]
    outcome_prices: list[float]
    yes_price: float
    volume_usd: float
    liquidity_usd: float
    closes_at: Optional[datetime]
    is_active: bool
    is_voided: bool
    temp_metric: str
    bucket_label: str
    bucket_boundaries: tuple[Optional[float], Optional[float]]


def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:19], fmt[:len(fmt)]).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        return None


def _parse_datetime(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _f_to_c(f: float) -> float:
    return (f - 32) * 5 / 9


def _extract_bucket(question: str) -> tuple[str, tuple[Optional[float], Optional[float]]]:
    """Return (bucket_label, (lo, hi)) boundaries in °C."""
    m = _RANGE_RE.search(question)
    if m:
        lo, hi = float(m.group(1)), float(m.group(2))
        unit_m = re.search(r"°\s*([CcFf])", question[m.start():])
        if unit_m and unit_m.group(1).upper() == "F":
            lo, hi = _f_to_c(lo), _f_to_c(hi)
        return f"{lo:.1f}-{hi:.1f}", (lo, hi)

    m = _ATLEAST_RE.search(question)
    if m:
        val = float(m.group(1))
        unit_m = re.search(r"°\s*([CcFf])", question[m.start():])
        if unit_m and unit_m.group(1).upper() == "F":
            val = _f_to_c(val)
        return f">={val:.1f}", (val - 0.5, None)

    temps = _DEGREE_RE.findall(question)
    if temps:
        val, unit = temps[-1]
        val = float(val)
        if unit.upper() == "F":
            val = _f_to_c(val)
        return f"{val:.0f}", (val - 0.5, val + 0.5)

    return "unknown", (None, None)


def parse_temperature_markets(
    raw_markets: list[dict], station_regex: str
) -> list[TemperatureMarket]:
    station_re = re.compile(station_regex)
    results = []

    for m in raw_markets:
        question = m.get("question") or m.get("title") or ""
        if not station_re.search(question):
            continue
        if not _TEMP_RANGE_RE.search(question):
            continue

        try:
            prices_raw = m.get("outcomePrices") or "[]"
            if isinstance(prices_raw, str):
                prices = [float(p) for p in json.loads(prices_raw)]
            else:
                prices = [float(p) for p in prices_raw]
        except Exception:
            prices = []

        if not prices:
            continue

        try:
            outcomes_raw = m.get("outcomes") or '["Yes","No"]'
            if isinstance(outcomes_raw, str):
                outcomes = json.loads(outcomes_raw)
            else:
                outcomes = list(outcomes_raw)
        except Exception:
            outcomes = ["Yes", "No"]

        yes_price = prices[0]

        is_voided = False
        res_source = m.get("resolutionSource") or ""
        if res_source == "N/A":
            is_voided = True
        if m.get("closed") and not m.get("volume"):
            is_voided = True
        if "void" in (m.get("resolutionNotes") or "").lower():
            is_voided = True

        temp_metric = "min" if re.search(r"\bmin\b", question, re.I) else "max"

        bucket_label, bucket_bounds = _extract_bucket(question)

        target_date = _parse_date(m.get("endDate"))
        closes_at = _parse_datetime(m.get("endDate"))

        try:
            volume = float(m.get("volume") or 0)
        except Exception:
            volume = 0.0
        try:
            liquidity = float(m.get("liquidity") or 0)
        except Exception:
            liquidity = 0.0

        results.append(TemperatureMarket(
            market_id=str(m.get("id") or ""),
            condition_id=str(m.get("conditionId") or ""),
            question=question,
            description=m.get("description") or "",
            target_date=target_date,
            station_name=station_re.search(question).group(0) if station_re.search(question) else "",
            outcome_labels=outcomes,
            outcome_prices=prices,
            yes_price=yes_price,
            volume_usd=volume,
            liquidity_usd=liquidity,
            closes_at=closes_at,
            is_active=bool(m.get("active")),
            is_voided=is_voided,
            temp_metric=temp_metric,
            bucket_label=bucket_label,
            bucket_boundaries=bucket_bounds,
        ))

    return results
