import logging
import math
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
from scipy.stats import norm

from ..config import SOURCE_WEIGHTS
from .climatology import ClimatologicalPrior

logger = logging.getLogger(__name__)

_WEIGHT_ANCHORS = [1, 3, 5, 7, 10]


@dataclass
class SourceStats:
    source_name: str
    mu: float
    sigma: float


@dataclass
class ProbabilityDistribution:
    mu_blended: float
    sigma_blended: float
    mu_final: float
    sigma_final: float
    mu_clim: float
    sigma_clim: float
    clim_weight: float
    bias_offset: float
    bucket_probs: dict[str, float]
    sources_used: list[str]


def _interpolate_weight(source_name: str, lead_days: float) -> float:
    anchors = SOURCE_WEIGHTS.get(source_name)
    if anchors is None:
        return 0.0
    anchors_days = [a[0] for a in anchors]
    anchors_vals = [a[1] for a in anchors]
    d = max(anchors_days[0], min(anchors_days[-1], lead_days))
    for i in range(len(anchors_days) - 1):
        if anchors_days[i] <= d <= anchors_days[i + 1]:
            t = (d - anchors_days[i]) / (anchors_days[i + 1] - anchors_days[i])
            return anchors_vals[i] + t * (anchors_vals[i + 1] - anchors_vals[i])
    return anchors_vals[-1]


def _source_stats_from_ensemble(source_name: str, member_temps: list[float]) -> Optional[SourceStats]:
    if len(member_temps) < 2:
        return None
    arr = np.array(member_temps)
    return SourceStats(source_name=source_name, mu=float(np.mean(arr)), sigma=float(np.std(arr, ddof=1)))


def _source_stats_from_deterministic(source_name: str, temp_max: float, sigma_ref: Optional[float]) -> SourceStats:
    sigma = (sigma_ref * 1.2) if sigma_ref is not None else 2.0
    return SourceStats(source_name=source_name, mu=temp_max, sigma=sigma)


def _bucket_prob(lo: Optional[float], hi: Optional[float], mu: float, sigma: float) -> float:
    if lo is None and hi is None:
        return 1.0
    if lo is None:
        return float(norm.cdf(hi, mu, sigma))
    if hi is None:
        return float(1.0 - norm.cdf(lo, mu, sigma))
    return float(norm.cdf(hi, mu, sigma) - norm.cdf(lo, mu, sigma))


def compute_bucket_probs(
    mu: float, sigma: float, bucket_boundaries: dict[str, tuple[Optional[float], Optional[float]]]
) -> dict[str, float]:
    return {
        label: _bucket_prob(lo, hi, mu, sigma)
        for label, (lo, hi) in bucket_boundaries.items()
    }


def compute_distribution(
    ensemble_forecasts: list,
    deterministic_forecasts: list,
    lead_days: float,
    clim_prior: Optional[ClimatologicalPrior],
    bias_offset: float,
    bucket_boundaries: dict[str, tuple[Optional[float], Optional[float]]],
) -> Optional[ProbabilityDistribution]:
    """
    ensemble_forecasts: list of EnsembleForecast
    deterministic_forecasts: list of DeterministicForecast / VisualCrossingForecast / etc.
    """
    source_stats: list[SourceStats] = []
    ecmwf_sigma: Optional[float] = None

    for ef in ensemble_forecasts:
        if ef is None:
            continue
        stats = _source_stats_from_ensemble(ef.source_name, ef.member_daily_max_temps)
        if stats is None:
            continue
        source_stats.append(stats)
        if ef.source_name == "ecmwf_ensemble":
            ecmwf_sigma = stats.sigma

    for df in deterministic_forecasts:
        if df is None:
            continue
        if df.det_temp_max_c is None:
            continue
        stats = _source_stats_from_deterministic(df.source_name, df.det_temp_max_c, ecmwf_sigma)
        source_stats.append(stats)

    if not source_stats:
        logger.warning("No valid source stats to blend")
        return None

    raw_weights = {s.source_name: _interpolate_weight(s.source_name, lead_days) for s in source_stats}
    total_w = sum(raw_weights.values())
    if total_w == 0:
        weights = {s.source_name: 1.0 / len(source_stats) for s in source_stats}
    else:
        weights = {k: v / total_w for k, v in raw_weights.items()}

    mu_blend = sum(weights[s.source_name] * s.mu for s in source_stats)
    variance_blend = sum(
        weights[s.source_name] * (s.sigma ** 2 + (s.mu - mu_blend) ** 2)
        for s in source_stats
    )
    sigma_blend = math.sqrt(max(variance_blend, 0.01))

    if clim_prior is not None:
        clim_weight = min(0.70, max(0.0, (lead_days - 2) / 12))
        fw = 1.0 - clim_weight
        mu_final = fw * mu_blend + clim_weight * clim_prior.mu_clim
        variance_final = (
            fw * sigma_blend ** 2
            + clim_weight * clim_prior.sigma_clim ** 2
            + fw * clim_weight * (mu_blend - clim_prior.mu_clim) ** 2
        )
        sigma_final = math.sqrt(max(variance_final, 0.01))
        mu_clim = clim_prior.mu_clim
        sigma_clim = clim_prior.sigma_clim
    else:
        clim_weight = 0.0
        mu_final = mu_blend
        sigma_final = sigma_blend
        mu_clim = mu_blend
        sigma_clim = sigma_blend

    mu_corrected = mu_final + bias_offset

    bucket_probs = compute_bucket_probs(mu_corrected, sigma_final, bucket_boundaries)

    return ProbabilityDistribution(
        mu_blended=mu_blend,
        sigma_blended=sigma_blend,
        mu_final=mu_corrected,
        sigma_final=sigma_final,
        mu_clim=mu_clim,
        sigma_clim=sigma_clim,
        clim_weight=clim_weight,
        bias_offset=bias_offset,
        bucket_probs=bucket_probs,
        sources_used=[s.source_name for s in source_stats],
    )
