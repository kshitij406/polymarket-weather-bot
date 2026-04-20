import logging
import math
from datetime import datetime, timezone

import numpy as np

from ..config import BIAS_CORRECTION_MAX_OFFSET_C, BIAS_CORRECTION_MIN_SAMPLES

logger = logging.getLogger(__name__)


def compute(pairs: list[tuple[float, float]]) -> tuple[float, int]:
    """
    pairs: list of (predicted_mu, actual_temp)
    Returns (bias_offset, n_samples).
    bias_offset should be added to predicted mu.
    """
    n = len(pairs)
    if n < BIAS_CORRECTION_MIN_SAMPLES:
        return 0.0, n

    errors = np.array([actual - predicted for predicted, actual in pairs])
    offset = float(np.mean(errors))

    if abs(offset) > BIAS_CORRECTION_MAX_OFFSET_C:
        logger.warning(
            "Bias offset %.2f°C exceeds cap of ±%.1f°C, capping",
            offset, BIAS_CORRECTION_MAX_OFFSET_C,
        )
        offset = math.copysign(BIAS_CORRECTION_MAX_OFFSET_C, offset)

    return offset, n
