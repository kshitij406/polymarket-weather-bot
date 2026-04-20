import json
import logging
from dataclasses import dataclass
from datetime import date
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class ClimatologicalPrior:
    mu_clim: float
    sigma_clim: float
    n_samples: int


def compute(era5_raw_json: str) -> Optional[ClimatologicalPrior]:
    """Compute climatological prior from ERA5 archive raw JSON (as stored in DB)."""
    try:
        data = json.loads(era5_raw_json)
        temps = data.get("all_temps", [])
        if len(temps) < 10:
            logger.warning("Too few ERA5 climatology samples: %d", len(temps))
            return None
        arr = np.array(temps, dtype=float)
        return ClimatologicalPrior(
            mu_clim=float(np.mean(arr)),
            sigma_clim=float(np.std(arr, ddof=1)),
            n_samples=len(arr),
        )
    except Exception as exc:
        logger.warning("Climatology computation failed: %s", exc)
        return None
