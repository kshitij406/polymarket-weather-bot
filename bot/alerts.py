import logging
import traceback
from datetime import datetime, timezone

import requests

from .config import DISCORD_WEBHOOK_URL

logger = logging.getLogger(__name__)


def send_alert(job: str, error: Exception, severity: str = "error") -> None:
    message = f"**[{severity.upper()}] {job}**\n```{type(error).__name__}: {error}\n{traceback.format_exc()[-1000:]}```"
    delivered = 0

    if DISCORD_WEBHOOK_URL:
        try:
            resp = requests.post(
                DISCORD_WEBHOOK_URL,
                json={"content": message[:2000]},
                timeout=10,
            )
            resp.raise_for_status()
            delivered = 1
        except Exception as exc:
            logger.error("Discord alert failed: %s", exc)

    if not delivered:
        logger.error("ALERT [%s] %s: %s", severity, job, error)

    try:
        from . import database
        database.log_alert(job=job, severity=severity, message=str(error)[:2000], delivered=delivered)
    except Exception:
        pass
