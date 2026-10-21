"""Dead-man switch. HEALTHCHECK_URL (healthchecks.io or any ping endpoint).
/start on begin, plain ping on success, /fail on failure. Silence → alert."""
import os

import requests


def ping(suffix: str = "", message: str = "") -> None:
    url = os.environ.get("HEALTHCHECK_URL") or ""
    if not url:
        return
    try:
        requests.post(url.rstrip("/") + suffix, data=message[:1000], timeout=8)
    except Exception:
        pass
