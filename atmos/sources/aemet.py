"""AEMET CAP storm alerts for NW Iberia. Optional: needs AEMET_API_KEY."""
import os
import xml.etree.ElementTree as ET
from typing import Optional, Tuple

import requests

SEV_RANK = {"Extreme": 4, "Severe": 3, "Moderate": 2, "Minor": 1}
KEYS = ("storm", "thunder", "tormen", "trovo", "granizo", "hail", "electric", "lightning")


def storm_alerts(area: Optional[str]) -> Tuple[bool, str, Optional[str]]:
    key = os.environ.get("AEMET_API_KEY", "")
    if not key or not area:
        return False, "AEMET not configured", None
    try:
        r = requests.get(f"https://opendata.aemet.es/opendata/api/avisos_cap/ultimoelaborado/area/{area}",
                         params={"api_key": key}, timeout=8)
        if r.status_code != 200:
            return False, f"AEMET HTTP {r.status_code}", None
        url = r.json().get("datos")
        if not url:
            return False, "AEMET no data URL", None
        r2 = requests.get(url, timeout=10)
        if r2.status_code != 200:
            return False, f"AEMET CAP HTTP {r2.status_code}", None
        ns = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}
        root = ET.fromstring(r2.content)
        events, max_sev = [], None
        for info in root.findall(".//cap:info", ns):
            ev = info.findtext("cap:event", "", ns); sev = info.findtext("cap:severity", "", ns)
            if any(k in ev.lower() for k in KEYS):
                events.append(f"{ev} ({sev})")
                if sev in SEV_RANK and (max_sev is None or SEV_RANK[sev] > SEV_RANK[max_sev]):
                    max_sev = sev
        if events:
            return True, "AEMET: " + ", ".join(events[:3]), max_sev
        return False, "AEMET: no active storm warnings", None
    except Exception as e:
        return False, f"AEMET error: {str(e)[:60]}", None
