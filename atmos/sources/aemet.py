"""AEMET CAP storm alerts for the Spanish zones bordering the course. Optional: needs AEMET_API_KEY.

The `datos` URL returns a TAR archive with one CAP XML per warning (not a single XML), each
with the same <info> twice (es-ES, en-GB). Every phenomenon is also published at "nivel verde"
(= no warning), so level and validity must be checked: a file mentioning "tormentas" is not a storm.
"""
import datetime
import io
import os
import tarfile
import xml.etree.ElementTree as ET
from typing import List, Optional, Tuple

import requests

from ..clock import now_utc

SEV_RANK = {"Extreme": 4, "Severe": 3, "Moderate": 2, "Minor": 1}
KEYS = ("storm", "thunder", "tormen", "trovo", "granizo", "hail", "electric", "lightning")
NS = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}


def _documents(payload: bytes) -> List[bytes]:
    """CAP XML documents inside the payload: a tar of XMLs (current API) or a bare XML (legacy)."""
    try:
        with tarfile.open(fileobj=io.BytesIO(payload)) as t:
            return [t.extractfile(m).read() for m in t.getmembers() if m.isfile()]
    except tarfile.ReadError:
        return [payload]


def _dt(s: str) -> Optional[datetime.datetime]:
    try:
        d = datetime.datetime.fromisoformat(s)
        return d if d.tzinfo else None
    except Exception:
        return None


def parse(payload: bytes, zones: Optional[List[str]] = None, now: Optional[datetime.datetime] = None) -> Tuple[bool, str, Optional[str]]:
    """Active storm warnings (yellow+) in force now, optionally restricted to AEMET zone geocodes."""
    now = now or now_utc()
    events, max_sev = [], None
    for doc in _documents(payload):
        root = ET.fromstring(doc)
        for info in root.findall(".//cap:info", NS):
            if not info.findtext("cap:language", "es-ES", NS).lower().startswith("es"):
                continue                                             # en-GB duplicate of the same warning
            ev = info.findtext("cap:event", "", NS); sev = info.findtext("cap:severity", "", NS)
            if not any(k in ev.lower() for k in KEYS):
                continue
            if "verde" in ev.lower() or SEV_RANK.get(sev, 0) < 2:
                continue                                             # "nivel verde" / Minor = no warning
            onset = _dt(info.findtext("cap:onset", "", NS) or info.findtext("cap:effective", "", NS))
            expires = _dt(info.findtext("cap:expires", "", NS))
            if (onset and onset > now) or (expires and expires <= now):
                continue                                             # not in force now
            codes = {g.findtext("cap:value", "", NS) for a in info.findall("cap:area", NS) for g in a.findall("cap:geocode", NS)}
            if zones and not (codes & set(zones)):
                continue
            desc = ", ".join(a.findtext("cap:areaDesc", "", NS) for a in info.findall("cap:area", NS))
            events.append(f"{ev} · {desc} ({sev})")
            if max_sev is None or SEV_RANK[sev] > SEV_RANK[max_sev]:
                max_sev = sev
    if events:
        return True, "AEMET: " + "; ".join(sorted(set(events))[:3]), max_sev
    return False, "AEMET: no active storm warnings", None


def storm_alerts(area: Optional[str], zones: Optional[List[str]] = None) -> Tuple[bool, str, Optional[str]]:
    key = os.environ.get("AEMET_API_KEY") or ""
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
        r2 = requests.get(url, timeout=15)
        if r2.status_code != 200:
            return False, f"AEMET CAP HTTP {r2.status_code}", None
        return parse(r2.content, zones)
    except Exception as e:
        return False, f"AEMET error: {str(e)[:60]}", None
