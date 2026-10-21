"""IPMA open data: warnings (institutional override) + point forecast anchor."""
import json
import urllib.request
from typing import Dict, List, Optional, Tuple

from ..clock import now_utc, parse_local_naive

WARNINGS_URL  = "https://api.ipma.pt/open-data/forecast/warnings/warnings_www.json"
LOCATIONS_URL = "https://api.ipma.pt/open-data/forecast/locations.json"
CITY_URL      = "https://api.ipma.pt/open-data/forecast/meteorology/cities/{kind}/{gid}.json"

STORM_TYPES = {"Trovoada", "Precipitação", "Neve", "Vento"}
LEVEL_RANK  = {"red": 4, "orange": 3, "yellow": 2, "green": 0}
LAPSE_C_PER_M = 0.0065

_WTYPE_TO_CODE = {1: 0, 2: 1, 3: 2, 4: 3, 5: 45, 6: 51, 7: 61, 8: 63, 9: 80, 10: 80,
                  11: 82, 12: 95, 13: 96, 14: 71, 15: 77}
_WIND_CLASS_KPH = {1: 7, 2: 20, 3: 31, 4: 45, 5: 60}

_cache: Dict[str, object] = {}


def _json(url: str, timeout: int = 8):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())


def warnings(tz: str = "Europe/Lisbon") -> List[dict]:
    if "warnings" not in _cache:
        try:
            _cache["warnings"] = _json(WARNINGS_URL)
        except Exception:
            _cache["warnings"] = []
    return _cache["warnings"]


def active_for_areas(areas: List[str], tz: str = "Europe/Lisbon") -> Tuple[bool, Optional[str], List[str]]:
    """Active storm-class warnings for the given IPMA area ids, compared in UTC."""
    now = now_utc()
    found, max_rank, max_level = [], 0, None
    for w in warnings(tz):
        if w.get("idAreaAviso") not in areas or w.get("awarenessTypeName") not in STORM_TYPES:
            continue
        level = w.get("awarenessLevelID", "green"); rank = LEVEL_RANK.get(level, 0)
        if rank < 2:
            continue
        try:
            start = parse_local_naive(w["startTime"], tz); end = parse_local_naive(w["endTime"], tz)
        except Exception:
            continue
        if start <= now <= end:
            found.append(f"[{level.upper()}] {w.get('awarenessTypeName','')}: {w.get('text','')[:60]}")
            if rank > max_rank:
                max_rank, max_level = rank, level
    return bool(found), max_level, found


def to_storm_status(level: Optional[str], texts: List[str]):
    c = " ".join(texts).lower()
    hail = any(k in c for k in ("granizo", "hail")); thunder = any(k in c for k in ("trovoada", "thunder", "elétric"))
    if level == "red":
        return ("HAIL — DO NOT RIDE" if hail else "EXTREME STORM — IPMA RED"), "EXTREME"
    if level == "orange":
        if hail:    return "HAIL WARNING — IPMA", "SEVERE"
        if thunder: return "SEVERE STORM — IPMA", "SEVERE"
        return "STORM WARNING — IPMA ORANGE", "SEVERE"
    if level == "yellow":
        if hail:    return "HAIL POSSIBLE — IPMA", "HIGH"
        if thunder: return "THUNDERSTORM — IPMA", "HIGH"
        return "STORM WARNING — IPMA YELLOW", "MODERATE"
    return None, "NONE"


def locations() -> List[dict]:
    if "locations" not in _cache:
        try:
            _cache["locations"] = _json(LOCATIONS_URL)
        except Exception:
            _cache["locations"] = []
    return _cache["locations"]


def nearest_location(lat: float, lon: float) -> Optional[dict]:
    locs = locations()
    if not locs:
        return None
    return min(locs, key=lambda l: (float(l["latitude"]) - lat) ** 2 + (float(l["longitude"]) - lon) ** 2)


def point_forecast(lat: float, lon: float, altitude_m: int, tz: str,
                   station_gid: Optional[int] = None, station_alt: Optional[int] = None) -> Optional[dict]:
    """
    Nearest-station anchor. Temperature is lapse-corrected to the point altitude
    only when the station altitude is known; otherwise temp=None (wind still used).
    Hourly endpoint first, daily as fallback; both schemas tolerated.
    """
    gid = station_gid
    name = None
    if gid is None:
        loc = nearest_location(lat, lon)
        if not loc:
            return None
        gid, name = int(loc["globalIdLocal"]), loc.get("local")
    entries, schema = [], None
    for kind in ("hourly", "daily"):
        try:
            data = _json(CITY_URL.format(kind=kind, gid=gid))
            entries = data.get("data", [])
            if entries:
                schema = kind; break
        except Exception:
            continue
    if not entries:
        return None
    from ..clock import to_local
    import datetime
    now_l = to_local(now_utc(), tz).replace(tzinfo=None)

    def dist(e):
        k = e.get("forecastDate") or e.get("dataPrev")
        try:
            return abs((datetime.datetime.fromisoformat(k) - now_l).total_seconds())
        except Exception:
            return float("inf")
    best = min(entries, key=dist)
    temp = best.get("tMed")
    if temp is None and best.get("tMin") is not None and best.get("tMax") is not None:
        temp = (float(best["tMin"]) + float(best["tMax"])) / 2.0
    temp = float(temp) if temp is not None else None
    if temp is not None:
        temp = temp - (altitude_m - station_alt) * LAPSE_C_PER_M if station_alt is not None else None
    return {
        "temp": temp,
        "wind": _WIND_CLASS_KPH.get(int(best.get("classWindSpeed", 1)), 20),
        "code": _WTYPE_TO_CODE.get(int(best.get("idWeatherType", 1)), 0),
        "source": f"IPMA/{name or gid} ({schema})",
    }
