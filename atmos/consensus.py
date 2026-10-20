"""
Weighted merge of deterministic models + IPMA anchor, with divergence grading.

Weights normalise over the sources that actually have a value, so a missing
value never drags the mean toward zero. If both models are identical the
secondary weight is zeroed and the divergence is INVALID, never AGREE.
"""
from typing import Dict, Optional

from .sources.openmeteo import MODEL_IFS, MODEL_ICON

W_IFS, W_ICON, W_IPMA = 0.55, 0.35, 0.10

TEMP_T1, TEMP_T2     = 1.5, 3.0     # °C
PRECIP_T1, PRECIP_T2 = 2.0, 5.0     # mm/h
WIND_T1, WIND_T2     = 10.0, 20.0   # km/h
GUST_T1, GUST_T2     = 15.0, 30.0   # km/h


def _wavg(pairs):
    valid = [(v, w) for v, w in pairs if v is not None and w > 0]
    if not valid:
        return None
    tw = sum(w for _, w in valid)
    return sum(v * w for v, w in valid) / tw


def _r(v, nd):
    return None if v is None else round(v, nd)


def slice_hour(models: Dict, h: int) -> Dict[str, Dict]:
    """Extract hour h from every model column. Out-of-range → None."""
    out = {}
    for m, cols in models.items():
        out[m] = {var: (col[h] if 0 <= h < len(col) else None) for var, col in cols.items()}
    return out


def merge(hour: Dict[str, Dict], ipma: Optional[dict] = None, identical: bool = False):
    ifs, icon = hour.get(MODEL_IFS, {}), hour.get(MODEL_ICON, {})
    wi, wc, wp = W_IFS, (0.0 if identical else W_ICON), (W_IPMA if ipma else 0.0)
    g = lambda d, k: d.get(k)

    c = {
        "temp": _r(_wavg([(g(ifs, "temperature_2m"), wi), (g(icon, "temperature_2m"), wc),
                          (ipma.get("temp") if ipma else None, wp)]), 1),
        "wind": _r(_wavg([(g(ifs, "wind_speed_10m"), wi), (g(icon, "wind_speed_10m"), wc),
                          (ipma.get("wind") if ipma else None, wp)]), 1),
        "gust": _r(_wavg([(g(ifs, "wind_gusts_10m"), wi), (g(icon, "wind_gusts_10m"), wc)]), 1),
        "rain": _r(_wavg([(g(ifs, "precipitation"), wi + wp / 2), (g(icon, "precipitation"), wc + wp / 2)]), 2),
        "hum":  _r(_wavg([(g(ifs, "relative_humidity_2m"), wi + wp / 2), (g(icon, "relative_humidity_2m"), wc + wp / 2)]), 0),
        "irradiance": _r(_wavg([(g(ifs, "shortwave_radiation"), wi + wp), (g(icon, "shortwave_radiation"), wc)]), 1),
        "snowfall": _r(_wavg([(g(ifs, "snowfall"), 0.5), (g(icon, "snowfall"), 0.5)]), 2),
        "freezing_level": _r(_wavg([(g(ifs, "freezing_level_height"), 0.5), (g(icon, "freezing_level_height"), 0.5)]), 0),
        "cape": _wavg([(g(ifs, "cape"), wi), (g(icon, "cape"), wc)]) or 0.0,
        "lifted_index": g(icon, "lifted_index") or 0.0,
        "cin": g(icon, "convective_inhibition") or 0.0,
        "lightning_potential": g(icon, "lightning_potential"),
        "code": int(g(ifs, "weather_code") if g(ifs, "weather_code") is not None else (g(icon, "weather_code") or 0)),
        "code_icon": int(g(icon, "weather_code") or 0),
        "irradiance_source": "OPEN_METEO",
    }
    defaults = {"temp": None, "wind": 0.0, "gust": None, "rain": 0.0, "hum": 50.0,
                "irradiance": 0.0, "snowfall": 0.0, "freezing_level": 9999.0}
    for k, d in defaults.items():
        if c[k] is None:
            c[k] = d
    if c["gust"] is None:
        c["gust"] = c["wind"]

    d = lambda a, b: abs((a or 0) - (b or 0))
    dt = d(g(ifs, "temperature_2m"), g(icon, "temperature_2m"))
    dp = d(g(ifs, "precipitation"), g(icon, "precipitation"))
    dv = d(g(ifs, "wind_speed_10m"), g(icon, "wind_speed_10m"))
    dg = d(g(ifs, "wind_gusts_10m"), g(icon, "wind_gusts_10m"))
    ci, cc = g(ifs, "weather_code") or 0, g(icon, "weather_code") or 0

    if identical:
        status = "INVALID"
    elif dt > TEMP_T2 or dp > PRECIP_T2 or dv > WIND_T2 or dg > GUST_T2:
        status = "DIVERGENT"
    elif dt > TEMP_T1 or dp > PRECIP_T1 or dv > WIND_T1 or dg > GUST_T1:
        status = "UNCERTAIN"
    else:
        status = "AGREE"

    div = {
        "status": status, "models_identical": identical,
        "temp_spread": round(dt, 1), "precip_spread": round(dp, 1),
        "wind_spread": round(dv, 1), "gust_spread": round(dg, 1),
        "ifs_temp": _r(g(ifs, "temperature_2m"), 1), "icon_temp": _r(g(icon, "temperature_2m"), 1),
        "ifs_gust": _r(g(ifs, "wind_gusts_10m"), 0), "icon_gust": _r(g(icon, "wind_gusts_10m"), 0),
        "ipma_temp": _r(ipma.get("temp"), 1) if ipma and ipma.get("temp") is not None else None,
        "storm_confirmed": (ci >= 95 and cc >= 95) and not identical,
        "hail_confirmed": (ci in (96, 99) and cc in (96, 99)) and not identical,
        "clear_confirmed": (ci <= 2 and cc <= 2) and not identical,
        "sources_active": (1 if identical else 2) + (1 if ipma else 0),
        "ipma_station": ipma.get("source") if ipma else None,
    }
    return c, div
