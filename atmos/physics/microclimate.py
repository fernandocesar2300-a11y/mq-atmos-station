"""
Terrain corrections. Temperature is NOT re-corrected for altitude: Open-Meteo
already applies lapse-rate to the point elevation. Wind exposure is tiered.
Valley warming only in the mixed-layer window (11h–18h local).
Irradiance priority: model > NASA (>0) > weather-code estimate.
"""
import datetime
from typing import Optional

from ..clock import to_local

_IRR_BY_CODE = {0: 850, 1: 650, 2: 450, 3: 250, 45: 150, 48: 100, 51: 200, 53: 150, 55: 100,
                61: 150, 63: 100, 65: 50, 71: 100, 73: 80, 75: 50, 77: 50, 80: 200, 81: 150,
                82: 100, 85: 80, 86: 60, 95: 50, 96: 30, 99: 30}


def irradiance_estimate(code: int, hour_local: int) -> float:
    if hour_local < 6 or hour_local > 20:
        return 0.0
    return round(_IRR_BY_CODE.get(code, 300) * max(0.0, 1.0 - abs(hour_local - 13) / 7.0), 1)


def rank_status(status: Optional[str]) -> int:
    s = (status or "").upper()
    if any(k in s for k in ("HAIL — DO NOT RIDE", "EXTREME", "IPMA RED", "CRITICAL")): return 90
    if any(k in s for k in ("SEVERE", "HAIL WARNING", "HEAVY SNOW", "DANGER", "IMMINENT")): return 80
    if any(k in s for k in ("THUNDERSTORM", "LIGHTNING RISK — HIGH", "HAIL POSSIBLE")): return 70
    if any(k in s for k in ("LIGHTNING RISK", "STORM PROBABLE", "SNOWFALL", "SNOW LIKELY", "HEAT WARNING", "STORM WARNING", "WARNING")): return 60
    if any(k in s for k in ("STORM POSSIBLE", "LIGHTNING POSSIBLE", "RAIN/SNOW", "LIGHT SNOW", "HEAT CAUTION", "CAUTION", "INSTABILITY")): return 40
    if s == "" or "GOOD" in s or "STABLE" in s: return 0
    return 50


def apply(point, d: dict, ts_utc: datetime.datetime, tz: str, nasa_val: Optional[float] = None,
          calib: Optional[dict] = None) -> dict:
    d = dict(d); alt = point.altitude_m; log = []
    hour_local = to_local(ts_utc, tz).hour

    if calib:
        if calib.get("temp_offset") and d.get("temp") is not None:
            d["temp"] = round(d["temp"] + calib["temp_offset"], 1); log.append(f"calT{calib['temp_offset']:+.1f}")
        wf = calib.get("wind_factor", 1.0)
        if wf != 1.0:
            d["wind"] = round(d["wind"] * wf, 1); d["gust"] = round(d["gust"] * wf, 1); log.append(f"calW×{wf:.2f}")

    if alt > 1200 and point.terrain in ("CLIMB", "FLAT"):
        d["wind"] = round(d["wind"] * 1.60, 1); d["gust"] = round(d["gust"] * 1.40, 1); log.append("ridge+60%V")
    elif alt > 1000:
        d["wind"] = round(d["wind"] * 1.30, 1); d["gust"] = round(d["gust"] * 1.20, 1); log.append("high+30%V")

    if 400 < alt < 800 and 11 <= hour_local <= 18 and d.get("temp") is not None:
        d["temp"] = round(d["temp"] + 1.5, 1); log.append("valley+1.5")

    if alt < 800 and (d.get("hum") or 0) > 85 and d.get("temp") is not None and 8 <= d["temp"] <= 12:
        d["fog_alert"] = "NORTADA FOG"; log.append("fog")
    if (d.get("rain") or 0) > 5:
        d["mtb_hazard"] = "SLIPPERY TRACK"; log.append("hazard")

    if (d.get("irradiance") or 0) <= 0:
        if nasa_val is not None and nasa_val > 0:
            d["irradiance"], d["irradiance_source"] = nasa_val, "NASA_POWER"; log.append("irr<-nasa")
        else:
            est = irradiance_estimate(d.get("code", 0), hour_local)
            if est > 0:
                d["irradiance"], d["irradiance_source"] = est, "CODE_ESTIMATE"; log.append("irr<-est")
    d["_log"] = log
    return d
