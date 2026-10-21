"""
field.json · 7-day forecast per point, hourly inside the riding window, with
ensemble probabilities from ECMWF IFS members. This is what DSS Field renders.

Probabilities are member fractions, not model opinions:
  p_rain      P(precip >= 0.5 mm/h)
  p_gust_50   P(gust >= gust_kmh threshold, default 50)
  p_cold      P(T <= cold_c threshold, default 5)
  p_heat      P(T >= heat_c threshold, default 28)
Spread = p10..p90 of members. n_members tells the reader how many were used.
"""
import datetime
import statistics
import uuid
from typing import Dict, Optional

from .. import ENGINE_VERSION, SCHEMA_VERSION
from ..clock import now_utc, to_local, parse_iso_utc
from ..consensus import merge, slice_hour
from ..physics import eei as eei_mod, heat as heat_mod, microclimate as mc
from ..sources.openmeteo import models_identical

DEFAULT_THR = {"rain_mmh": 0.5, "gust_kmh": 50.0, "cold_c": 5.0, "heat_c": 28.0}


def _pct(vals, q):
    if not vals:
        return None
    s = sorted(vals); k = (len(s) - 1) * q; f = int(k); c = min(f + 1, len(s) - 1)
    return round(s[f] + (s[c] - s[f]) * (k - f), 1)


def _frac(vals, pred):
    return round(sum(1 for v in vals if pred(v)) / len(vals), 2) if vals else None


def twilight(lat, lon, day_local: datetime.date, tz: str):
    """Sunrise, sunset (h=-0.83°) and civil dusk (h=-6°) by minute scan. Local ISO times."""
    from zoneinfo import ZoneInfo
    z = ZoneInfo(tz)
    start = datetime.datetime.combine(day_local, datetime.time(0, 0), tzinfo=z)
    prev = eei_mod.solar_elevation(lat, lon, start)
    sunrise = sunset = dusk = dawn = None
    for m in range(1, 24 * 60, 2):
        ts = start + datetime.timedelta(minutes=m)
        h = eei_mod.solar_elevation(lat, lon, ts)
        if dawn is None and prev < -6 <= h: dawn = ts
        if sunrise is None and prev < -0.83 <= h: sunrise = ts
        if sunset is None and prev >= -0.83 > h: sunset = ts
        if dusk is None and prev >= -6 > h: dusk = ts
        prev = h
    f = lambda t: t.strftime("%H:%M") if t else None
    return {"civil_dawn": f(dawn), "sunrise": f(sunrise), "sunset": f(sunset), "civil_dusk": f(dusk)}


def build_point(event, point, fetched: Dict, ensemble: Optional[Dict], ts_now: datetime.datetime) -> dict:
    thr = {**DEFAULT_THR, **event.thresholds}
    models, times = fetched["models"], fetched["time"]
    identical = models_identical(models)
    ens_times = ensemble["time"] if ensemble else []
    ens_idx = {t: i for i, t in enumerate(ens_times)}
    days: Dict[str, dict] = {}

    for i, t in enumerate(times):
        ts = parse_iso_utc(t); loc = to_local(ts, event.timezone)
        if not (event.window.start_local <= loc.hour <= event.window.end_local):
            continue
        c, div = merge(slice_hour(models, i), None, identical)
        if c["temp"] is None:
            continue
        c = mc.apply(point, c, ts, event.timezone)
        e, est, _ = eei_mod.eei(c["temp"], c["wind"], c["hum"], c["rain"], c["irradiance"], point.lat, point.lon, ts, point.altitude_m)
        row = {
            "time_local": loc.strftime("%H:%M"), "time_utc": t,
            "temp": c["temp"], "wind": c["wind"], "gust": c["gust"], "rain": c["rain"], "hum": c["hum"],
            "code": c["code"], "irradiance": c["irradiance"],
            "eei": e, "eei_level": est["level"],
            "wbgt": heat_mod.wbgt(c["temp"], c["hum"], c["irradiance"], c["wind"]),
            "consensus": div["status"],
        }
        j = ens_idx.get(t)
        if j is not None and ensemble:
            m = ensemble["members"]
            T = m.get("temperature_2m", [[]])[j] if j < len(m.get("temperature_2m", [])) else []
            P = m.get("precipitation", [[]])[j] if j < len(m.get("precipitation", [])) else []
            G = m.get("wind_gusts_10m", [[]])[j] if j < len(m.get("wind_gusts_10m", [])) else []
            gust_factor = (c["gust"] / (statistics.median(G) or c["gust"])) if G and c["gust"] else 1.0  # carry microclimate exposure into members
            Gx = [g * gust_factor for g in G]
            row["ensemble"] = {
                "n_members": max(len(T), len(P), len(G)),
                "p_rain": _frac(P, lambda v: v >= thr["rain_mmh"]),
                "p_gust_50": _frac(Gx, lambda v: v >= thr["gust_kmh"]),
                "p_cold": _frac(T, lambda v: v <= thr["cold_c"]),
                "p_heat": _frac(T, lambda v: v >= thr["heat_c"]),
                "temp_p10": _pct(T, 0.1), "temp_p90": _pct(T, 0.9),
                "gust_p10": _pct(Gx, 0.1), "gust_p90": _pct(Gx, 0.9),
                "rain_p90": _pct(P, 0.9),
            }
        key = loc.strftime("%Y-%m-%d")
        days.setdefault(key, {"date": key, "hours": []})["hours"].append(row)

    out_days = []
    for key in sorted(days):
        hrs = days[key]["hours"]
        ens_rows = [h["ensemble"] for h in hrs if "ensemble" in h]
        day_local = datetime.date.fromisoformat(key)
        out_days.append({
            "date": key, "is_event_day": (event.event_date == key),
            "astro": twilight(point.lat, point.lon, day_local, event.timezone),
            "summary": {
                "temp_min": min(h["temp"] for h in hrs), "temp_max": max(h["temp"] for h in hrs),
                "rain_total": round(sum(h["rain"] for h in hrs), 1),
                "gust_max": max(h["gust"] for h in hrs), "wind_max": max(h["wind"] for h in hrs),
                "eei_min": min(h["eei"] for h in hrs), "wbgt_max": max(h["wbgt"] for h in hrs),
                "p_rain_max": max((r["p_rain"] for r in ens_rows if r["p_rain"] is not None), default=None),
                "p_gust_50_max": max((r["p_gust_50"] for r in ens_rows if r["p_gust_50"] is not None), default=None),
                "p_cold_max": max((r["p_cold"] for r in ens_rows if r["p_cold"] is not None), default=None),
                "p_heat_max": max((r["p_heat"] for r in ens_rows if r["p_heat"] is not None), default=None),
                "consensus_worst": max((h["consensus"] for h in hrs), key=lambda s: {"AGREE": 0, "UNCERTAIN": 1, "DIVERGENT": 2, "INVALID": 3}[s]),
            },
            "hours": hrs,
        })
    return {
        "id": point.id, "name": point.name, "coords": {"lat": point.lat, "lon": point.lon, "elevation": point.altitude_m},
        "terrain_type": point.terrain, "ensemble_available": ensemble is not None,
        "ensemble_members": ensemble["n_members"] if ensemble else 0,
        "sources": {"missing": fetched.get("missing", {}), "rejected": fetched.get("rejected", {}), "models_identical": identical},
        "days": out_days,
    }


def build(event, per_point: Dict[int, dict], failed: list, ts_now: Optional[datetime.datetime] = None) -> dict:
    ts_now = ts_now or now_utc()
    pts = [per_point[p.id] for p in event.sorted_points if p.id in per_point]
    return {
        "schema_version": SCHEMA_VERSION, "engine_version": ENGINE_VERSION, "product": "field",
        "run_id": uuid.uuid4().hex[:12], "event": event.slug, "event_name": event.name,
        "event_date": event.event_date, "timezone": event.timezone,
        "window": {"start_local": event.window.start_local, "end_local": event.window.end_local},
        "thresholds": {**DEFAULT_THR, **event.thresholds},
        "timestamp_utc": ts_now.isoformat(), "timestamp_local": to_local(ts_now, event.timezone).isoformat(),
        "degraded": False, "degraded_reason": None,
        "data_sources": ["ECMWF IFS 0.25° deterministic + 51-member ensemble (Open-Meteo)", "DWD ICON-EU (Open-Meteo)",
                         "DSS microclimate — wind exposure, valley window"],
        "points_failed": failed, "points_ok": len(pts), "points_expected": len(event.points),
        "points": pts,
    }
