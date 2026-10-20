"""
status.json · nowcast per point: +0 / +3 / +6 h.
Consumers: Track Status web, Ghost Rider, BIOS. Contract: ATMOS_CONTRACT.md.
"""
import datetime
import uuid
from typing import Dict, Optional

from .. import ENGINE_VERSION, SCHEMA_VERSION
from ..clock import now_utc, to_local, parse_iso_utc
from ..consensus import merge, slice_hour
from ..physics import eei as eei_mod, heat as heat_mod, storm as storm_mod, microclimate as mc
from ..sources.openmeteo import models_identical, MODEL_IFS, MODEL_ICON

HEAT_RANK = heat_mod.RANK


def _hour_index(times, ts_utc: datetime.datetime) -> int:
    """Index of the hourly slot containing ts_utc (floor). Arrays are UTC."""
    if not times:
        return 0
    t0 = parse_iso_utc(times[0])
    idx = int((ts_utc - t0).total_seconds() // 3600)
    return max(0, min(idx, len(times) - 1))


def _in_window(ts_utc, event) -> bool:
    h = to_local(ts_utc, event.timezone).hour
    return event.window.start_local <= h <= event.window.end_local


def evaluate_point(event, point, fetched: Dict, ipma_fc: Optional[dict], aemet, nasa: Optional[list],
                   ts_now: datetime.datetime, calib: Optional[dict] = None) -> dict:
    """Pure evaluation of one point from already-fetched data. No I/O except IPMA warnings (cached)."""
    models = fetched["models"]; times = fetched["time"]
    identical = models_identical(models)
    h0 = _hour_index(times, ts_now)
    slots = {}
    for off in (0, 3, 6):
        ts = ts_now + datetime.timedelta(hours=off)
        hour = slice_hour(models, h0 + off)
        c, div = merge(hour, ipma_fc if off == 0 else None, identical)
        if c["temp"] is None:
            raise ValueError("no temperature from any source")
        # freezing level sanity
        efl = point.altitude_m + c["temp"] / 0.0065
        if abs(c["freezing_level"] - efl) > 500:
            c["freezing_level"] = round(efl, 0)
        nasa_val = nasa[(h0 + off) % 24] if nasa else None
        c = mc.apply(point, c, ts, event.timezone, nasa_val, calib)
        slots[off] = (c, div, ts)

    d0, div0, ts0 = slots[0]
    e_now, est, _ = eei_mod.eei(d0["temp"], d0["wind"], d0["hum"], d0["rain"], d0["irradiance"], point.lat, point.lon, ts0, point.altitude_m)
    e_3h, _, _ = eei_mod.eei(*(slots[3][0][k] for k in ("temp", "wind", "hum", "rain", "irradiance")), point.lat, point.lon, slots[3][2], point.altitude_m)
    e_6h, _, _ = eei_mod.eei(*(slots[6][0][k] for k in ("temp", "wind", "hum", "rain", "irradiance")), point.lat, point.lon, slots[6][2], point.altitude_m)

    status, color = est["level"], est["color"]
    h_status, h_color, heat_level, wbgt = heat_mod.evaluate(d0["temp"], d0["hum"], d0["irradiance"], d0["wind"], _in_window(ts0, event))
    s_status, s_color, storm_level, ipma_texts = storm_mod.evaluate(
        point.ipma_areas, event.timezone, d0["code"], d0["cape"], d0["lifted_index"], d0["cin"],
        d0["lightning_potential"], aemet[0], aemet[2], div0["storm_confirmed"], div0["hail_confirmed"])

    snow, snow_int, mixed = False, None, False
    if s_status:
        status, color = s_status, s_color
    elif 0 < d0["temp"] <= 3 and d0["rain"] > 0.1:
        mixed = True
        status, color = ("SNOW LIKELY", "#3498db") if d0["temp"] <= 1.5 else ("RAIN/SNOW MIX", "#e67e22")
    elif (d0["rain"] > 0.1 and d0["temp"] <= 1) or (point.altitude_m > d0["freezing_level"] and d0["freezing_level"] < 9000 and d0["rain"] > 0.1):
        snow = True
        snow_int = "HEAVY" if d0["rain"] > 10 else "MODERATE" if d0["rain"] > 3 else "LIGHT"
        status, color = {"HEAVY": ("HEAVY SNOWFALL", "#e74c3c"), "MODERATE": ("SNOWFALL", "#e67e22"), "LIGHT": ("LIGHT SNOW", "#f1c40f")}[snow_int]
    elif h_status:
        status, color = h_status, h_color

    def fc(off):
        c, _, _ = slots[off]
        return {"temp": c["temp"], "wind": c["wind"], "gust": c["gust"], "rain": c["rain"], "code": c["code"],
                "lightning_potential": round((c.get("lightning_potential") or 0) * 100, 1)}

    return {
        "id": point.id, "name": point.name,
        "coords": {"lat": point.lat, "lon": point.lon, "elevation": point.altitude_m},
        "terrain_type": point.terrain, "description": point.description,
        "current": {
            "eei": int(e_now), "status": status, "status_rank": mc.rank_status(status), "color": color,
            "temp": d0["temp"], "wind": d0["wind"], "gust": d0["gust"], "rain": d0["rain"], "humidity": d0["hum"],
            "irradiance": d0["irradiance"], "irradiance_source": d0["irradiance_source"],
            "freezing_level": d0["freezing_level"], "code": d0["code"],
            "snow_detected": snow or mixed, "snow_intensity": snow_int,
            "cape": round(d0["cape"], 0), "lifted_index": round(d0["lifted_index"], 1), "cin": round(d0["cin"], 0),
            "lightning_potential": round((d0.get("lightning_potential") or 0) * 100, 1),
            "storm_level": storm_level, "ipma_warnings": ipma_texts,
            "heat": {"wbgt_c": wbgt, "heat_level": heat_level, "in_window": _in_window(ts0, event)},
            "microclimate_notes": d0.get("fog_alert") or d0.get("mtb_hazard"),
            "microclimate_log": d0.get("_log", []),
            "calibration": calib,
        },
        "forecast_3h": {"eei": e_3h, **fc(3)},
        "forecast_6h": {"eei": e_6h, **fc(6)},
        "consensus": {k: div0[k] for k in ("status", "models_identical", "temp_spread", "precip_spread", "wind_spread",
                                           "gust_spread", "ifs_temp", "icon_temp", "ifs_gust", "icon_gust", "ipma_temp",
                                           "storm_confirmed", "hail_confirmed", "clear_confirmed", "sources_active", "ipma_station")},
        "sources": {"missing": fetched.get("missing", {}), "rejected": fetched.get("rejected", {})},
    }


def build(event, per_point: Dict[int, dict], failed: list, aemet, ipma_active: bool, nasa_cov: str, ipma_cov: str,
          ts_now: Optional[datetime.datetime] = None) -> dict:
    ts_now = ts_now or now_utc()
    sectors = [per_point[p.id] for p in event.sorted_points if p.id in per_point]
    worst = max(sectors, key=lambda s: s["current"]["status_rank"], default=None)
    identical = any(s["consensus"]["models_identical"] for s in sectors)
    heat_level = max((s["current"]["heat"]["heat_level"] for s in sectors), key=lambda l: HEAT_RANK[l], default="NONE")
    storm_level = max((s["current"]["storm_level"] for s in sectors),
                      key=lambda l: {"NONE": 0, "LOW": 1, "MODERATE": 2, "HIGH": 3, "SEVERE": 4, "EXTREME": 5}[l], default="NONE")
    div_log = [{"sector": s["name"], "status": s["consensus"]["status"], "temp_spread": s["consensus"]["temp_spread"],
                "gust_spread": s["consensus"]["gust_spread"]} for s in sectors]
    return {
        "schema_version": SCHEMA_VERSION, "engine_version": ENGINE_VERSION, "product": "status",
        "run_id": uuid.uuid4().hex[:12],
        "event": event.slug, "event_name": event.name,
        "timestamp_utc": ts_now.isoformat(), "timestamp_local": to_local(ts_now, event.timezone).isoformat(),
        "last_update": ts_now.strftime("%H:%M"),
        "degraded": False, "degraded_reason": None,
        "data_sources": [
            f"ECMWF IFS 0.25° (Open-Meteo {MODEL_IFS}) — primary", f"DWD ICON-EU (Open-Meteo {MODEL_ICON}) — secondary + storm diagnostics",
            "IPMA point forecast — anchor", "IPMA warnings — institutional override", "AEMET CAP — NW Iberia override",
            "NASA POWER — irradiance fallback", "DSS microclimate — wind exposure, valley window",
        ],
        "consensus_engine": {"weights": {"IFS": 0.55, "ICON": 0.35, "IPMA": 0.10}, "models_identical_detected": identical},
        "summary": {
            "alert_level": worst["current"]["status"] if worst else "STABLE",
            "alert_rank": worst["current"]["status_rank"] if worst else 0,
            "worst_sector": worst["name"] if worst else "ALL SECTORS",
            "min_mri": min((s["current"]["eei"] for s in sectors), default=None),
            "max_wind": max((s["current"]["wind"] for s in sectors), default=None),
            "max_gust": max((s["current"]["gust"] for s in sectors), default=None),
            "snow_detected": any(s["current"]["snow_detected"] for s in sectors),
            "storm_detected": storm_level != "NONE", "storm_level": storm_level,
            "heat_detected": heat_level != "NONE", "heat_level": heat_level,
            "max_wbgt_c": max((s["current"]["heat"]["wbgt_c"] for s in sectors), default=None),
            "aemet_storm_alert": aemet[0], "aemet_alert_text": aemet[1], "ipma_active": ipma_active,
            "nasa_power_coverage": nasa_cov, "ipma_forecast_coverage": ipma_cov,
            "divergence_log": div_log,
            "divergent_sectors": [d["sector"] for d in div_log if d["status"] == "DIVERGENT"],
            "sectors_failed": failed, "sectors_ok": len(sectors), "sectors_expected": len(event.points),
        },
        "sectors": sectors,
    }
