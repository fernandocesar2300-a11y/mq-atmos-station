"""
Station calibration (IPMA observations vs model at the station, produced by
ipma_calibration.py every 6 h). Applied only when it is credible:
fresh < 48 h, station within MAX_DIST_KM, |temp_offset| ≤ MAX_TEMP_OFFSET,
wind_factor inside [WF_MIN, WF_MAX]. Anything else is reported, not applied.
"""
import datetime
import json
import pathlib
from typing import Dict, Tuple

from .clock import UTC, now_utc

MAX_AGE_H = 48; MAX_DIST_KM = 25.0; MAX_TEMP_OFFSET = 3.0; WF_MIN, WF_MAX = 0.6, 1.6


def load(path: str) -> Tuple[Dict[str, dict], Dict[str, str]]:
    """Returns (applicable corrections by point name, report by point name)."""
    ok, report = {}, {}
    p = pathlib.Path(path)
    if not p.exists():
        return ok, {"_": "file not found"}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        return ok, {"_": f"unreadable: {e}"}
    now = now_utc()
    for name, c in data.items():
        try:
            lu = datetime.datetime.fromisoformat(c["last_update"])
            if lu.tzinfo is None:
                lu = lu.replace(tzinfo=UTC)
            age_h = (now - lu).total_seconds() / 3600
        except Exception:
            report[name] = "no timestamp"; continue
        dist = float(c.get("station_dist_km", 999)); to = float(c.get("temp_offset", 0)); wf = float(c.get("wind_factor", 1))
        if age_h > MAX_AGE_H:          report[name] = f"stale {age_h:.0f}h"; continue
        if dist > MAX_DIST_KM:         report[name] = f"station {dist:.0f} km away"; continue
        if abs(to) > MAX_TEMP_OFFSET:  report[name] = f"temp_offset {to:+.1f} out of range"; continue
        if not (WF_MIN <= wf <= WF_MAX): report[name] = f"wind_factor {wf:.2f} out of range"; continue
        ok[name] = {"temp_offset": to, "wind_factor": wf, "station": c.get("station_name"), "dist_km": dist, "age_h": round(age_h, 1)}
        report[name] = f"applied {c.get('station_name')} {dist:.0f} km T{to:+.1f} W×{wf:.2f}"
    return ok, report
