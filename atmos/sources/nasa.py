"""NASA POWER hourly irradiance. Fallback only: ERA/MERRA products lag days, so
the current day usually returns fill values (-999), which are discarded."""
import datetime
from typing import Dict, List, Optional

import requests

URL = "https://power.larc.nasa.gov/api/temporal/hourly/point"


def irradiance_day(lat: float, lon: float, date_utc: datetime.datetime) -> Optional[List[Optional[float]]]:
    ds = date_utc.strftime("%Y%m%d")
    try:
        r = requests.get(URL, params={"parameters": "ALLSKY_SFC_SW_DWN", "community": "RE", "longitude": lon,
                                      "latitude": lat, "start": ds, "end": ds, "format": "JSON",
                                      "time-standard": "UTC"}, timeout=10)
        if r.status_code != 200:
            return None
        hd = r.json()["properties"]["parameter"]["ALLSKY_SFC_SW_DWN"]
        vals = [hd.get(f"{ds}{h:02d}", -999) for h in range(24)]
        if all(v is None or v < 0 for v in vals):
            return None
        return [v if (v is not None and v >= 0) else None for v in vals]
    except Exception:
        return None


def fetch_all(points, date_utc) -> Dict[int, List]:
    out = {}
    for p in points:
        v = irradiance_day(p.lat, p.lon, date_utc)
        if v:
            out[p.id] = v
    return out
