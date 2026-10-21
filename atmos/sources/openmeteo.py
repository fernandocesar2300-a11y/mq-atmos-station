"""
Open-Meteo: deterministic multi-model (one call, both models) + ECMWF ensemble.

Design rules:
  * ONE request per point for the deterministic models (models=a,b). The API
    returns <var>_<model> columns; a column missing for a model means that
    model does not serve it. Halves the request count vs V21.
  * timezone=UTC always. The engine never indexes arrays by local hour.
  * Retry with backoff on 429/5xx. Explicit User-Agent (Open-Meteo asks for it).
  * Every value passes a plausibility filter before it reaches the consensus.
"""
import time
from typing import Dict, List, Optional

import requests

from .. import ENGINE_VERSION

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"

MODEL_IFS  = "ecmwf_ifs025"
MODEL_ICON = "icon_seamless"
MODELS     = [MODEL_IFS, MODEL_ICON]

# Variables (new Open-Meteo naming; the forecast API accepts both old and new)
VARS_CORE = [
    "temperature_2m", "relative_humidity_2m", "precipitation", "snowfall",
    "weather_code", "wind_speed_10m", "wind_gusts_10m", "shortwave_radiation",
    "freezing_level_height", "cape",
]
VARS_STORM = ["lifted_index", "convective_inhibition", "lightning_potential"]   # ICON only
VARS_ENSEMBLE = ["temperature_2m", "precipitation", "wind_gusts_10m", "wind_speed_10m"]

PLAUSIBLE = {   # inclusive bounds; outside → None and counted as rejected
    "temperature_2m": (-35.0, 50.0),
    "relative_humidity_2m": (0.0, 100.0),
    "precipitation": (0.0, 150.0),
    "snowfall": (0.0, 50.0),
    "weather_code": (0, 99),
    "wind_speed_10m": (0.0, 200.0),
    "wind_gusts_10m": (0.0, 250.0),
    "shortwave_radiation": (0.0, 1300.0),
    "freezing_level_height": (-500.0, 9000.0),
    "cape": (0.0, 8000.0),
    "lifted_index": (-20.0, 40.0),
    "convective_inhibition": (-2000.0, 0.0),
    "lightning_potential": (0.0, 200.0),   # LPI, J/kg (not a probability)
}

HEADERS = {"User-Agent": f"DSS-ATMOS/{ENGINE_VERSION} (fernando@dreamsports.pt)"}


class SourceError(RuntimeError):
    pass


def _get(url: str, params: dict, timeout: int = 15, retries: int = 3) -> dict:
    delay = 2.0
    last = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
        except requests.RequestException as e:
            last = e
        else:
            if r.status_code == 200:
                return r.json()
            if r.status_code == 400:
                raise SourceError(f"400 {r.text[:160]}")
            last = SourceError(f"HTTP {r.status_code}")
            if r.status_code not in (429, 500, 502, 503, 504):
                raise last
        if attempt < retries:
            time.sleep(delay); delay *= 2
    raise SourceError(f"{url}: {last}")


def plausible(var: str, v):
    if v is None:
        return None
    lo, hi = PLAUSIBLE.get(var, (-1e9, 1e9))
    try:
        fv = float(v)
    except (TypeError, ValueError):
        return None
    return fv if lo <= fv <= hi else None


def fetch_models(lat: float, lon: float, forecast_days: int = 7) -> Dict:
    """
    Returns:
      {
        'time': [iso...],                         # UTC
        'models': { model: { var: [values...] } },
        'rejected': { model: { var: n_rejected } },
        'missing':  { model: [vars not served] },
      }
    """
    params = {
        "latitude": lat, "longitude": lon, "forecast_days": forecast_days,
        "timezone": "UTC", "models": ",".join(MODELS),
        "hourly": ",".join(VARS_CORE + VARS_STORM),
    }
    raw = _get(FORECAST_URL, params)
    hourly = raw.get("hourly", {})
    times = hourly.get("time", [])
    out = {"time": times, "models": {}, "rejected": {}, "missing": {}}
    for m in MODELS:
        out["models"][m] = {}; out["rejected"][m] = {}; out["missing"][m] = []
        for var in VARS_CORE + VARS_STORM:
            key = f"{var}_{m}"
            if key not in hourly:          # single-model fallback naming
                key = var if len(MODELS) == 1 else None
            col = hourly.get(key) if key else None
            if col is None or all(v is None for v in col):
                out["missing"][m].append(var)
                out["models"][m][var] = [None] * len(times)
                continue
            vals, rej = [], 0
            for v in col:
                pv = plausible(var, v)
                if pv is None and v is not None:
                    rej += 1
                vals.append(pv)
            out["models"][m][var] = vals
            if rej:
                out["rejected"][m][var] = rej
    return out


def fetch_ensemble(lat: float, lon: float, forecast_days: int = 7) -> Optional[Dict]:
    """
    ECMWF IFS ensemble (51 members, 0.25°). Returns per-hour member arrays:
      {'time': [...], 'members': {var: [[m0..mN] per hour]}}
    None on failure: ensemble is an enrichment, never a dependency.
    """
    params = {
        "latitude": lat, "longitude": lon, "forecast_days": forecast_days,
        "timezone": "UTC", "models": MODEL_IFS, "hourly": ",".join(VARS_ENSEMBLE),
    }
    try:
        raw = _get(ENSEMBLE_URL, params, retries=2)
    except SourceError:
        return None
    hourly = raw.get("hourly", {})
    times = hourly.get("time", [])
    members: Dict[str, List[List[float]]] = {}
    for var in VARS_ENSEMBLE:
        cols = [k for k in hourly if k == var or k.startswith(var + "_member")]
        if not cols:
            continue
        per_hour = []
        for i in range(len(times)):
            vals = [plausible(var, hourly[c][i]) for c in cols if i < len(hourly[c])]
            per_hour.append([v for v in vals if v is not None])
        members[var] = per_hour
    if not members:
        return None
    return {"time": times, "members": members, "n_members": max(len(h) for h in next(iter(members.values())))}


def models_identical(models: Dict, var: str = "temperature_2m", n: int = 24) -> bool:
    a = models.get(MODEL_IFS, {}).get(var, [])
    b = models.get(MODEL_ICON, {}).get(var, [])
    pairs = [(x, y) for x, y in zip(a[:n], b[:n]) if x is not None and y is not None]
    return bool(pairs) and all(x == y for x, y in pairs)
