"""
MQ ATMOS LAB — BELLATOR V21.2
MULTI-SOURCE CONSENSUS ENGINE + HEAT STRESS + GUSTS

CHANGELOG V21.2 vs V21.1 (auditoría Albus, 20 OCT 2026):
✅ FIX A1: Primary model is now ECMWF IFS 0.25° (models=ecmwf_ifs025). best_match was
          resolving to ICON in Portugal → "two models" were one. Identity check at
          startup: if IFS and ICON temperature arrays are identical, consensus is
          flagged MODELS_IDENTICAL and divergence is reported as INVALID, never AGREE.
✅ FIX A2: Irradiance priority: model shortwave_radiation > NASA POWER (only if > 0)
          > weathercode estimate. V21.1 let a NASA fill value (0) overwrite valid data.
✅ FIX A3: worst_status is now the MAX by severity rank, not the last sector processed.
✅ FIX B4: Removed the extra −1/−2 °C at altitude. Open-Meteo already applies lapse-rate
          to the point elevation; V21.1 double-corrected. Wind tiering kept.
✅ FIX B5: Valley +1.5 °C only between 11h and 18h local (inversion outside that window).
✅ FIX B6/B7: HeatStress: removed dead pressure/vapour functions and the false
          "altitude-corrected pressure" claim. Stull gives psychrometric Tw; a +1.0 °C
          sun offset approximates natural Tnwb when irradiance > 400 W/m². Documented
          as approximation, not Liljegren.
✅ FIX B8: EEI solar term uses GHI directly (GHI already contains sin(h)); elevation
          only gates night (h ≤ 0 → 0).
✅ FIX B9: windgusts_10m requested from both models; gust in cards, banner, JSON, and
          wind divergence (WIND_T1/T2) now active.
✅ FIX C10: Single clock. All timestamps UTC-aware; Open-Meteo called with timezone=UTC;
          NASA POWER with time-standard=UTC; local time via zoneinfo Europe/Lisbon
          (no hard-coded June offset).
✅ FIX D11: IPMA point forecast hardened: schema tolerant (tMed | tMin/tMax), endpoint
          failure logged, never raises.
✅ FIX D12: IPMA temperature lapse-corrected to sector altitude when station altitude
          is known (IPMA_STATION_ALT table); otherwise IPMA weight on temp = 0.
✅ FIX D13: IPMA warning windows parsed as Europe/Lisbon, compared in UTC.
✅ FIX E14: summary.sectors_failed[] — a sector that raises is listed, not silently dropped.
✅ FIX E15: Hail (code 96/99) escalates to EXTREME only with dual-model confirmation;
          single model → "HAIL POSSIBLE" (HIGH).
✅ FIX E16: FTP_TLS first, plain FTP fallback with explicit warning.
✅ FIX E17: Version strings unified; datetime.utcnow() removed (deprecated 3.12).
✅ NEW:   Open-Meteo fetch retries dropping variables the model does not serve (HTTP 400),
          so IFS (no lifted_index/CIN/lightning) and ICON share one fetcher.
✅ Storm parameters (LI, CIN, lightning_potential) come from ICON; CAPE from both.

Language: English / Portuguese. No Spanish.
"""

import gpxpy, gpxpy.gpx
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import requests
import datetime, os, ftplib, folium, math, json, time
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo

VERSION   = "V21.2"
TZ_LOCAL  = ZoneInfo("Europe/Lisbon")
UTC       = datetime.timezone.utc

print(f"📡 MQ ATMOS BELLATOR {VERSION} — MULTI-SOURCE CONSENSUS")

# ─────────────────────────────────────────────────────────────────────────────
# CREDENTIALS
# ─────────────────────────────────────────────────────────────────────────────
NASA_EARTHDATA_TOKEN = os.environ.get("NASA_EARTHDATA_TOKEN", "")
AEMET_API_KEY        = os.environ.get("AEMET_API_KEY", "")

# ─────────────────────────────────────────────────────────────────────────────
# CLOCK — FIX C10: one UTC-aware clock, local derived via zoneinfo
# ─────────────────────────────────────────────────────────────────────────────
def now_utc():
    return datetime.datetime.now(UTC)

def to_local(dt_utc):
    return dt_utc.astimezone(TZ_LOCAL)

# ─────────────────────────────────────────────────────────────────────────────
# AEMET CALIBRATION FRESHNESS
# ─────────────────────────────────────────────────────────────────────────────
def check_aemet_freshness():
    try:
        with open('aemet_corrections.json') as f:
            data = json.load(f)
        for _, sd in data.items():
            if 'last_update' in sd:
                lu = datetime.datetime.fromisoformat(sd['last_update'])
                if lu.tzinfo is None: lu = lu.replace(tzinfo=UTC)
                age = now_utc() - lu
                h   = age.total_seconds() / 3600
                if age > datetime.timedelta(hours=48):
                    return {}, f"⚠️  AEMET corrections >48h ({h:.0f}h) — DISABLED"
                elif age > datetime.timedelta(hours=24):
                    return data, f"⚠️  AEMET corrections >24h ({h:.0f}h) — degraded"
                else:
                    return data, f"✅ AEMET corrections OK ({h:.0f}h)"
        return data, "⚠️  AEMET corrections: no timestamp"
    except FileNotFoundError:
        return {}, "⚠️  aemet_corrections.json not found"
    except Exception as e:
        return {}, f"⚠️  AEMET error: {str(e)[:50]}"

aemet_corrections, aemet_status = check_aemet_freshness()
print(aemet_status)

# ─────────────────────────────────────────────────────────────────────────────
# STATUS SEVERITY RANK — FIX A3
# ─────────────────────────────────────────────────────────────────────────────
def status_rank(status):
    """Higher = worse. Pattern match on the label text; order matters."""
    s = (status or "").upper()
    if any(k in s for k in ["HAIL — DO NOT RIDE", "EXTREME", "IPMA RED", "CRITICAL"]):     return 90
    if any(k in s for k in ["SEVERE", "HAIL WARNING", "HEAVY SNOW", "DANGER", "IMMINENT"]): return 80
    if any(k in s for k in ["THUNDERSTORM", "LIGHTNING RISK — HIGH", "HAIL POSSIBLE"]):     return 70
    if any(k in s for k in ["LIGHTNING RISK", "STORM PROBABLE", "SNOWFALL", "SNOW LIKELY",
                            "HEAT WARNING", "STORM WARNING", "WARNING"]):                   return 60
    if any(k in s for k in ["STORM POSSIBLE", "LIGHTNING POSSIBLE", "RAIN/SNOW", "LIGHT SNOW",
                            "HEAT CAUTION", "CAUTION", "INSTABILITY"]):                     return 40
    if "GOOD" in s or "STABLE" in s or s == "":                                             return 0
    return 50   # unknown label: treat as moderate so it surfaces

# ─────────────────────────────────────────────────────────────────────────────
# TIER 0-A: IPMA WARNINGS
# ─────────────────────────────────────────────────────────────────────────────
SECTOR_IPMA_AREAS = {
    "AMARANTE":         ["PTO", "AVR", "BRG"],
    "S. DA ABOBOREIRA": ["BRG", "PTO"],
    "SERRA DO MARÃO":   ["VRL", "BRG", "AVR"],
    "GAVIÃO":           ["VRL"],
    "SERRA DO ALVÃO":   ["VRL"],
    "SRA. GRAÇA":       ["VRL", "BRG"],
}

STORM_AWARENESS_TYPES = {"Trovoada", "Precipitação", "Neve", "Vento"}
IPMA_LEVEL_RANK       = {"red": 4, "orange": 3, "yellow": 2, "green": 0}
_ipma_warnings_cache  = None

def _parse_ipma_time(s):
    """FIX D13: IPMA times are Portugal local (naive). Return UTC-aware."""
    dt = datetime.datetime.fromisoformat(s)
    if dt.tzinfo is None: dt = dt.replace(tzinfo=TZ_LOCAL)
    return dt.astimezone(UTC)

def fetch_ipma_warnings():
    global _ipma_warnings_cache
    if _ipma_warnings_cache is not None:
        return _ipma_warnings_cache
    try:
        with urllib.request.urlopen(
            "https://api.ipma.pt/open-data/forecast/warnings/warnings_www.json",
            timeout=8) as r:
            _ipma_warnings_cache = json.loads(r.read())
            print(f"✅ IPMA warnings: {len(_ipma_warnings_cache)} entries")
            return _ipma_warnings_cache
    except Exception as e:
        print(f"⚠️  IPMA warnings failed: {e}")
        _ipma_warnings_cache = []
        return []

def check_ipma_for_sector(sector_name):
    areas    = SECTOR_IPMA_AREAS.get(sector_name, [])
    warnings = fetch_ipma_warnings()
    now      = now_utc()
    active   = []; max_rank = 0; max_level = None
    for w in warnings:
        if w.get('idAreaAviso') not in areas: continue
        if w.get('awarenessTypeName') not in STORM_AWARENESS_TYPES: continue
        level = w.get('awarenessLevelID', 'green')
        rank  = IPMA_LEVEL_RANK.get(level, 0)
        if rank < 2: continue
        try:
            start = _parse_ipma_time(w['startTime'])
            end   = _parse_ipma_time(w['endTime'])
        except Exception: continue
        if start <= now <= end:
            active.append({'level': level, 'rank': rank,
                'type': w.get('awarenessTypeName',''), 'text': w.get('text','')})
            if rank > max_rank: max_rank = rank; max_level = level
    if not active: return False, None, []
    texts = [f"[{w['level'].upper()}] {w['type']}: {w['text'][:60]}" for w in active]
    return True, max_level, texts

def ipma_to_storm_status(max_level, texts):
    combined    = " ".join(texts).lower()
    has_hail    = any(k in combined for k in ['granizo','hail'])
    has_thunder = any(k in combined for k in ['trovoada','thunder','elétric'])
    if max_level == 'red':
        if has_hail: return "HAIL — DO NOT RIDE", "#4a0000", "EXTREME"
        return "EXTREME STORM — IPMA RED", "#4a0000", "EXTREME"
    if max_level == 'orange':
        if has_hail:    return "HAIL WARNING — IPMA",  "#8b0000", "SEVERE"
        if has_thunder: return "SEVERE STORM — IPMA",  "#8b0000", "SEVERE"
        return "STORM WARNING — IPMA ORANGE",            "#8b0000", "SEVERE"
    if max_level == 'yellow':
        if has_hail:    return "HAIL POSSIBLE — IPMA", "#c0392b", "HIGH"
        if has_thunder: return "THUNDERSTORM — IPMA",  "#c0392b", "HIGH"
        return "STORM WARNING — IPMA YELLOW",            "#e67e22", "MODERATE"
    return None, None, "NONE"

# ─────────────────────────────────────────────────────────────────────────────
# TIER 0-B: AEMET STORM ALERTS
# ─────────────────────────────────────────────────────────────────────────────
def check_aemet_storm_alerts():
    if not AEMET_API_KEY:
        return False, "AEMET key not configured", None
    try:
        r = requests.get(
            "https://opendata.aemet.es/opendata/api/avisos_cap/ultimoelaborado/area/61",
            params={"api_key": AEMET_API_KEY}, timeout=8)
        if r.status_code != 200: return False, f"AEMET HTTP {r.status_code}", None
        meta = r.json(); data_url = meta.get("datos")
        if not data_url: return False, "AEMET no data URL", None
        r2 = requests.get(data_url, timeout=10)
        if r2.status_code != 200: return False, f"AEMET CAP HTTP {r2.status_code}", None
        root     = ET.fromstring(r2.content)
        ns       = {'cap': 'urn:oasis:names:tc:emergency:cap:1.2'}
        events   = []; max_sev = None
        sev_rank = {'Extreme': 4, 'Severe': 3, 'Moderate': 2, 'Minor': 1}
        for alert in root.findall('.//cap:alert', ns):
            for info in alert.findall('cap:info', ns):
                event = info.findtext('cap:event', '', ns)
                sev   = info.findtext('cap:severity', '', ns)
                if any(k in event.lower() for k in
                       ['storm','thunder','tormen','trovo','granizo','hail','electric','lightning']):
                    events.append(f"{event} ({sev})")
                    if sev in sev_rank:
                        if max_sev is None or sev_rank[sev] > sev_rank[max_sev]:
                            max_sev = sev
        if events: return True, f"AEMET: {', '.join(events[:3])}", max_sev
        return False, "AEMET: no active storm warnings", None
    except Exception as e:
        return False, f"AEMET alerts error: {str(e)[:60]}", None

# ─────────────────────────────────────────────────────────────────────────────
# EEI v3.2 — FIX B8: GHI used directly (no second sin(h)); altitude-scaled ALPHA kept
# ─────────────────────────────────────────────────────────────────────────────
class EEI_v32:
    MU=0.6; V_RIDER=16; V_EFF_MIN=4.8; T_THRESHOLD=20
    LAMBDA_BASE=0.3; LAMBDA_HR=0.4; R_THRESHOLD=0.5; ALPHA=0.007

    @staticmethod
    def calcular_elevacion_solar(lat, lon, timestamp):
        """timestamp must be UTC-aware."""
        ts = timestamp.astimezone(UTC)
        jd = (ts.timestamp()/86400.0)+2440587.5
        jc = (jd-2451545.0)/36525.0
        l0 = (280.46646+jc*(36000.76983+jc*0.0003032))%360
        m  = 357.52911+jc*(35999.05029-0.0001537*jc)
        mr = math.radians(m)
        c  = (math.sin(mr)*(1.914602-jc*(0.004817+0.000014*jc))+
              math.sin(2*mr)*(0.019993-0.000101*jc)+math.sin(3*mr)*0.000289)
        tl = l0+c; om=125.04-1934.136*jc
        al = tl-0.00569-0.00478*math.sin(math.radians(om))
        e0 = 23.0+(26.0+((21.448-jc*(46.8150+jc*(0.00059-jc*0.001813)))/60.0))/60.0
        e  = e0+0.00256*math.cos(math.radians(om))
        dec= math.degrees(math.asin(math.sin(math.radians(e))*math.sin(math.radians(al))))
        y  = math.tan(math.radians(e/2))**2
        et = 4*math.degrees(y*math.sin(2*math.radians(l0))-2*0.016708634*math.sin(mr)+
             4*0.016708634*y*math.sin(mr)*math.cos(2*math.radians(l0))-
             0.5*y*y*math.sin(4*math.radians(l0))-1.25*0.016708634**2*math.sin(2*mr))
        tst= ts.hour*60+ts.minute+ts.second/60+et+4*lon
        ha = (tst/4)-180
        se = (math.sin(math.radians(lat))*math.sin(math.radians(dec))+
              math.cos(math.radians(lat))*math.cos(math.radians(dec))*math.cos(math.radians(ha)))
        return math.degrees(math.asin(max(-1.0, min(1.0, se))))

    @staticmethod
    def calcular(T_a, v_meteo, HR, R_rate, I_sol, lat, lon, timestamp, altitude_m=0):
        v_eff = (v_meteo*EEI_v32.MU)+EEI_v32.V_RIDER
        T_wc  = T_a if v_eff<EEI_v32.V_EFF_MIN else (
            13.12+0.6215*T_a-11.37*v_eff**0.16+0.3965*T_a*v_eff**0.16)
        P_wet = 0.0 if R_rate<=EEI_v32.R_THRESHOLD else max(
            0,(EEI_v32.T_THRESHOLD-T_a)*(EEI_v32.LAMBDA_BASE+EEI_v32.LAMBDA_HR*HR/100))
        h_sol = EEI_v32.calcular_elevacion_solar(lat, lon, timestamp)
        alpha_adj = EEI_v32.ALPHA * (1.0 + altitude_m * 0.0001)
        G_sol = 0.0 if h_sol<=0 else (I_sol or 0.0)*alpha_adj       # FIX B8
        EEI   = T_wc-P_wet+G_sol
        if EEI>15:   estado={'nivel':'GOOD CONDITIONS','color':'#2ecc71'}
        elif EEI>10: estado={'nivel':'CAUTION',         'color':'#f1c40f'}
        elif EEI>5:  estado={'nivel':'WARNING',          'color':'#e67e22'}
        elif EEI>0:  estado={'nivel':'DANGER',           'color':'#e74c3c'}
        else:        estado={'nivel':'CRITICAL',         'color':'#8b0000'}
        return round(EEI,1), {
            'T_wc':round(T_wc,1),'P_wet':round(P_wet,1),
            'G_sol':round(G_sol,1),'h_sol':round(h_sol,1),
            'v_eff':round(v_eff,1),'alpha_adj':round(alpha_adj,5)
        }, estado

# ─────────────────────────────────────────────────────────────────────────────
# TIER 0: UNIFIED STORM EVALUATOR — FIX E15: hail requires dual confirmation
# ─────────────────────────────────────────────────────────────────────────────
def evaluate_storm(sector_name, weathercode, cape, lifted_index, cin,
                   lightning_potential, aemet_active=False, aemet_severity=None,
                   storm_confirmed=False, hail_confirmed=False):
    cape = cape or 0; li = lifted_index or 0; cin = cin or 0; lp = lightning_potential or 0

    ipma_active, ipma_level, ipma_texts = check_ipma_for_sector(sector_name)
    if ipma_active:
        s, c, lvl = ipma_to_storm_status(ipma_level, ipma_texts)
        if s: return s, c, lvl

    if aemet_active:
        if aemet_severity == 'Extreme': return "EXTREME STORM — AEMET", "#4a0000", "EXTREME"
        return "SEVERE STORM — AEMET", "#8b0000", "SEVERE"

    if weathercode in [96, 99]:
        if hail_confirmed: return "HAIL — DO NOT RIDE", "#4a0000", "EXTREME"
        return "HAIL POSSIBLE", "#c0392b", "HIGH"                     # single model

    if 95 <= weathercode <= 99:
        if storm_confirmed:
            if cape > 2500 or lp > 0.5: return "SEVERE THUNDERSTORM", "#8b0000", "SEVERE"
            return "THUNDERSTORM", "#c0392b", "HIGH"
        return "STORM POSSIBLE", "#e67e22", "MODERATE"

    if lp > 0.5: return "LIGHTNING RISK — HIGH",  "#8b0000", "SEVERE"
    if lp > 0.3: return "LIGHTNING RISK",          "#e74c3c", "MODERATE"
    if lp > 0.1: return "LIGHTNING POSSIBLE",      "#e67e22", "LOW"

    cin_suppressed = cin < -200
    if not cin_suppressed:
        if cape > 2500 and li < -4: return "STORM IMMINENT",  "#8b0000", "SEVERE"
        if cape > 1500 and li < -2: return "STORM PROBABLE",  "#e74c3c", "MODERATE"
    if cape > 800: return "ATMOSPHERIC INSTABILITY", "#e67e22", "LOW"

    return None, None, "NONE"

# ─────────────────────────────────────────────────────────────────────────────
# TIER 0-H: HEAT STRESS — FIX B6/B7/C10
# WBGT = 0.7·Tnwb + 0.2·Tg + 0.1·Ta (Yaglou & Minard 1957).
# Tnwb: Stull (2011) psychrometric wet-bulb + sun offset (+1.0 °C when GHI > 400
# W/m²) as an approximation of the natural wet-bulb. This is NOT a Liljegren
# energy-balance solution; error is on the conservative side (under-estimates
# WBGT by ~1 °C in full sun with low wind). Dry-bulb trigger C remains the safety net.
# ─────────────────────────────────────────────────────────────────────────────
class HeatStress:
    WBGT_CAUTION = 23.0
    WBGT_WARNING = 28.0
    WBGT_DANGER  = 30.0
    DRY_CAUTION  = 30.0
    DRY_WARNING  = 35.0
    DRY_DANGER   = 40.0
    WINDOW_START_LOCAL = 10
    WINDOW_END_LOCAL   = 18
    SUN_OFFSET_C       = 1.0
    SUN_OFFSET_GHI     = 400.0

    PALETTE = {
        "HEAT CAUTION": "#f39c12",
        "HEAT WARNING": "#e74c3c",
        "HEAT DANGER":  "#7d0a0a",
    }

    @staticmethod
    def _wet_bulb_stull(T_a, HR):
        rh = max(5.0, min(99.0, HR or 50.0))
        return (T_a * math.atan(0.151977 * math.sqrt(rh + 8.313659))
                + math.atan(T_a + rh) - math.atan(rh - 1.676331)
                + 0.00391838 * rh ** 1.5 * math.atan(0.023101 * rh)
                - 4.686035)

    @staticmethod
    def _natural_wet_bulb(T_a, HR, I_sol):
        Tw = HeatStress._wet_bulb_stull(T_a, HR)
        if (I_sol or 0) > HeatStress.SUN_OFFSET_GHI:
            Tw += HeatStress.SUN_OFFSET_C
        return Tw

    @staticmethod
    def _globe_temp(T_a, I_sol, v_ms):
        v = max(0.5, v_ms)
        dT_g = ((I_sol or 0) * 0.025) / (1.0 + 1.6 * v ** 0.85)
        return T_a + dT_g

    @staticmethod
    def wbgt(T_a, HR, I_sol, v_kmh):
        v_ms = max(0.0, v_kmh or 0) / 3.6
        Tnwb = HeatStress._natural_wet_bulb(T_a, HR, I_sol)
        Tg   = HeatStress._globe_temp(T_a, I_sol, v_ms)
        return 0.7 * Tnwb + 0.2 * Tg + 0.1 * T_a

    @staticmethod
    def _level(wbgt_v, T_a):
        def rank(s): return {"NONE":0,"HEAT CAUTION":1,"HEAT WARNING":2,"HEAT DANGER":3}[s]
        b = ("HEAT DANGER"  if wbgt_v >= HeatStress.WBGT_DANGER  else
             "HEAT WARNING" if wbgt_v >= HeatStress.WBGT_WARNING else
             "HEAT CAUTION" if wbgt_v >= HeatStress.WBGT_CAUTION else "NONE")
        c = ("HEAT DANGER"  if T_a >= HeatStress.DRY_DANGER  else
             "HEAT WARNING" if T_a >= HeatStress.DRY_WARNING else
             "HEAT CAUTION" if T_a >= HeatStress.DRY_CAUTION else "NONE")
        return b if rank(b) >= rank(c) else c

    @staticmethod
    def in_window(ts_utc):
        h = to_local(ts_utc).hour
        return HeatStress.WINDOW_START_LOCAL <= h <= HeatStress.WINDOW_END_LOCAL

    @staticmethod
    def evaluate(T_a, HR, I_sol, v_kmh, ts_utc):
        """Returns (status, color, heat_level, wbgt_value). wbgt always returned."""
        wbgt_v = round(HeatStress.wbgt(T_a, HR, I_sol, v_kmh), 1)
        if not HeatStress.in_window(ts_utc):
            return None, None, "NONE", wbgt_v
        level = HeatStress._level(wbgt_v, T_a)
        if level == "NONE":
            return None, None, "NONE", wbgt_v
        return level, HeatStress.PALETTE[level], level, wbgt_v

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
OUTPUT_FOLDER = 'output/'
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

track_points = []
try:
    if os.path.exists('MQ_TRACK.gpx'):
        with open('MQ_TRACK.gpx','r') as g:
            gpx = gpxpy.parse(g)
            for t in gpx.tracks:
                for s in t.segments:
                    for p in s.points:
                        track_points.append([p.latitude, p.longitude])
except Exception: pass
if not track_points: track_points = [[41.27,-8.08],[41.27,-8.08]]

sectors = [
    {"id":1,"name":"AMARANTE",         "lat":41.2709,"lon":-8.0797,"alt":"65m",  "altitude_m":65,  "type":"FLAT",   "desc":"START / BASE"},
    {"id":2,"name":"S. DA ABOBOREIRA","lat":41.1946,"lon":-8.0563,"alt":"760m", "altitude_m":760, "type":"CLIMB",  "desc":"ENTRY POINT"},
    {"id":3,"name":"SERRA DO MARÃO",  "lat":41.2484,"lon":-7.8862,"alt":"1390m","altitude_m":1390,"type":"DESCEND","desc":"HIGH MOUNTAIN"},
    {"id":4,"name":"GAVIÃO",          "lat":41.2777,"lon":-7.9462,"alt":"986m", "altitude_m":986, "type":"CLIMB",  "desc":"THE FILTER"},
    {"id":5,"name":"SERRA DO ALVÃO",  "lat":41.3738,"lon":-7.8053,"alt":"1043m","altitude_m":1043,"type":"FLAT",   "desc":"PLATEAU"},
    {"id":6,"name":"SRA. GRAÇA",      "lat":41.4168,"lon":-7.9106,"alt":"950m", "altitude_m":950, "type":"CLIMB",  "desc":"THE CLIMB"},
]

# ─────────────────────────────────────────────────────────────────────────────
# IRRADIANCE FALLBACK (third priority)
# ─────────────────────────────────────────────────────────────────────────────
_IRRADIANCE_BY_CODE = {
    0:850, 1:650, 2:450, 3:250,
    45:150, 48:100,
    51:200, 53:150, 55:100,
    61:150, 63:100, 65:50,
    71:100, 73:80,  75:50,
    77:50,  80:200, 81:150, 82:100,
    85:80,  86:60,
    95:50,  96:30,  99:30,
}

def irradiance_fallback(code, hour_local):
    """Estimate GHI from weathercode. Bell curve peaking at 13h local."""
    if hour_local < 6 or hour_local > 20: return 0.0
    base   = _IRRADIANCE_BY_CODE.get(code, 300)
    factor = max(0.0, 1.0 - abs(hour_local - 13) / 7.0)
    return round(base * factor, 1)

# ─────────────────────────────────────────────────────────────────────────────
# OPEN-METEO FETCHER — FIX A1 + B9 + C10 + variable-drop retry
# ─────────────────────────────────────────────────────────────────────────────
_OM_BASE   = "https://api.open-meteo.com/v1/forecast"
_OM_CORE   = ['temperature_2m','windspeed_10m','windgusts_10m','weathercode',
              'precipitation','relativehumidity_2m','shortwave_radiation',
              'snowfall','freezing_level_height','cape']
_OM_STORM  = ['lifted_index','convective_inhibition','lightning_potential']

MODEL_IFS  = 'ecmwf_ifs025'     # FIX A1: explicit ECMWF, not best_match
MODEL_ICON = 'icon_seamless'    # DWD ICON-EU 7 km over Portugal

def fetch_open_meteo(lat, lon, model, hourly_vars, max_retries=4):
    """
    Fetch with timezone=UTC. If the model does not serve a variable, Open-Meteo
    answers 400 with the variable name in 'reason'; drop it and retry.
    Returns (json, dropped_vars).
    """
    vars_ = list(hourly_vars); dropped = []
    for _ in range(max_retries):
        r = requests.get(_OM_BASE, params={
            'latitude': lat, 'longitude': lon, 'forecast_days': 2,
            'timezone': 'UTC', 'models': model, 'hourly': ','.join(vars_),
        }, timeout=12)
        if r.status_code == 200:
            return r.json(), dropped
        if r.status_code == 400:
            reason = r.json().get('reason', '') if r.headers.get('content-type','').startswith('application/json') else r.text
            hit = [v for v in vars_ if v in reason]
            if hit:
                for v in hit: vars_.remove(v); dropped.append(v)
                continue
        r.raise_for_status()
    raise RuntimeError(f"Open-Meteo {model}: could not resolve variable set")

def fetch_ifs(lat, lon):
    return fetch_open_meteo(lat, lon, MODEL_IFS, _OM_CORE)

def fetch_icon(lat, lon):
    return fetch_open_meteo(lat, lon, MODEL_ICON, _OM_CORE + _OM_STORM)

def models_identical(ifs_raw, icon_raw):
    """FIX A1: guard against two labels resolving to one model."""
    a = ifs_raw.get('hourly',{}).get('temperature_2m')
    b = icon_raw.get('hourly',{}).get('temperature_2m')
    if not a or not b: return False
    n = min(len(a), len(b), 24)
    return all(a[i] == b[i] for i in range(n) if a[i] is not None and b[i] is not None)

# ─────────────────────────────────────────────────────────────────────────────
# IPMA POINT FORECAST — FIX D11 / D12
# ─────────────────────────────────────────────────────────────────────────────
_ipma_locations_cache = None

# Station altitude (m) for lapse correction. Extend as verified. Unknown → no temp weight.
IPMA_STATION_ALT = {
    1171400: 460,   # Vila Real
    1130800: 400,   # Amarante (verify)
    1030300: 190,   # Braga (verify)
}
LAPSE_C_PER_M = 0.0065

def fetch_ipma_locations():
    global _ipma_locations_cache
    if _ipma_locations_cache is not None: return _ipma_locations_cache
    try:
        with urllib.request.urlopen(
            "https://api.ipma.pt/open-data/forecast/locations.json", timeout=8) as r:
            _ipma_locations_cache = json.loads(r.read())
            print(f"✅ IPMA locations: {len(_ipma_locations_cache)} stations")
            return _ipma_locations_cache
    except Exception as e:
        print(f"⚠️  IPMA locations failed: {e}")
        _ipma_locations_cache = []
        return []

def nearest_ipma_location(lat, lon):
    locs = fetch_ipma_locations()
    if not locs: return None
    return min(locs, key=lambda l:
        (float(l['latitude']) - lat)**2 + (float(l['longitude']) - lon)**2)

_WTYPE_TO_CODE = {
    1:0, 2:1, 3:2, 4:3, 5:45, 6:51, 7:61, 8:63,
    9:80, 10:80, 11:82, 12:95, 13:96, 14:71, 15:77,
}
_WIND_CLASS_KPH = {1:7, 2:20, 3:31, 4:45, 5:60}

def fetch_ipma_point_forecast(sector):
    """
    Nearest IPMA forecast. Tries hourly endpoint, falls back to daily.
    Temp lapse-corrected to sector altitude when station altitude is known;
    otherwise temp=None (excluded from consensus) and wind is still used.
    """
    loc = nearest_ipma_location(sector['lat'], sector['lon'])
    if not loc: return None
    gid = int(loc['globalIdLocal'])
    entries = []; schema = None
    for kind in ('hourly', 'daily'):
        try:
            url = f"https://api.ipma.pt/open-data/forecast/meteorology/cities/{kind}/{gid}.json"
            with urllib.request.urlopen(url, timeout=8) as r:
                data = json.loads(r.read())
            entries = data.get('data', [])
            if entries: schema = kind; break
        except Exception as e:
            print(f"⚠️  IPMA {kind} {sector['name']}: {str(e)[:50]}")
    if not entries: return None
    try:
        now_l = to_local(now_utc()).replace(tzinfo=None)
        def _dist(e):
            k = e.get('forecastDate') or e.get('dataPrev')
            try: return abs((datetime.datetime.fromisoformat(k) - now_l).total_seconds())
            except Exception: return float('inf')
        best   = min(entries, key=_dist)
        wtype  = int(best.get('idWeatherType', 1))
        wclass = int(best.get('classWindSpeed', 1))
        tmed   = best.get('tMed')
        if tmed is not None:
            temp = float(tmed)
        else:
            tmin = best.get('tMin'); tmax = best.get('tMax')
            temp = (float(tmin) + float(tmax)) / 2.0 if (tmin is not None and tmax is not None) else None
        st_alt = IPMA_STATION_ALT.get(gid)
        if temp is not None and st_alt is not None:
            temp = temp - (sector['altitude_m'] - st_alt) * LAPSE_C_PER_M   # FIX D12
        elif temp is not None:
            temp = None   # unknown station altitude → do not anchor temperature
        return {
            'temp':   temp,
            'wind':   _WIND_CLASS_KPH.get(wclass, 20),
            'code':   _WTYPE_TO_CODE.get(wtype, 0),
            'schema': schema,
            'source': f"IPMA/{loc['local']} ({gid}, {schema})",
        }
    except Exception as e:
        print(f"⚠️  IPMA parse {sector['name']}: {str(e)[:50]}")
        return None

def fetch_all_ipma_forecasts(sectors):
    result = {}
    def _one(sec): return sec['id'], fetch_ipma_point_forecast(sec)
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(_one, s): s for s in sectors}
        for fut in as_completed(futs):
            try:
                sid, d = fut.result(timeout=12)
                if d: result[sid] = d
            except Exception: pass
    return result

# ─────────────────────────────────────────────────────────────────────────────
# CONSENSUS ENGINE — FIX A1 (identity flag), B9 (wind/gust divergence)
# ─────────────────────────────────────────────────────────────────────────────
class ConsensusEngine:
    W_IFS  = 0.55
    W_ICON = 0.35
    W_IPMA = 0.10

    TEMP_T1   = 1.5;  TEMP_T2   = 3.0
    PRECIP_T1 = 2.0;  PRECIP_T2 = 5.0
    WIND_T1   = 10.0; WIND_T2   = 20.0
    GUST_T1   = 15.0; GUST_T2   = 30.0

    @staticmethod
    def _wavg(pairs):
        valid = [(v, w) for v, w in pairs if v is not None]
        if not valid: return None
        tw = sum(w for _, w in valid)
        return sum(v*w for v, w in valid) / tw if tw > 0 else None

    @staticmethod
    def _r(v, nd): return None if v is None else round(v, nd)

    @staticmethod
    def merge(ifs, icon, ipma=None, identical=False):
        wa = ConsensusEngine._wavg; r = ConsensusEngine._r
        wi = ConsensusEngine.W_IFS; wc = ConsensusEngine.W_ICON
        wp = ConsensusEngine.W_IPMA if ipma else 0.0
        if identical: wc = 0.0   # do not double-count one model

        consensus = {
            'temp': r(wa([(ifs.get('temp'), wi), (icon.get('temp'), wc),
                          (ipma['temp'] if ipma else None, wp)]), 1),
            'wind': r(wa([(ifs.get('wind'), wi), (icon.get('wind'), wc),
                          (ipma['wind'] if ipma else None, wp)]), 1),
            'gust': r(wa([(ifs.get('gust'), wi), (icon.get('gust'), wc)]), 1),
            'rain': r(wa([(ifs.get('rain'), wi + wp/2), (icon.get('rain'), wc + wp/2)]), 2),
            'hum':  r(wa([(ifs.get('hum'),  wi + wp/2), (icon.get('hum'),  wc + wp/2)]), 0),
            'irradiance': r(wa([(ifs.get('irradiance'), wi + wp), (icon.get('irradiance'), wc)]), 1),
            'snowfall':       r(wa([(ifs.get('snowfall'), 0.5), (icon.get('snowfall'), 0.5)]), 2),
            'freezing_level': r(wa([(ifs.get('freezing_level'), 0.5), (icon.get('freezing_level'), 0.5)]), 0),
            'cape':                wa([(ifs.get('cape'), wi), (icon.get('cape'), wc)]) or 0,
            # Storm diagnostics are ICON-only variables on Open-Meteo
            'lifted_index':        icon.get('lifted_index', 0) or 0,
            'cin':                 icon.get('cin', 0) or 0,
            'lightning_potential': icon.get('lightning_potential'),
            'code':                ifs.get('code', 0) or 0,
            'code_icon':           icon.get('code', 0) or 0,
            'irradiance_source':   ifs.get('irradiance_source', 'OPEN_METEO'),
        }
        # Defaults where every source failed
        for k, dflt in (('temp',0.0),('wind',0.0),('gust',None),('rain',0.0),('hum',50.0),
                        ('irradiance',0.0),('snowfall',0.0),('freezing_level',9999.0)):
            if consensus[k] is None: consensus[k] = dflt
        if consensus['gust'] is None: consensus['gust'] = consensus['wind']

        def _d(a, b): return abs((a or 0) - (b or 0))
        dt = _d(ifs.get('temp'), icon.get('temp'))
        dp = _d(ifs.get('rain'), icon.get('rain'))
        dv = _d(ifs.get('wind'), icon.get('wind'))
        dg = _d(ifs.get('gust'), icon.get('gust'))

        ifs_code = ifs.get('code', 0) or 0; icon_code = icon.get('code', 0) or 0
        storm_confirmed = (ifs_code >= 95 and icon_code >= 95) and not identical
        hail_confirmed  = (ifs_code in (96,99) and icon_code in (96,99)) and not identical
        clear_confirmed = (ifs_code <= 2 and icon_code <= 2) and not identical

        if identical:
            div_status = 'INVALID'
        elif dt > ConsensusEngine.TEMP_T2 or dp > ConsensusEngine.PRECIP_T2 \
             or dv > ConsensusEngine.WIND_T2 or dg > ConsensusEngine.GUST_T2:
            div_status = 'DIVERGENT'
        elif dt > ConsensusEngine.TEMP_T1 or dp > ConsensusEngine.PRECIP_T1 \
             or dv > ConsensusEngine.WIND_T1 or dg > ConsensusEngine.GUST_T1:
            div_status = 'UNCERTAIN'
        else:
            div_status = 'AGREE'

        divergence = {
            'status':          div_status,
            'models_identical': identical,
            'temp_spread':     round(dt, 1),
            'precip_spread':   round(dp, 1),
            'wind_spread':     round(dv, 1),
            'gust_spread':     round(dg, 1),
            'ifs_temp':        r(ifs.get('temp'), 1),
            'icon_temp':       r(icon.get('temp'), 1),
            'ifs_precip':      r(ifs.get('rain'), 1),
            'icon_precip':     r(icon.get('rain'), 1),
            'ifs_gust':        r(ifs.get('gust'), 0),
            'icon_gust':       r(icon.get('gust'), 0),
            'ipma_temp':       r(ipma['temp'], 1) if (ipma and ipma.get('temp') is not None) else None,
            'storm_confirmed': storm_confirmed,
            'hail_confirmed':  hail_confirmed,
            'clear_confirmed': clear_confirmed,
            'sources_active':  (1 if identical else 2) + (1 if ipma else 0),
            'ipma_station':    ipma.get('source') if ipma else None,
        }
        return consensus, divergence

# ─────────────────────────────────────────────────────────────────────────────
# NASA POWER — FIX C10 (UTC) + FIX A2 (fallback only)
# ─────────────────────────────────────────────────────────────────────────────
def get_nasa_irradiance(lat, lon, date_utc):
    date_str = date_utc.strftime('%Y%m%d')
    try:
        r = requests.get(
            "https://power.larc.nasa.gov/api/temporal/hourly/point",
            params={'parameters':'ALLSKY_SFC_SW_DWN','community':'RE',
                    'longitude':lon,'latitude':lat,
                    'start':date_str,'end':date_str,'format':'JSON',
                    'time-standard':'UTC'}, timeout=10)
        if r.status_code != 200: return None
        d = r.json()
        if 'properties' not in d: return None
        hd = d['properties']['parameter']['ALLSKY_SFC_SW_DWN']
        vals = [hd.get(f"{date_str}{h:02d}", -999) for h in range(24)]
        if all(v is None or v < 0 for v in vals): return None    # fill values only
        return [v if (v is not None and v >= 0) else None for v in vals]
    except Exception: return None

def fetch_nasa_parallel(sectors, date_utc):
    cache = {}
    def _one(sec):
        data = get_nasa_irradiance(sec['lat'], sec['lon'], date_utc)
        return (sec['id'], data) if data else None
    with ThreadPoolExecutor(max_workers=3) as ex:
        for fut in [ex.submit(_one, s) for s in sectors]:
            try:
                result = fut.result(timeout=10)
                if result: cache[result[0]] = result[1]
            except Exception: pass
    return cache

# ─────────────────────────────────────────────────────────────────────────────
# WEATHER TEXT
# ─────────────────────────────────────────────────────────────────────────────
def weather_text(code, temp, precip, snowfall):
    if snowfall>0.1 or (temp<=2 and precip>0.5):
        return "HEAVY SNOW" if precip>10 else "SNOW" if precip>3 else "LIGHT SNOW"
    if code==0: return "CLEAR"
    if 1<=code<=3: return "CLOUDY"
    if code in [45,48]: return "FOG"
    if 51<=code<=67: return "RAIN"
    if code in [71,73,75,77,85,86]: return "SNOW"
    if 80<=code<=82: return "SHOWERS"
    if code in [96,99]: return "HAIL"
    if 95<=code<=99: return "THUNDERSTORM"
    return "OVERCAST"

# ─────────────────────────────────────────────────────────────────────────────
# MICROCLIMATE — FIX B4, B5, A2
# ─────────────────────────────────────────────────────────────────────────────
def microclimate(sector, data, ts_utc, nasa_hour_val=None):
    d = data.copy(); alt = sector['altitude_m']; terrain = sector.get('type','FLAT')
    log = []; hour_local = to_local(ts_utc).hour

    if sector['name'] in aemet_corrections:
        c = aemet_corrections[sector['name']]
        if c.get('temp_offset', 0) != 0:
            d['temp'] += c['temp_offset']; log.append(f"AEMET T{c['temp_offset']:+.1f}°")
        if c.get('wind_factor', 1.0) != 1.0:
            w0 = d['wind']; d['wind'] *= c['wind_factor']; d['gust'] *= c['wind_factor']
            log.append(f"AEMET V {w0:.1f}→{d['wind']:.1f}")

    # Wind exposure tiering (temperature untouched — FIX B4)
    if alt > 1200 and terrain in ('CLIMB', 'FLAT'):
        w0 = d['wind']; d['wind'] *= 1.60; d['gust'] *= 1.40
        log.append(f"Ridge+60%V ({w0:.1f}→{d['wind']:.1f})")
    elif alt > 1000:
        w0 = d['wind']; d['wind'] *= 1.30; d['gust'] *= 1.20
        log.append(f"High-alt+30%V ({w0:.1f}→{d['wind']:.1f})")

    # Valley warming only in the mixed-layer window — FIX B5
    if 400 < alt < 800 and 11 <= hour_local <= 18:
        d['temp'] += 1.5; log.append("Valley+1.5°")

    if alt < 800 and (d.get('hum') or 0) > 85 and 8 <= (d.get('temp') or 0) <= 12:
        d['fog_alert'] = "NORTADA FOG"; log.append("Fog")

    if (d.get('rain') or 0) > 5:
        d['mtb_hazard'] = "SLIPPERY TRACK"; log.append("MTB hazard")

    # Irradiance priority — FIX A2: model > NASA(>0) > code estimate
    irr = d.get('irradiance') or 0
    if irr <= 0:
        if nasa_hour_val is not None and nasa_hour_val > 0:
            d['irradiance'] = nasa_hour_val; d['irradiance_source'] = 'NASA_POWER'
            log.append(f"Irr←NASA {nasa_hour_val:.0f}W")
        else:
            est = irradiance_fallback(d.get('code', 0), hour_local)
            if est > 0:
                d['irradiance'] = est; d['irradiance_source'] = 'CODE_ESTIMATE'
                log.append(f"Irr↑{est:.0f}W(est)")

    if log: print(f"🔧 {sector['name']:20} | {' | '.join(log)}")
    return d

# ─────────────────────────────────────────────────────────────────────────────
# RAW HOURLY EXTRACTION — index on UTC hours (arrays are UTC, FIX C10)
# ─────────────────────────────────────────────────────────────────────────────
def get_hourly_from_raw(r, h, source):
    H = r['hourly']; n = len(H.get('time', []))
    h = max(0, min(h, n - 1)) if n else 0
    def g(key, default=None):
        arr = H.get(key)
        if not arr or h >= len(arr): return default
        return arr[h]
    return {
        'temp':                g('temperature_2m'),
        'wind':                g('windspeed_10m'),
        'gust':                g('windgusts_10m'),
        'rain':                g('precipitation'),
        'hum':                 g('relativehumidity_2m'),
        'code':                g('weathercode', 0),
        'irradiance':          g('shortwave_radiation'),
        'snowfall':            g('snowfall'),
        'freezing_level':      g('freezing_level_height'),
        'cape':                g('cape'),
        'lifted_index':        g('lifted_index'),
        'cin':                 g('convective_inhibition'),
        'lightning_potential': g('lightning_potential'),
        'irradiance_source':   'OPEN_METEO',
    }

# ─────────────────────────────────────────────────────────────────────────────
# CARD GENERATOR
# ─────────────────────────────────────────────────────────────────────────────
def generate_card(sector, d_now, d_3h, d_6h, ts_utc,
                  aemet_active, aemet_severity, div=None):
    eei_now,_,estado = EEI_v32.calcular(
        d_now['temp'], d_now['wind'], d_now['hum'], d_now['rain'],
        d_now['irradiance'], sector['lat'], sector['lon'], ts_utc,
        altitude_m=sector['altitude_m'])
    eei_3h,_,_ = EEI_v32.calcular(
        d_3h['temp'], d_3h['wind'], d_3h['hum'], d_3h['rain'],
        d_3h['irradiance'], sector['lat'], sector['lon'],
        ts_utc+datetime.timedelta(hours=3), altitude_m=sector['altitude_m'])
    eei_6h,_,_ = EEI_v32.calcular(
        d_6h['temp'], d_6h['wind'], d_6h['hum'], d_6h['rain'],
        d_6h['irradiance'], sector['lat'], sector['lon'],
        ts_utc+datetime.timedelta(hours=6), altitude_m=sector['altitude_m'])

    status = estado['nivel']; color = estado['color']
    storm_level = "NONE"; is_snow = is_mixed = False; snow_int = "LIGHT"
    storm_confirmed = div.get('storm_confirmed', False) if div else False
    hail_confirmed  = div.get('hail_confirmed',  False) if div else False

    h_status, h_color, heat_level, wbgt_now = HeatStress.evaluate(
        d_now['temp'], d_now['hum'], d_now['irradiance'], d_now['wind'], ts_utc)

    s_status, s_color, storm_level = evaluate_storm(
        sector['name'], d_now['code'],
        d_now.get('cape',0), d_now.get('lifted_index',0),
        d_now.get('cin',0), d_now.get('lightning_potential'),
        aemet_active, aemet_severity, storm_confirmed, hail_confirmed)

    if s_status:
        status = s_status; color = s_color
    elif 0 < d_now['temp'] <= 3 and d_now['rain'] > 0.1:
        is_mixed = True
        status = "SNOW LIKELY" if d_now['temp'] <= 1.5 else "RAIN/SNOW MIX"
        color  = "#3498db"    if d_now['temp'] <= 1.5 else "#e67e22"
    elif d_now['rain'] > 0.1 and d_now['temp'] <= 1:
        is_snow   = True
        snow_int  = "HEAVY" if d_now['rain']>10 else "MODERATE" if d_now['rain']>3 else "LIGHT"
    elif (sector['altitude_m'] > d_now['freezing_level'] and
          d_now['freezing_level'] < 9000 and d_now['rain'] > 0.1):
        is_snow  = True
        snow_int = "HEAVY" if d_now['rain']>10 else "MODERATE" if d_now['rain']>3 else "LIGHT"
    if is_snow:
        status = {"HEAVY":"HEAVY SNOWFALL","MODERATE":"SNOWFALL","LIGHT":"LIGHT SNOW"}[snow_int]
        color  = {"HEAVY":"#e74c3c","MODERATE":"#e67e22","LIGHT":"#f1c40f"}[snow_int]
    elif h_status:
        status = h_status; color = h_color

    arrow = lambda c,f: "(-)" if f-c<-2 else "(+)" if f-c>2 else "(=)"

    fig,ax = plt.subplots(figsize=(6,3.4), facecolor='#0f172a')
    ax.set_facecolor('#0f172a')
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax.add_patch(patches.Rectangle((0,0),0.03,1,transform=ax.transAxes,linewidth=0,facecolor=color))

    plt.text(0.08,0.80,sector['name'],color='white',fontsize=16,fontweight='bold',transform=ax.transAxes)
    plt.text(0.08,0.68,f"{sector['desc']} | {sector['alt']}",color='#94a3b8',fontsize=9,fontweight='bold',transform=ax.transAxes)

    if storm_level in ["EXTREME","SEVERE"]:   wm = "HAIL" if "HAIL" in status else "STORM"
    elif storm_level in ["HIGH","MODERATE"]:  wm = "⚡"
    elif storm_level == "LOW":                wm = "UNSTABLE"
    elif is_snow or is_mixed:                 wm = "SNOW"
    else: wm = weather_text(d_now['code'],d_now['temp'],d_now['rain'],d_now['snowfall'])
    plt.text(0.5,0.40,wm,color='white',alpha=0.10,fontsize=40,fontweight='bold',ha='center',transform=ax.transAxes)

    plt.text(0.92,0.68,f"{int(d_now['temp'])}°",color='white',fontsize=38,fontweight='bold',ha='right',transform=ax.transAxes)

    mri_col = "#ffffff" if status_rank(status) >= 80 else "#38bdf8" if eei_now<d_now['temp'] else "#fca5a5"
    plt.text(0.92,0.55,f"MRI: {int(eei_now)}°",color=mri_col,fontsize=10,fontweight='bold',ha='right',transform=ax.transAxes)
    gust_col = "#e67e22" if d_now['gust'] >= 40 else "#94a3b8"
    plt.text(0.08,0.45,f"WIND {int(d_now['wind'])} · GUST {int(d_now['gust'])} km/h",
             color=gust_col,fontsize=9,transform=ax.transAxes)

    lp = (d_now.get('lightning_potential') or 0)
    if lp > 0.1:
        lp_col = "#8b0000" if lp>0.5 else "#e74c3c" if lp>0.3 else "#e67e22"
        plt.text(0.92,0.45,f"⚡ {int(lp*100)}%",color=lp_col,fontsize=8,fontweight='bold',ha='right',transform=ax.transAxes)

    if div:
        ds = div.get('status','AGREE')
        if ds == 'INVALID':
            div_lbl = "⚠ SINGLE MODEL"; div_col = '#e74c3c'
        elif ds == 'DIVERGENT':
            div_lbl = f"⚠ IFS:{div['ifs_temp']}° ICON:{div['icon_temp']}° G±{div['gust_spread']:.0f}"
            div_col = '#e67e22'
        elif ds == 'UNCERTAIN':
            div_lbl = f"? IFS:{div['ifs_temp']}° ICON:{div['icon_temp']}° G±{div['gust_spread']:.0f}"
            div_col = '#f1c40f'
        else:
            div_lbl = f"◈ {div.get('sources_active',2)}SRC AGREE"; div_col = '#475569'
        plt.text(0.08,0.35,div_lbl,color=div_col,fontsize=9,fontweight='bold',transform=ax.transAxes)

    plt.text(0.92,0.25,f" {status} ",color='white',fontsize=9,fontweight='bold',ha='right',
             bbox=dict(boxstyle="round,pad=0.4",fc=color,ec="none",alpha=0.9),transform=ax.transAxes)
    plt.plot([0.05,0.95],[0.15,0.15],color='#334155',linewidth=1,transform=ax.transAxes)

    def flabel(d, alt):
        if 95<=d['code']<=99: return "HAIL" if d['code'] in [96,99] else "THUNDER"
        if d['snowfall']>0.1 or (d['temp']<=2 and d['rain']>0.5): return "SNOW"
        if 0<d['temp']<=3 and d['rain']>0.1: return "SNOW?" if d['temp']<=1.5 else "MIXED"
        if d['temp']<=1 and d['rain']>0.1: return "SNOW"
        if alt>d['freezing_level'] and d['freezing_level']<9000 and d['rain']>0.1: return "SNOW"
        return weather_text(d['code'],d['temp'],d['rain'],d['snowfall'])

    plt.text(0.05,0.09,f"+3H: {flabel(d_3h,sector['altitude_m'])} {int(d_3h['temp'])}° {arrow(eei_now,eei_3h)}",
             color='#94a3b8',fontsize=9,fontweight='bold',ha='left',transform=ax.transAxes)
    plt.text(0.95,0.09,f"+6H: {flabel(d_6h,sector['altitude_m'])} {int(d_6h['temp'])}° {arrow(eei_now,eei_6h)}",
             color='#94a3b8',fontsize=9,fontweight='bold',ha='right',transform=ax.transAxes)

    ax.axis('off')
    plt.savefig(f"{OUTPUT_FOLDER}MQ_SECTOR_{sector['id']}_STATUS.png",dpi=150,facecolor='#0f172a')
    plt.close()

    return (status, int(eei_now), d_now['wind'], d_now['gust'], is_snow or is_mixed,
            snow_int, eei_3h, eei_6h, storm_level, heat_level, wbgt_now)

# ─────────────────────────────────────────────────────────────────────────────
# BANNER — FIX A3 colour by rank, B9 gust, E17 version
# ─────────────────────────────────────────────────────────────────────────────
def generate_banner(status, min_eei, max_wind, max_gust, worst_sector, time_str,
                    snow, storm_level, single_model):
    fig,ax = plt.subplots(figsize=(8,2.5),facecolor='#0a0a0a')
    ax.set_facecolor('#0a0a0a')
    plt.subplots_adjust(left=0,right=1,top=1,bottom=0)
    rk = status_rank(status)
    color = ("#8b0000" if rk >= 90 else "#e74c3c" if rk >= 70 else
             "#e67e22" if rk >= 60 else "#f1c40f" if rk >= 40 else "#2ecc71")
    if "HEAT DANGER" in status: color = "#7d0a0a"
    ax.add_patch(patches.Rectangle((0,0),0.015,1,transform=ax.transAxes,linewidth=0,facecolor=color))
    ax_r = fig.add_axes([0.05,0.15,0.20,0.70]); ax_r.set_facecolor('#0a0a0a')
    lats=[p[0] for p in track_points]; lons=[p[1] for p in track_points]
    ax_r.plot(lons,lats,color=color,linewidth=1.2,alpha=0.9)
    ax_r.set_aspect('equal'); ax_r.axis('off')
    ax_r.add_patch(patches.Circle((0.5,0.5),0.48,transform=ax_r.transAxes,
                                  fill=False,edgecolor='#333',linewidth=1,linestyle=':'))
    plt.text(0.28,0.70,"MQ METEO STATION",color='white',fontsize=14,fontweight='bold',transform=ax.transAxes)
    src = "IFS · ICON-EU · IPMA" if not single_model else "⚠ SINGLE MODEL · IPMA"
    if rk == 0:
        hook = "ALL SECTORS: GO"
    elif storm_level not in ["NONE", None]:
        hook = f"⚡ {status} — {worst_sector}"
    elif snow:
        hook = f"SNOW: {worst_sector}"
    else:
        hook = f"ALERT: {worst_sector}"
    sub = f"UPDATED: {time_str} UTC  |  {src}  |  BELLATOR {VERSION}"
    plt.text(0.28,0.50,hook,color=color,fontsize=10,fontweight='bold',transform=ax.transAxes)
    plt.text(0.28,0.35,sub, color='#888', fontsize=8,transform=ax.transAxes)
    plt.plot([0.68,0.68],[0.2,0.8],color='#222',linewidth=1,transform=ax.transAxes)
    plt.text(0.76,0.70,"MIN MRI",color='#666',fontsize=7,ha='center',transform=ax.transAxes)
    plt.text(0.76,0.45,f"{min_eei}°",
             color="#38bdf8" if min_eei<10 else "white",
             fontsize=20,fontweight='bold',ha='center',transform=ax.transAxes)
    plt.text(0.90,0.70,"MAX GUST",color='#666',fontsize=7,ha='center',transform=ax.transAxes)
    plt.text(0.90,0.45,f"{int(max_gust)}",
             color="#e67e22" if max_gust>40 else "white",
             fontsize=20,fontweight='bold',ha='center',transform=ax.transAxes)
    plt.text(0.90,0.32,f"km/h · wind {int(max_wind)}",color='#666',fontsize=7,ha='center',transform=ax.transAxes)
    ax.axis('off')
    plt.savefig(f"{OUTPUT_FOLDER}MQ_HOME_BANNER.png",facecolor='#0a0a0a',dpi=150)
    plt.close()

def generate_map():
    center = track_points[len(track_points)//2] if len(track_points)>10 else [41.30,-7.95]
    m = folium.Map(location=center,zoom_start=10,tiles='CartoDB dark_matter')
    folium.PolyLine(track_points,color="#00f2ff",weight=3,opacity=0.9,tooltip="MQ TRACK").add_to(m)
    for s in sectors:
        folium.CircleMarker([s['lat'],s['lon']],radius=6,color="#ff9900",
            fill=True,fill_color="#ff9900",fill_opacity=0.9,
            popup=f"<b>{s['name']}</b><br>{s['alt']}<br>{s['type']}",
            tooltip=s['name']).add_to(m)
    m.save(f"{OUTPUT_FOLDER}MQ_TACTICAL_MAP_CALIBRATED.html")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
print("\n📡 INIT...")
ts_now   = now_utc()
time_str = ts_now.strftime("%H:%M")
hour_utc = ts_now.hour          # index into Open-Meteo UTC arrays (day 0)

fetch_ipma_warnings()
fetch_ipma_locations()

print("⚡ AEMET storm alerts...")
aemet_storm_active, aemet_alert_text, aemet_storm_severity = check_aemet_storm_alerts()
print(f"   {aemet_alert_text}")

print("🌐 IPMA point forecasts...")
ipma_forecasts = fetch_all_ipma_forecasts(sectors)
print(f"   ✅ {len(ipma_forecasts)}/{len(sectors)} stations resolved")

print("☀️  NASA POWER (fallback only)...")
t0 = time.time()
nasa_cache = fetch_nasa_parallel(sectors, ts_now)
print(f"   ✅ {len(nasa_cache)}/{len(sectors)} sectors with usable data ({time.time()-t0:.1f}s)")

worst_status = "STABLE"; worst_sector = ""; worst_rank = -1
g_min_eei = 99; g_max_wind = 0; g_max_gust = 0
snow_detected = storm_detected = False; g_storm_level = "NONE"
heat_detected = False; g_heat_level = "NONE"; g_max_wbgt = -99.0
any_identical = False
def _heat_rank(s): return {"NONE":0,"HEAT CAUTION":1,"HEAT WARNING":2,"HEAT DANGER":3}.get(s,0)
json_sectors = []; divergence_log = []; sectors_failed = []; dropped_vars = {}

print("\n🌍 Processing sectors...")
for sec in sectors:
    try:
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_ifs  = ex.submit(fetch_ifs,  sec['lat'], sec['lon'])
            f_icon = ex.submit(fetch_icon, sec['lat'], sec['lon'])
            ifs_raw,  ifs_dropped  = f_ifs.result(timeout=20)
            icon_raw, icon_dropped = f_icon.result(timeout=20)
        if ifs_dropped or icon_dropped:
            dropped_vars[sec['name']] = {'ifs': ifs_dropped, 'icon': icon_dropped}

        identical = models_identical(ifs_raw, icon_raw)
        if identical:
            any_identical = True
            print(f"❌ {sec['name']:20} | IFS and ICON arrays IDENTICAL — consensus invalid")

        ifs_now  = get_hourly_from_raw(ifs_raw,  hour_utc,   'IFS')
        ifs_3h   = get_hourly_from_raw(ifs_raw,  hour_utc+3, 'IFS')
        ifs_6h   = get_hourly_from_raw(ifs_raw,  hour_utc+6, 'IFS')
        icon_now = get_hourly_from_raw(icon_raw, hour_utc,   'ICON')
        icon_3h  = get_hourly_from_raw(icon_raw, hour_utc+3, 'ICON')
        icon_6h  = get_hourly_from_raw(icon_raw, hour_utc+6, 'ICON')

        ipma_d = ipma_forecasts.get(sec['id'])

        d_now, div_now = ConsensusEngine.merge(ifs_now, icon_now, ipma_d, identical)
        d_3h,  div_3h  = ConsensusEngine.merge(ifs_3h,  icon_3h,  None,   identical)
        d_6h,  div_6h  = ConsensusEngine.merge(ifs_6h,  icon_6h,  None,   identical)

        if d_now['temp'] != 0:
            efl = sec['altitude_m'] + (d_now['temp'] / 0.0065)
            if abs(d_now['freezing_level'] - efl) > 500:
                d_now['freezing_level'] = d_3h['freezing_level'] = d_6h['freezing_level'] = efl

        nasa = nasa_cache.get(sec['id'])
        def _nasa(h): return nasa[h % 24] if nasa else None
        d_now = microclimate(sec, d_now, ts_now,                                _nasa(hour_utc))
        d_3h  = microclimate(sec, d_3h,  ts_now + datetime.timedelta(hours=3), _nasa(hour_utc+3))
        d_6h  = microclimate(sec, d_6h,  ts_now + datetime.timedelta(hours=6), _nasa(hour_utc+6))

        (stat, eei_val, wind_val, gust_val, is_snow, snow_int, eei_3h, eei_6h,
         storm_level, heat_level, wbgt_now) = generate_card(
            sec, d_now, d_3h, d_6h, ts_now,
            aemet_storm_active, aemet_storm_severity, div_now)

        g_min_eei  = min(g_min_eei,  eei_val)
        g_max_wind = max(g_max_wind, wind_val)
        g_max_gust = max(g_max_gust, gust_val)
        if is_snow: snow_detected = True
        if storm_level != "NONE": storm_detected = True; g_storm_level = storm_level
        if heat_level != "NONE":
            heat_detected = True
            if _heat_rank(heat_level) > _heat_rank(g_heat_level): g_heat_level = heat_level
        if wbgt_now is not None and wbgt_now > g_max_wbgt: g_max_wbgt = wbgt_now

        rk = status_rank(stat)                                   # FIX A3
        if rk > worst_rank:
            worst_rank = rk; worst_status = stat; worst_sector = sec['name']

        lp      = (d_now.get('lightning_potential') or 0)
        div_tag = f"[{div_now['status'][:3]}]"
        flags   = " ".join(filter(None,[
            f"⚡{storm_level}" if storm_level != "NONE" else "",
            f"❄{snow_int}"    if is_snow else "",
            f"🔥{heat_level.split()[-1]}" if heat_level != "NONE" else "",
            "🌫"              if d_now.get('fog_alert') else "",
            f"LP:{lp:.0%}"   if lp > 0.05 else "",
            f"G{int(gust_val)}" if gust_val >= 40 else "",
        ]))
        print(f"✅ {sec['name']:20} | MRI:{eei_val:3d}° | WBGT:{wbgt_now:4.1f}° | {stat:<28} {div_tag} {flags}")

        divergence_log.append({
            'sector':       sec['name'],
            'status':       div_now['status'],
            'temp_spread':  div_now['temp_spread'],
            'gust_spread':  div_now['gust_spread'],
            'ifs_temp':     div_now['ifs_temp'],
            'icon_temp':    div_now['icon_temp'],
            'ipma_temp':    div_now['ipma_temp'],
        })

    except Exception as e:
        print(f"❌ {sec['name']:20} | {str(e)[:80]}")
        sectors_failed.append({'id': sec['id'], 'name': sec['name'], 'error': str(e)[:120]})   # FIX E14
        continue

    json_sectors.append({
        "id": sec['id'], "name": sec['name'],
        "coords": {"lat":sec['lat'],"lon":sec['lon'],"elevation":sec['altitude_m']},
        "terrain_type": sec['type'], "description": sec['desc'],
        "current": {
            "eei":              eei_val,
            "status":           stat,
            "status_rank":      status_rank(stat),
            "temp":             round(d_now['temp'],1),
            "wind":             round(d_now['wind'],1),
            "gust":             round(d_now['gust'],1),
            "rain":             round(d_now['rain'],1),
            "humidity":         d_now['hum'],
            "irradiance":       round(d_now['irradiance'],1),
            "irradiance_source":d_now.get('irradiance_source','OPEN_METEO'),
            "freezing_level":   round(d_now['freezing_level'],0),
            "snow_detected":    is_snow,
            "snow_intensity":   snow_int if is_snow else None,
            "cape":             round(d_now.get('cape',0) or 0,0),
            "lifted_index":     round(d_now.get('lifted_index',0) or 0,1),
            "cin":              round(d_now.get('cin',0) or 0,0),
            "lightning_potential": round((d_now.get('lightning_potential') or 0)*100,1),
            "storm_level":      storm_level,
            "heat": {
                "wbgt_c":     wbgt_now,
                "heat_level": heat_level,
                "in_window":  HeatStress.in_window(ts_now),
            },
            "ipma_areas":       SECTOR_IPMA_AREAS.get(sec['name'],[]),
            "aemet_storm_alert":aemet_storm_active,
            "microclimate_notes": d_now.get('fog_alert') or d_now.get('mtb_hazard') or None,
            "aemet_calibrated": sec['name'] in aemet_corrections,
        },
        "forecast_3h": {
            "eei":   round(eei_3h,1),   "temp": round(d_3h['temp'],1),
            "wind":  round(d_3h['wind'],1), "gust": round(d_3h['gust'],1), "rain": round(d_3h['rain'],1),
            "lightning_potential": round((d_3h.get('lightning_potential') or 0)*100,1),
        },
        "forecast_6h": {
            "eei":   round(eei_6h,1),   "temp": round(d_6h['temp'],1),
            "wind":  round(d_6h['wind'],1), "gust": round(d_6h['gust'],1), "rain": round(d_6h['rain'],1),
            "lightning_potential": round((d_6h.get('lightning_potential') or 0)*100,1),
        },
        "consensus": {
            "status":            div_now['status'],
            "models_identical":  div_now['models_identical'],
            "temp_spread_c":     div_now['temp_spread'],
            "precip_spread_mm":  div_now['precip_spread'],
            "wind_spread_kmh":   div_now['wind_spread'],
            "gust_spread_kmh":   div_now['gust_spread'],
            "ifs_temp":          div_now['ifs_temp'],
            "icon_temp":         div_now['icon_temp'],
            "ifs_gust":          div_now['ifs_gust'],
            "icon_gust":         div_now['icon_gust'],
            "ipma_temp":         div_now['ipma_temp'],
            "storm_confirmed":   div_now['storm_confirmed'],
            "hail_confirmed":    div_now['hail_confirmed'],
            "clear_confirmed":   div_now['clear_confirmed'],
            "sources_active":    div_now['sources_active'],
            "ipma_station":      div_now['ipma_station'],
        },
    })

generate_banner(worst_status, g_min_eei, g_max_wind, g_max_gust, worst_sector, time_str,
                snow_detected, g_storm_level, any_identical)
generate_map()

print("\n📊 Writing JSON...")
status_data = {
    "timestamp_utc":  ts_now.isoformat(),
    "timestamp_local":to_local(ts_now).isoformat(),
    "last_update":    time_str,
    "event":          "MQ2026",
    "model_version":  f"MQ Rider Index™ v3.2 | BELLATOR {VERSION} — CONSENSUS + HEAT + GUSTS",
    "data_sources": [
        f"ECMWF IFS 0.25° (Open-Meteo {MODEL_IFS}) — PRIMARY T/W/G/P/Irr/CAPE",
        f"DWD ICON-EU 7km (Open-Meteo {MODEL_ICON}) — SECONDARY T/W/G/P/Irr + LI/CIN/lightning",
        "IPMA Point Forecast API (nearest station, lapse-corrected) — ANCHOR PT",
        "NASA POWER (irradiance fallback when model value missing) — TIER 2",
        f"AEMET calibration ({len(aemet_corrections)} sectors) — TIER 3",
        "Portugal Microclimate Algorithms (wind exposure, valley window) — TIER 4",
        "IPMA Warnings (VRL/BRG/PTO/AVR) — TIER 0-A OVERRIDE",
        "AEMET Storm Alerts (NW Iberia) — TIER 0-B",
        "ICON lightning_potential + CAPE + LI + CIN — TIER 0-C/D/E/F",
    ],
    "api_version":    "v21.2_consensus_heat_gust",
    "consensus_engine": {
        "weights":    {"IFS": ConsensusEngine.W_IFS, "ICON": ConsensusEngine.W_ICON,
                       "IPMA": ConsensusEngine.W_IPMA},
        "thresholds": {
            "temp_uncertain_c":    ConsensusEngine.TEMP_T1,
            "temp_divergent_c":    ConsensusEngine.TEMP_T2,
            "precip_uncertain_mm": ConsensusEngine.PRECIP_T1,
            "precip_divergent_mm": ConsensusEngine.PRECIP_T2,
            "wind_uncertain_kmh":  ConsensusEngine.WIND_T1,
            "wind_divergent_kmh":  ConsensusEngine.WIND_T2,
            "gust_uncertain_kmh":  ConsensusEngine.GUST_T1,
            "gust_divergent_kmh":  ConsensusEngine.GUST_T2,
        },
        "models_identical_detected": any_identical,
        "variables_dropped": dropped_vars,
        "storm_confirmation": "code>=95 in BOTH IFS and ICON to reach SEVERE; hail 96/99 in both to reach EXTREME",
        "clear_confirmation": "both models code<=2",
    },
    "summary": {
        "alert_level":    worst_status,
        "alert_rank":     worst_rank,
        "worst_sector":   worst_sector or "ALL SECTORS",
        "min_mri":        g_min_eei,
        "max_wind":       round(g_max_wind,1),
        "max_gust":       round(g_max_gust,1),
        "snow_detected":  snow_detected,
        "storm_detected": storm_detected,
        "storm_level":    g_storm_level,
        "heat_detected":  heat_detected,
        "heat_level":     g_heat_level,
        "max_wbgt_c":     round(g_max_wbgt,1) if g_max_wbgt > -99 else None,
        "aemet_storm_alert":  aemet_storm_active,
        "aemet_alert_text":   aemet_alert_text,
        "ipma_active":    any(check_ipma_for_sector(s['name'])[0] for s in sectors),
        "nasa_power_coverage":  f"{len(nasa_cache)}/{len(sectors)}",
        "ipma_forecast_coverage": f"{len(ipma_forecasts)}/{len(sectors)}",
        "aemet_calibrated_sectors": len(aemet_corrections),
        "aemet_status":   aemet_status,
        "divergence_log": divergence_log,
        "divergent_sectors": [d['sector'] for d in divergence_log if d['status']=='DIVERGENT'],
        "sectors_failed": sectors_failed,
        "sectors_ok":     len(json_sectors),
    },
    "ipma_area_mapping": SECTOR_IPMA_AREAS,
    "sectors": json_sectors,
    "usage": {
        "ghost_rider":  "sectors[].current.eei — pace per sector",
        "bios":         "sectors[].current.eei + storm_level — stress multiplier",
        "lightning":    "sectors[].current.lightning_potential — %",
        "gust":         "sectors[].current.gust — km/h (exposure above 1000 m)",
        "consensus":    "sectors[].consensus.status — AGREE/UNCERTAIN/DIVERGENT/INVALID",
        "integrity":    "summary.sectors_failed — consumers must not assume 6 sectors",
        "endpoint":     "https://mountainquest.pt/atmos/MQ_ATMOS_STATUS.json",
    }
}

with open(f"{OUTPUT_FOLDER}MQ_ATMOS_STATUS.json",'w') as f:
    json.dump(status_data, f, indent=2)
print(f"✅ {OUTPUT_FOLDER}MQ_ATMOS_STATUS.json")

# ─────────────────────────────────────────────────────────────────────────────
# UPLOAD — FIX E16: FTPS first, plain FTP fallback
# ─────────────────────────────────────────────────────────────────────────────
print("\n🚀 Upload...")
if "FTP_USER" in os.environ:
    files = [(f"{OUTPUT_FOLDER}MQ_HOME_BANNER.png", "MQ_HOME_BANNER.png"),
             (f"{OUTPUT_FOLDER}MQ_TACTICAL_MAP_CALIBRATED.html", "MQ_TACTICAL_MAP_CALIBRATED.html"),
             (f"{OUTPUT_FOLDER}MQ_ATMOS_STATUS.json", "MQ_ATMOS_STATUS.json")] + \
            [(f"{OUTPUT_FOLDER}MQ_SECTOR_{i}_STATUS.png", f"MQ_SECTOR_{i}_STATUS.png") for i in range(1,7)]
    def _upload(ftp):
        for local, remote in files:
            if os.path.exists(local):
                with open(local,'rb') as f: ftp.storbinary(f'STOR {remote}', f)
                print(f"   ✓ {remote}")
    done = False
    try:
        ftp = ftplib.FTP_TLS(); ftp.connect("ftp.nexplore.pt", 21, timeout=30)
        ftp.login(os.environ["FTP_USER"], os.environ["FTP_PASS"]); ftp.prot_p(); ftp.set_pasv(True)
        _upload(ftp); ftp.quit(); done = True
        print("✅ FTPS → mountainquest.pt/atmos/MQ_ATMOS_STATUS.json")
    except Exception as e:
        print(f"⚠️  FTPS failed ({str(e)[:60]}) — falling back to PLAIN FTP (credentials unencrypted)")
    if not done:
        try:
            ftp = ftplib.FTP(); ftp.connect("ftp.nexplore.pt", 21, timeout=30)
            ftp.login(os.environ["FTP_USER"], os.environ["FTP_PASS"]); ftp.set_pasv(True)
            _upload(ftp); ftp.quit()
            print("✅ FTP → mountainquest.pt/atmos/MQ_ATMOS_STATUS.json")
        except Exception as e: print(f"❌ FTP: {e}")
else:
    print("⚠️  LOCAL MODE — FTP_USER not set")

# ── TERMINAL SUMMARY ──
divergent = [d['sector'] for d in divergence_log if d['status']=='DIVERGENT']
uncertain = [d['sector'] for d in divergence_log if d['status']=='UNCERTAIN']
print(f"\n{'═'*62}")
print(f"BELLATOR {VERSION}  |  {ts_now.strftime('%Y-%m-%d %H:%M UTC')}  |  {to_local(ts_now).strftime('%H:%M')} Lisboa")
print(f"SOURCES: {MODEL_IFS} · {MODEL_ICON} · IPMA ({len(ipma_forecasts)}) · NASA ({len(nasa_cache)})")
print(f"MIN MRI: {g_min_eei}°  |  MAX WIND: {int(g_max_wind)}  |  MAX GUST: {int(g_max_gust)} km/h")
print(f"SNOW:  {'YES' if snow_detected else 'NO'}  |  STORM: {'YES — '+g_storm_level if storm_detected else 'NO'}  |  HEAT: {g_heat_level}")
print(f"IPMA:  {'ACTIVE' if status_data['summary']['ipma_active'] else 'OK'}  |  AEMET: {'ALERT' if aemet_storm_active else 'OK'}")
print(f"STATUS: {worst_status} (rank {worst_rank}) @ {worst_sector or 'ALL'}")
if any_identical: print("❌ MODELS IDENTICAL — consensus INVALID, check models= parameter")
if dropped_vars:  print(f"ℹ  VARS DROPPED: {json.dumps(dropped_vars)}")
if divergent:     print(f"DIVERGENT:  {', '.join(divergent)}")
if uncertain:     print(f"UNCERTAIN:  {', '.join(uncertain)}")
if sectors_failed:print(f"❌ FAILED:   {', '.join(s['name'] for s in sectors_failed)}")
print(f"{'═'*62}\n")
