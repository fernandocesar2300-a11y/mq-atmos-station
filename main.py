"""
MQ ATMOS LAB — BELLATOR V20.0
IPMA + LIGHTNING UPDATE

CHANGELOG V20.0 vs V19.1:
✅ IPMA warnings as TIER 0 override (no API key needed)
   VRL covers Marão + Alvão + Gavião directly
   Yellow/Orange/Red Trovoada = immediate status override
✅ lightning_potential field from Open-Meteo
✅ convective_inhibition (CIN) — suppression factor
✅ evaluate_storm() — unified priority override above EEI
✅ AEMET storm alerts maintained as secondary check
✅ Language: English / Portuguese. No Spanish.
"""

import gpxpy, gpxpy.gpx
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import requests
import datetime, os, ftplib, folium, math, json, time
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, TimeoutError

print("📡 MQ ATMOS BELLATOR V20.0 — IPMA + LIGHTNING")

# ─────────────────────────────────────────────────────────────────────────────
# CREDENTIALS
# ─────────────────────────────────────────────────────────────────────────────
NASA_EARTHDATA_TOKEN = os.environ.get("NASA_EARTHDATA_TOKEN", "")
AEMET_API_KEY        = os.environ.get("AEMET_API_KEY", "")

# ─────────────────────────────────────────────────────────────────────────────
# AEMET CALIBRATION FRESHNESS
# ─────────────────────────────────────────────────────────────────────────────
def check_aemet_freshness():
    try:
        with open('aemet_corrections.json') as f:
            data = json.load(f)
        for _, sd in data.items():
            if 'last_update' in sd:
                age = datetime.datetime.utcnow() - datetime.datetime.fromisoformat(sd['last_update'])
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
# TIER 0-A: IPMA WARNINGS — no API key, real-time, covers VRL/BRG/PTO
# ─────────────────────────────────────────────────────────────────────────────

# MQ sector → IPMA district area codes
SECTOR_IPMA_AREAS = {
    "AMARANTE":         ["PTO", "AVR", "BRG"],
    "S. DA ABOBOREIRA": ["BRG", "PTO"],
    "SERRA DO MARÃO":   ["VRL", "BRG", "AVR"],
    "GAVIÃO":           ["VRL"],
    "SERRA DO ALVÃO":   ["VRL"],
    "SRA. GRAÇA":       ["VRL", "BRG"],
}

STORM_AWARENESS_TYPES = {"Trovoada", "Precipitação", "Neve", "Vento"}
IPMA_LEVEL_RANK = {"red": 4, "orange": 3, "yellow": 2, "green": 0}

_ipma_warnings_cache = None

def fetch_ipma_warnings():
    """Fetch once, cache for the run."""
    global _ipma_warnings_cache
    if _ipma_warnings_cache is not None:
        return _ipma_warnings_cache
    try:
        with urllib.request.urlopen(
            "https://api.ipma.pt/open-data/forecast/warnings/warnings_www.json",
            timeout=8) as r:
            _ipma_warnings_cache = json.loads(r.read())
            print(f"✅ IPMA warnings: {len(_ipma_warnings_cache)} entries loaded")
            return _ipma_warnings_cache
    except Exception as e:
        print(f"⚠️  IPMA warnings fetch failed: {e}")
        _ipma_warnings_cache = []
        return []

def check_ipma_for_sector(sector_name):
    """
    Returns (active: bool, max_level: str, texts: list[str])
    Only yellow/orange/red trigger. Green = informational only.
    """
    areas    = SECTOR_IPMA_AREAS.get(sector_name, [])
    warnings = fetch_ipma_warnings()
    now      = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    active_warnings = []
    max_rank        = 0
    max_level       = None

    for w in warnings:
        if w.get('idAreaAviso') not in areas:
            continue
        if w.get('awarenessTypeName') not in STORM_AWARENESS_TYPES:
            continue

        level = w.get('awarenessLevelID', 'green')
        rank  = IPMA_LEVEL_RANK.get(level, 0)
        if rank < 2:  # green = 0, skip; only yellow (2), orange (3), red (4)
            continue

        try:
            start = datetime.datetime.fromisoformat(w['startTime'])
            end   = datetime.datetime.fromisoformat(w['endTime'])
        except Exception:
            continue

        if start <= now <= end:
            active_warnings.append({
                'level':    level,
                'rank':     rank,
                'type':     w.get('awarenessTypeName', ''),
                'text':     w.get('text', ''),
                'area':     w.get('idAreaAviso', ''),
            })
            if rank > max_rank:
                max_rank  = rank
                max_level = level

    if not active_warnings:
        return False, None, []

    texts = [f"[{w['level'].upper()}] {w['type']}: {w['text'][:60]}"
             for w in active_warnings]
    return True, max_level, texts

def ipma_to_storm_status(max_level, texts):
    """Convert IPMA level to rider-facing status + color."""
    combined = " ".join(texts).lower()
    has_hail    = any(k in combined for k in ['granizo', 'hail'])
    has_thunder = any(k in combined for k in ['trovoada', 'thunder', 'elétric'])

    if max_level == 'red':
        if has_hail:    return "HAIL — DO NOT RIDE",    "#4a0000", "EXTREME"
        return "EXTREME STORM — IPMA RED",  "#4a0000", "EXTREME"
    if max_level == 'orange':
        if has_hail:    return "HAIL WARNING — IPMA",   "#8b0000", "SEVERE"
        if has_thunder: return "SEVERE STORM — IPMA",   "#8b0000", "SEVERE"
        return "STORM WARNING — IPMA ORANGE",            "#8b0000", "SEVERE"
    if max_level == 'yellow':
        if has_hail:    return "HAIL POSSIBLE — IPMA",  "#c0392b", "HIGH"
        if has_thunder: return "THUNDERSTORM — IPMA",   "#c0392b", "HIGH"
        return "STORM WARNING — IPMA YELLOW",            "#e67e22", "MODERATE"
    return None, None, "NONE"

# ─────────────────────────────────────────────────────────────────────────────
# TIER 0-B: AEMET STORM ALERTS (Galicia / NW Iberia)
# ─────────────────────────────────────────────────────────────────────────────
def check_aemet_storm_alerts():
    if not AEMET_API_KEY:
        return False, "AEMET key not configured", None
    try:
        r = requests.get(
            "https://opendata.aemet.es/opendata/api/avisos_cap/ultimoelaborado/area/61",
            params={"api_key": AEMET_API_KEY}, timeout=8)
        if r.status_code != 200:
            return False, f"AEMET HTTP {r.status_code}", None
        meta = r.json()
        data_url = meta.get("datos")
        if not data_url:
            return False, "AEMET no data URL", None
        r2 = requests.get(data_url, timeout=10)
        if r2.status_code != 200:
            return False, f"AEMET CAP HTTP {r2.status_code}", None
        root     = ET.fromstring(r2.content)
        ns       = {'cap': 'urn:oasis:names:tc:emergency:cap:1.2'}
        events   = []
        max_sev  = None
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
        if events:
            return True, f"AEMET: {', '.join(events[:3])}", max_sev
        return False, "AEMET: no active storm warnings", None
    except Exception as e:
        return False, f"AEMET alerts error: {str(e)[:60]}", None

# ─────────────────────────────────────────────────────────────────────────────
# EEI v3.1 — UNCHANGED
# ─────────────────────────────────────────────────────────────────────────────
class EEI_v31:
    MU=0.6; V_RIDER=16; V_EFF_MIN=4.8; T_THRESHOLD=20
    LAMBDA_BASE=0.3; LAMBDA_HR=0.4; R_THRESHOLD=0.5; ALPHA=0.007

    @staticmethod
    def calcular_elevacion_solar(lat, lon, timestamp):
        jd = (timestamp.timestamp()/86400.0)+2440587.5
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
        tst= timestamp.hour*60+timestamp.minute+timestamp.second/60+et+4*lon
        ha = (tst/4)-180
        se = (math.sin(math.radians(lat))*math.sin(math.radians(dec))+
              math.cos(math.radians(lat))*math.cos(math.radians(dec))*math.cos(math.radians(ha)))
        return math.degrees(math.asin(max(-1.0, min(1.0, se))))

    @staticmethod
    def calcular(T_a, v_meteo, HR, R_rate, I_sol, lat, lon, timestamp):
        v_eff = (v_meteo*EEI_v31.MU)+EEI_v31.V_RIDER
        T_wc  = T_a if v_eff<EEI_v31.V_EFF_MIN else (
            13.12+0.6215*T_a-11.37*v_eff**0.16+0.3965*T_a*v_eff**0.16)
        P_wet = 0.0 if R_rate<=EEI_v31.R_THRESHOLD else max(
            0,(EEI_v31.T_THRESHOLD-T_a)*(EEI_v31.LAMBDA_BASE+EEI_v31.LAMBDA_HR*HR/100))
        h_sol = EEI_v31.calcular_elevacion_solar(lat, lon, timestamp)
        G_sol = 0.0 if h_sol<=0 else I_sol*EEI_v31.ALPHA*math.sin(math.radians(h_sol))
        EEI   = T_wc-P_wet+G_sol
        if EEI>15:   estado={'nivel':'GOOD CONDITIONS','color':'#2ecc71'}
        elif EEI>10: estado={'nivel':'CAUTION',         'color':'#f1c40f'}
        elif EEI>5:  estado={'nivel':'WARNING',          'color':'#e67e22'}
        elif EEI>0:  estado={'nivel':'DANGER',           'color':'#e74c3c'}
        else:        estado={'nivel':'CRITICAL',         'color':'#8b0000'}
        return round(EEI,1), {
            'T_wc':round(T_wc,1),'P_wet':round(P_wet,1),
            'G_sol':round(G_sol,1),'h_sol':round(h_sol,1),'v_eff':round(v_eff,1)
        }, estado

# ─────────────────────────────────────────────────────────────────────────────
# TIER 0 UNIFIED STORM EVALUATOR
# Priority: IPMA > AEMET > lightning_potential > weathercode > CAPE+LI
# ─────────────────────────────────────────────────────────────────────────────
def evaluate_storm(sector_name, weathercode, cape, lifted_index, cin,
                   lightning_potential, aemet_active=False, aemet_severity=None):
    """
    Returns (status, color, level) or (None, None, 'NONE').
    Absolute override above EEI.
    """
    cape = cape or 0
    li   = lifted_index or 0
    cin  = cin or 0
    lp   = lightning_potential or 0

    # ── TIER 0-A: IPMA active warning (VRL/BRG/PTO) ──
    ipma_active, ipma_level, ipma_texts = check_ipma_for_sector(sector_name)
    if ipma_active:
        s, c, lvl = ipma_to_storm_status(ipma_level, ipma_texts)
        if s: return s, c, lvl

    # ── TIER 0-B: AEMET active warning (Galicia) ──
    if aemet_active:
        if aemet_severity == 'Extreme': return "EXTREME STORM — AEMET", "#4a0000", "EXTREME"
        return "SEVERE STORM — AEMET", "#8b0000", "SEVERE"

    # ── TIER 0-C: Hail confirmed by model ──
    if weathercode in [96, 99]:
        return "HAIL — DO NOT RIDE", "#4a0000", "EXTREME"

    # ── TIER 0-D: Active thunderstorm (weathercode) ──
    if 95 <= weathercode <= 99:
        if cape > 2500 or lp > 0.5:
            return "SEVERE THUNDERSTORM", "#8b0000", "SEVERE"
        return "THUNDERSTORM", "#c0392b", "HIGH"

    # ── TIER 0-E: Lightning potential (Open-Meteo real-time field) ──
    if lp > 0.5: return "LIGHTNING RISK — HIGH",  "#8b0000", "SEVERE"
    if lp > 0.3: return "LIGHTNING RISK",          "#e74c3c", "MODERATE"
    if lp > 0.1: return "LIGHTNING POSSIBLE",      "#e67e22", "LOW"

    # ── TIER 0-F: Developing cell (CAPE + LI + CIN) ──
    cin_suppressed = cin < -200
    if not cin_suppressed:
        if cape > 2500 and li < -4: return "STORM IMMINENT",  "#8b0000", "SEVERE"
        if cape > 1500 and li < -2: return "STORM PROBABLE",  "#e74c3c", "MODERATE"
    if cape > 800:                  return "ATMOSPHERIC INSTABILITY", "#e67e22", "LOW"

    return None, None, "NONE"

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
except: pass
if not track_points:
    track_points = [[41.27,-8.08],[41.27,-8.08]]

sectors = [
    {"id":1,"name":"AMARANTE",         "lat":41.2709,"lon":-8.0797,"alt":"65m",  "altitude_m":65,  "type":"FLAT",   "desc":"START / BASE"},
    {"id":2,"name":"S. DA ABOBOREIRA","lat":41.1946,"lon":-8.0563,"alt":"760m", "altitude_m":760, "type":"CLIMB",  "desc":"ENTRY POINT"},
    {"id":3,"name":"SERRA DO MARÃO",  "lat":41.2484,"lon":-7.8862,"alt":"1390m","altitude_m":1390,"type":"DESCEND","desc":"HIGH MOUNTAIN"},
    {"id":4,"name":"GAVIÃO",          "lat":41.2777,"lon":-7.9462,"alt":"986m", "altitude_m":986, "type":"CLIMB",  "desc":"THE FILTER"},
    {"id":5,"name":"SERRA DO ALVÃO",  "lat":41.3738,"lon":-7.8053,"alt":"1043m","altitude_m":1043,"type":"FLAT",   "desc":"PLATEAU"},
    {"id":6,"name":"SRA. GRAÇA",      "lat":41.4168,"lon":-7.9106,"alt":"950m", "altitude_m":950, "type":"CLIMB",  "desc":"THE CLIMB"},
]

# ─────────────────────────────────────────────────────────────────────────────
# NASA POWER
# ─────────────────────────────────────────────────────────────────────────────
def get_nasa_irradiance(lat, lon, date):
    date_str = date.strftime('%Y%m%d')
    try:
        r = requests.get(
            "https://power.larc.nasa.gov/api/temporal/hourly/point",
            params={'parameters':'ALLSKY_SFC_SW_DWN','community':'RE',
                    'longitude':lon,'latitude':lat,
                    'start':date_str,'end':date_str,'format':'JSON'},
            timeout=10)
        if r.status_code!=200: return None
        d = r.json()
        if 'properties' not in d: return None
        hd = d['properties']['parameter']['ALLSKY_SFC_SW_DWN']
        return [max(0, hd.get(f"{date_str}{h:02d}", 0)) for h in range(24)]
    except: return None

def fetch_nasa_parallel(sectors, date):
    cache = {}
    def fetch_one(sec):
        try:
            data = get_nasa_irradiance(sec['lat'], sec['lon'], date)
            if data: return (sec['id'], data)
        except: pass
        return None
    with ThreadPoolExecutor(max_workers=3) as ex:
        for future in {ex.submit(fetch_one, s): s for s in sectors}:
            try:
                result = future.result(timeout=10)
                if result: cache[result[0]] = result[1]
            except: pass
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
# MICROCLIMATE
# ─────────────────────────────────────────────────────────────────────────────
def microclimate(sector, data):
    d=data.copy(); alt=sector['altitude_m']; log=[]
    if sector['name'] in aemet_corrections:
        c=aemet_corrections[sector['name']]
        if c.get('temp_offset',0)!=0:
            d['temp']+=c['temp_offset']; log.append(f"AEMET T{c['temp_offset']:+.1f}°")
        if c.get('wind_factor',1.0)!=1.0:
            w0=d['wind']; d['wind']*=c['wind_factor']; log.append(f"AEMET V {w0:.1f}→{d['wind']:.1f}")
    if alt>1000:
        w0=d['wind']; d['wind']*=1.60; d['temp']-=2
        log.append(f"Ridge +60%V ({w0:.1f}→{d['wind']:.1f})")
    if 400<alt<800:
        d['temp']+=1.5; log.append("Valley +1.5°")
    if alt<800 and d.get('hum',0)>85 and 8<=d.get('temp',0)<=12:
        d['fog_alert']="NORTADA FOG"; log.append("Fog")
    if d.get('rain',0)>5:
        d['mtb_hazard']="SLIPPERY TRACK"; log.append("MTB hazard")
    if log: print(f"🔧 {sector['name']:20} | {' | '.join(log)}")
    return d

# ─────────────────────────────────────────────────────────────────────────────
# HOURLY DATA
# ─────────────────────────────────────────────────────────────────────────────
def get_hourly(r, sec, h, nasa_cache):
    h=min(h,47); e48=[0]*48
    d={
        'temp':                r['hourly']['temperature_2m'][h],
        'wind':                r['hourly']['windspeed_10m'][h],
        'rain':                r['hourly']['precipitation'][h],
        'hum':                 r['hourly']['relativehumidity_2m'][h],
        'code':                r['hourly']['weathercode'][h],
        'irradiance':          r['hourly'].get('global_tilted_irradiance',e48)[h],
        'snowfall':            r['hourly'].get('snowfall',e48)[h],
        'freezing_level':      r['hourly'].get('freezing_level_height',[9999]*48)[h],
        'cape':                r['hourly'].get('cape',e48)[h] or 0,
        'lifted_index':        r['hourly'].get('lifted_index',e48)[h] or 0,
        'cin':                 r['hourly'].get('convective_inhibition',e48)[h] or 0,
        'lightning_potential': r['hourly'].get('lightning_potential',[None]*48)[h],
    }
    if sec['id'] in nasa_cache and h<len(nasa_cache[sec['id']]):
        d['irradiance']=nasa_cache[sec['id']][h]; d['irradiance_source']='NASA_POWER'
    else:
        d['irradiance_source']='OPEN_METEO'
    return microclimate(sec, d)

# ─────────────────────────────────────────────────────────────────────────────
# CARD GENERATOR
# ─────────────────────────────────────────────────────────────────────────────
def generate_card(sector, d_now, d_3h, d_6h, time_str, aemet_active, aemet_severity):
    ts=datetime.datetime.utcnow()
    eei_now,_,estado = EEI_v31.calcular(d_now['temp'],d_now['wind'],d_now['hum'],
        d_now['rain'],d_now['irradiance'],sector['lat'],sector['lon'],ts)
    eei_3h,_,_ = EEI_v31.calcular(d_3h['temp'],d_3h['wind'],d_3h['hum'],
        d_3h['rain'],d_3h['irradiance'],sector['lat'],sector['lon'],ts+datetime.timedelta(hours=3))
    eei_6h,_,_ = EEI_v31.calcular(d_6h['temp'],d_6h['wind'],d_6h['hum'],
        d_6h['rain'],d_6h['irradiance'],sector['lat'],sector['lon'],ts+datetime.timedelta(hours=6))

    status=estado['nivel']; color=estado['color']
    storm_level="NONE"; is_snow=is_mixed=False; snow_int="LIGHT"

    # ── TIER 0: storm override — runs first, always ──
    s_status,s_color,storm_level = evaluate_storm(
        sector['name'], d_now['code'],
        d_now.get('cape',0), d_now.get('lifted_index',0),
        d_now.get('cin',0), d_now.get('lightning_potential'),
        aemet_active, aemet_severity)
    if s_status:
        status=s_status; color=s_color
    elif 0<d_now['temp']<=3 and d_now['rain']>0.1:
        is_mixed=True
        status="SNOW LIKELY" if d_now['temp']<=1.5 else "RAIN/SNOW MIX"
        color="#3498db" if d_now['temp']<=1.5 else "#e67e22"
    elif d_now['rain']>0.1 and d_now['temp']<=1:
        is_snow=True
        snow_int="HEAVY" if d_now['rain']>10 else "MODERATE" if d_now['rain']>3 else "LIGHT"
    elif sector['altitude_m']>d_now['freezing_level'] and d_now['freezing_level']<9000 and d_now['rain']>0.1:
        is_snow=True
        snow_int="HEAVY" if d_now['rain']>10 else "MODERATE" if d_now['rain']>3 else "LIGHT"
    if is_snow:
        status={"HEAVY":"HEAVY SNOWFALL","MODERATE":"SNOWFALL","LIGHT":"LIGHT SNOW"}[snow_int]
        color ={"HEAVY":"#e74c3c","MODERATE":"#e67e22","LIGHT":"#f1c40f"}[snow_int]

    arrow=lambda c,f: "(-)" if f-c<-2 else "(+)" if f-c>2 else "(=)"

    fig,ax=plt.subplots(figsize=(6,3.4),facecolor='#0f172a')
    ax.set_facecolor('#0f172a')
    plt.subplots_adjust(left=0,right=1,top=1,bottom=0)
    ax.add_patch(patches.Rectangle((0,0),0.03,1,transform=ax.transAxes,linewidth=0,facecolor=color))

    plt.text(0.08,0.80,sector['name'],color='white',fontsize=16,fontweight='bold',transform=ax.transAxes)
    plt.text(0.08,0.68,f"{sector['desc']} | {sector['alt']}",color='#94a3b8',fontsize=8,fontweight='bold',transform=ax.transAxes)

    if storm_level in ["EXTREME","SEVERE"]:   wm="HAIL" if "HAIL" in status else "STORM"
    elif storm_level in ["HIGH","MODERATE"]:  wm="⚡"
    elif storm_level=="LOW":                  wm="UNSTABLE"
    elif is_snow or is_mixed:                 wm="SNOW"
    else: wm=weather_text(d_now['code'],d_now['temp'],d_now['rain'],d_now['snowfall'])
    plt.text(0.5,0.40,wm,color='white',alpha=0.10,fontsize=40,fontweight='bold',ha='center',transform=ax.transAxes)

    plt.text(0.92,0.68,f"{int(d_now['temp'])}°",color='white',fontsize=38,fontweight='bold',ha='right',transform=ax.transAxes)

    danger_list=["CRITICAL","DANGER","HEAVY SNOWFALL","HAIL — DO NOT RIDE",
                 "SEVERE THUNDERSTORM","EXTREME STORM — IPMA RED","HAIL WARNING — IPMA",
                 "SEVERE STORM — AEMET","EXTREME STORM — AEMET"]
    mri_col="#ffffff" if status in danger_list else "#38bdf8" if eei_now<d_now['temp'] else "#fca5a5"
    plt.text(0.92,0.55,f"MRI: {int(eei_now)}°",color=mri_col,fontsize=10,fontweight='bold',ha='right',transform=ax.transAxes)
    plt.text(0.08,0.45,f"WIND {int(d_now['wind'])} km/h",color='#94a3b8',fontsize=7,transform=ax.transAxes)

    lp=(d_now.get('lightning_potential') or 0)
    if lp>0.1:
        lp_col="#8b0000" if lp>0.5 else "#e74c3c" if lp>0.3 else "#e67e22"
        plt.text(0.92,0.45,f"⚡ {int(lp*100)}%",color=lp_col,fontsize=8,fontweight='bold',ha='right',transform=ax.transAxes)

    plt.text(0.92,0.25,f" {status} ",color='white',fontsize=9,fontweight='bold',ha='right',
             bbox=dict(boxstyle="round,pad=0.4",fc=color,ec="none",alpha=0.9),transform=ax.transAxes)

    plt.plot([0.05,0.95],[0.15,0.15],color='#334155',linewidth=1,transform=ax.transAxes)

    def flabel(d,alt):
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
    plt.text(0.5,0.02,f"UPDATED: {time_str} UTC | MQ RIDER INDEX™ v3.1 + IPMA + LIGHTNING",
             color='#475569',fontsize=6,ha='center',transform=ax.transAxes)

    ax.axis('off')
    plt.savefig(f"{OUTPUT_FOLDER}MQ_SECTOR_{sector['id']}_STATUS.png",dpi=150,facecolor='#0f172a')
    plt.close()

    return status, int(eei_now), d_now['wind'], is_snow or is_mixed, snow_int, eei_3h, eei_6h, storm_level

# ─────────────────────────────────────────────────────────────────────────────
# BANNER
# ─────────────────────────────────────────────────────────────────────────────
def generate_banner(status, min_eei, max_wind, worst_sector, time_str, snow, storm_level):
    fig,ax=plt.subplots(figsize=(8,2.5),facecolor='#0a0a0a')
    ax.set_facecolor('#0a0a0a')
    plt.subplots_adjust(left=0,right=1,top=1,bottom=0)

    color="#2ecc71"
    if any(x in status for x in ["INSTABILITY","LIGHT SNOW","MIXED","PROBABLE","POSSIBLE","CAUTION"]): color="#f1c40f"
    if any(x in status for x in ["WARNING","SNOW ","STORM PROBABLE","IMMINENT","YELLOW"]): color="#e67e22"
    if any(x in status for x in ["THUNDERSTORM","SEVERE","DANGER","HEAVY SNOW","LIGHTNING RISK","HIGH","ORANGE"]): color="#e74c3c"
    if any(x in status for x in ["HAIL","CRITICAL","EXTREME","RED"]): color="#8b0000"

    ax.add_patch(patches.Rectangle((0,0),0.015,1,transform=ax.transAxes,linewidth=0,facecolor=color))
    ax_r=fig.add_axes([0.05,0.15,0.20,0.70]); ax_r.set_facecolor('#0a0a0a')
    lats=[p[0] for p in track_points]; lons=[p[1] for p in track_points]
    ax_r.plot(lons,lats,color=color,linewidth=1.2,alpha=0.9)
    ax_r.set_aspect('equal'); ax_r.axis('off')
    ax_r.add_patch(patches.Circle((0.5,0.5),0.48,transform=ax_r.transAxes,
                                  fill=False,edgecolor='#333',linewidth=1,linestyle=':'))

    plt.text(0.28,0.70,"MQ METEO STATION",color='white',fontsize=14,fontweight='bold',transform=ax.transAxes)

    if color=="#2ecc71":
        hook="ALL SECTORS: GO"; sub=f"UPDATED: {time_str} UTC | IPMA + LIGHTNING V20.0"
    elif storm_level not in ["NONE",None]:
        hook=f"⚡ {status} — {worst_sector}"; sub=f"UPDATED: {time_str} UTC | IPMA ACTIVE"
    elif snow:
        hook=f"SNOW: {worst_sector}"; sub=f"UPDATED: {time_str} UTC"
    else:
        hook=f"ALERT: {worst_sector}"; sub=f"UPDATED: {time_str} UTC"

    plt.text(0.28,0.50,hook,color=color,fontsize=10,fontweight='bold',transform=ax.transAxes)
    plt.text(0.28,0.35,sub,color='#888',fontsize=8,transform=ax.transAxes)
    plt.plot([0.68,0.68],[0.2,0.8],color='#222',linewidth=1,transform=ax.transAxes)

    plt.text(0.76,0.70,"MIN MRI",color='#666',fontsize=7,ha='center',transform=ax.transAxes)
    plt.text(0.76,0.45,f"{min_eei}°",color="#38bdf8" if min_eei<10 else "white",
             fontsize=20,fontweight='bold',ha='center',transform=ax.transAxes)
    plt.text(0.90,0.70,"MAX WIND",color='#666',fontsize=7,ha='center',transform=ax.transAxes)
    plt.text(0.90,0.45,f"{int(max_wind)}",color="#e67e22" if max_wind>30 else "white",
             fontsize=20,fontweight='bold',ha='center',transform=ax.transAxes)
    plt.text(0.90,0.32,"km/h",color='#666',fontsize=7,ha='center',transform=ax.transAxes)

    ax.axis('off')
    plt.savefig(f"{OUTPUT_FOLDER}MQ_HOME_BANNER.png",facecolor='#0a0a0a',dpi=150)
    plt.close()

def generate_map():
    center=track_points[len(track_points)//2] if len(track_points)>10 else [41.30,-7.95]
    m=folium.Map(location=center,zoom_start=10,tiles='CartoDB dark_matter')
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
print("\n📡 FETCHING DATA...")
now=datetime.datetime.now(); time_str=now.strftime("%H:%M"); current_hour=now.hour

# Pre-fetch IPMA once for all sectors
fetch_ipma_warnings()

# AEMET storm check
print("⚡ Checking AEMET storm alerts...")
aemet_storm_active,aemet_alert_text,aemet_storm_severity=check_aemet_storm_alerts()
print(f"   {aemet_alert_text}")

worst_status="STABLE"; worst_sector=""
g_min_eei=99; g_max_wind=0
snow_detected=storm_detected=False; g_storm_level="NONE"
json_sectors=[]

print("\n☀️  NASA POWER (parallel)...")
t0=time.time()
nasa_cache=fetch_nasa_parallel(sectors,now)
elapsed=time.time()-t0
print(f"   ✅ {len(nasa_cache)}/{len(sectors)} sectors ({elapsed:.1f}s)")

print("\n🌍 Processing sectors...")
for sec in sectors:
    try:
        r=requests.get(
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={sec['lat']}&longitude={sec['lon']}"
            f"&hourly=temperature_2m,windspeed_10m,weathercode,precipitation,"
            f"relativehumidity_2m,global_tilted_irradiance,snowfall,"
            f"freezing_level_height,cape,lifted_index,"
            f"convective_inhibition,lightning_potential"
            f"&forecast_days=2",
            timeout=10).json()

        d_now=get_hourly(r,sec,current_hour,   nasa_cache)
        d_3h =get_hourly(r,sec,current_hour+3, nasa_cache)
        d_6h =get_hourly(r,sec,current_hour+6, nasa_cache)

        if d_now['temp']!=0:
            efl=sec['altitude_m']+(d_now['temp']/0.0065)
            if abs(d_now['freezing_level']-efl)>500:
                d_now['freezing_level']=d_3h['freezing_level']=d_6h['freezing_level']=efl

        stat,eei_val,wind_val,is_snow,snow_int,eei_3h,eei_6h,storm_level=generate_card(
            sec,d_now,d_3h,d_6h,time_str,aemet_storm_active,aemet_storm_severity)

        if eei_val <g_min_eei:  g_min_eei =eei_val
        if wind_val>g_max_wind: g_max_wind=wind_val
        if is_snow:  snow_detected=True
        if storm_level!="NONE": storm_detected=True; g_storm_level=storm_level

        if any(x in stat for x in ["WARNING","SNOW","STORM","HAIL","DANGER","CRITICAL",
                                    "THUNDER","LIGHTNING","INSTABILITY","IMMINENT","PROBABLE","IPMA","AEMET"]):
            worst_status=stat; worst_sector=sec['name']

        lp=(d_now.get('lightning_potential') or 0)
        flags=" ".join(filter(None,[
            f"⚡{storm_level}" if storm_level!="NONE" else "",
            f"❄{snow_int}" if is_snow else "",
            "🌫" if d_now.get('fog_alert') else "",
            f"LP:{lp:.0%}" if lp>0.05 else ""
        ]))
        print(f"✅ {sec['name']:20} | MRI:{eei_val:3d}° | {stat:<30} {flags}")

    except Exception as e:
        print(f"❌ {sec['name']:20} | {str(e)[:60]}")
        continue

    json_sectors.append({
        "id":sec['id'],"name":sec['name'],
        "coords":{"lat":sec['lat'],"lon":sec['lon'],"elevation":sec['altitude_m']},
        "terrain_type":sec['type'],"description":sec['desc'],
        "current":{
            "eei":eei_val,"status":stat,
            "temp":round(d_now['temp'],1),"wind":round(d_now['wind'],1),
            "rain":round(d_now['rain'],1),"humidity":d_now['hum'],
            "irradiance":round(d_now['irradiance'],1),
            "irradiance_source":d_now.get('irradiance_source','OPEN_METEO'),
            "freezing_level":round(d_now['freezing_level'],0),
            "snow_detected":is_snow,"snow_intensity":snow_int if is_snow else None,
            "cape":round(d_now.get('cape',0),0),
            "lifted_index":round(d_now.get('lifted_index',0),1),
            "cin":round(d_now.get('cin',0),0),
            "lightning_potential":round((d_now.get('lightning_potential') or 0)*100,1),
            "storm_level":storm_level,
            "ipma_areas":SECTOR_IPMA_AREAS.get(sec['name'],[]),
            "aemet_storm_alert":aemet_storm_active,
            "microclimate_notes":d_now.get('fog_alert') or d_now.get('mtb_hazard') or None,
            "aemet_calibrated":sec['name'] in aemet_corrections
        },
        "forecast_3h":{
            "eei":round(eei_3h,1),"temp":round(d_3h['temp'],1),
            "wind":round(d_3h['wind'],1),"rain":round(d_3h['rain'],1),
            "lightning_potential":round((d_3h.get('lightning_potential') or 0)*100,1)
        },
        "forecast_6h":{
            "eei":round(eei_6h,1),"temp":round(d_6h['temp'],1),
            "wind":round(d_6h['wind'],1),"rain":round(d_6h['rain'],1),
            "lightning_potential":round((d_6h.get('lightning_potential') or 0)*100,1)
        }
    })

generate_banner(worst_status,g_min_eei,g_max_wind,worst_sector,time_str,snow_detected,g_storm_level)
generate_map()

print("\n📊 Writing JSON...")
status_data={
    "timestamp_utc":now.isoformat(),"last_update":time_str,
    "event":"MQ2026","model_version":"MQ Rider Index™ v3.1 + IPMA + Lightning | BELLATOR V20.0",
    "data_sources":[
        "ECMWF IFS (Open-Meteo) — TIER 1",
        "NASA POWER (parallel) — TIER 2",
        f"AEMET calibration ({len(aemet_corrections)} sectors) — TIER 3",
        "Portugal Microclimate Algorithms — TIER 4",
        "IPMA Warnings (VRL/BRG/PTO/AVR) — TIER 0-A OVERRIDE",
        "AEMET Storm Alerts (Galicia) — TIER 0-B",
        "Open-Meteo lightning_potential + CAPE + CIN — TIER 0-C/D/E/F"
    ],
    "api_version":"v20.0_ipma_lightning",
    "summary":{
        "alert_level":worst_status,"worst_sector":worst_sector or "ALL SECTORS",
        "min_mri":g_min_eei,"max_wind":round(g_max_wind,1),
        "snow_detected":snow_detected,"storm_detected":storm_detected,
        "storm_level":g_storm_level,
        "aemet_storm_alert":aemet_storm_active,"aemet_alert_text":aemet_alert_text,
        "ipma_active": any(
            check_ipma_for_sector(s['name'])[0] for s in sectors
        ),
        "nasa_power_coverage":f"{len(nasa_cache)}/{len(sectors)}",
        "aemet_calibrated_sectors":len(aemet_corrections),"aemet_status":aemet_status
    },
    "ipma_area_mapping": SECTOR_IPMA_AREAS,
    "sectors":json_sectors,
    "usage":{
        "ghost_rider":"sectors[].current.eei — pace adjustment per sector",
        "bios":"sectors[].current.eei + storm_level — stress multiplier",
        "lightning":"sectors[].current.lightning_potential — % probability",
        "ipma":"sectors[].current.ipma_areas — district codes for manual check",
        "endpoint":"https://mountainquest.pt/atmos/MQ_ATMOS_STATUS.json"
    }
}

with open(f"{OUTPUT_FOLDER}MQ_ATMOS_STATUS.json",'w') as f:
    json.dump(status_data,f,indent=2)
print(f"✅ {OUTPUT_FOLDER}MQ_ATMOS_STATUS.json")

print("\n🚀 FTP upload...")
if "FTP_USER" in os.environ:
    try:
        ftp=ftplib.FTP(); ftp.connect("ftp.nexplore.pt",21,timeout=30)
        ftp.login(os.environ["FTP_USER"],os.environ["FTP_PASS"]); ftp.set_pasv(True)
        def up(local,remote):
            if os.path.exists(local):
                with open(local,'rb') as f: ftp.storbinary(f'STOR {remote}',f)
                print(f"   ✓ {remote}")
        up(f"{OUTPUT_FOLDER}MQ_HOME_BANNER.png",              "MQ_HOME_BANNER.png")
        up(f"{OUTPUT_FOLDER}MQ_TACTICAL_MAP_CALIBRATED.html", "MQ_TACTICAL_MAP_CALIBRATED.html")
        up(f"{OUTPUT_FOLDER}MQ_ATMOS_STATUS.json",            "MQ_ATMOS_STATUS.json")
        for i in range(1,7): up(f"{OUTPUT_FOLDER}MQ_SECTOR_{i}_STATUS.png",f"MQ_SECTOR_{i}_STATUS.png")
        ftp.quit(); print("✅ FTP → mountainquest.pt/atmos/MQ_ATMOS_STATUS.json")
    except Exception as e: print(f"❌ FTP: {e}")
else:
    print("⚠️  LOCAL MODE — FTP_USER not set")

print(f"\n{'═'*60}")
print(f"BELLATOR V20.0 | {now.strftime('%Y-%m-%d %H:%M UTC')}")
print(f"MIN MRI: {g_min_eei}°C | MAX WIND: {int(g_max_wind)} km/h")
print(f"SNOW: {'YES' if snow_detected else 'NO'} | STORM: {'YES — '+g_storm_level if storm_detected else 'NO'}")
print(f"IPMA: {'ACTIVE' if status_data['summary']['ipma_active'] else 'OK'}")
print(f"AEMET: {'ALERT' if aemet_storm_active else 'OK'}")
print(f"STATUS: {worst_status}")
print(f"{'═'*60}\n")
