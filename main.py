"""
═══════════════════════════════════════════════════════════════════════════
MQ ATMOS LAB: BELLATOR V20.0 — CONVECTION UPDATE
═══════════════════════════════════════════════════════════════════════════
CHANGELOG V20.0:
✅ FIX #5: weathercode 95-99 (trovoada/granizo) override status direto
✅ FIX #6: CAPE + Lifted Index via Open-Meteo (convective risk)
✅ FIX #7: CAPE entra no EEI summary e no banner
✅ FIX #8: Granizo (code 96/99) = HAIL — DANGER, cor #4a0000
✅ KEEP: V19.1 FIXED architecture 100% intacta

MODELO EEI v3.1 + CONVECTION LAYER:
  EEI = T_wc - P_wet + G_sol          ← térmico (inalterado)
  CONVECTION = f(weathercode, CAPE, LI) ← novo layer, override prioritário

AUTOR: Mountain Quest ATMOS LAB / DSS
═══════════════════════════════════════════════════════════════════════════
"""

import gpxpy
import gpxpy.gpx
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import requests
import datetime
import os
import ftplib
import folium
import math
import json
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import time

print("📡 INICIANDO SISTEMA V20.0 CONVECTION UPDATE...")

# ═══════════════════════════════════════════════════════════════════════════
# API CREDENTIALS
# ═══════════════════════════════════════════════════════════════════════════

NASA_EARTHDATA_TOKEN = os.environ.get("NASA_EARTHDATA_TOKEN", "")
AEMET_API_KEY = os.environ.get("AEMET_API_KEY", "")

# ═══════════════════════════════════════════════════════════════════════════
# AEMET HEALTH CHECK
# ═══════════════════════════════════════════════════════════════════════════

def check_aemet_freshness():
    try:
        with open('aemet_corrections.json') as f:
            data = json.load(f)
        for sector_name, sector_data in data.items():
            if 'last_update' in sector_data:
                ts = datetime.datetime.fromisoformat(sector_data['last_update'])
                age = datetime.datetime.utcnow() - ts
                age_hours = age.total_seconds() / 3600
                if age > datetime.timedelta(hours=48):
                    return {}, f"⚠️  AEMET corrections >48h old ({age_hours:.0f}h) - DISABLED"
                elif age > datetime.timedelta(hours=24):
                    return data, f"⚠️  AEMET corrections >24h old ({age_hours:.0f}h) - degraded"
                else:
                    return data, f"✅ AEMET corrections fresh ({age_hours:.0f}h old)"
        return data, "⚠️  AEMET corrections: no timestamp found"
    except FileNotFoundError:
        return {}, "⚠️  aemet_corrections.json not found - run aemet_calibration.py"
    except json.JSONDecodeError:
        return {}, "⚠️  aemet_corrections.json corrupted"
    except Exception as e:
        return {}, f"⚠️  AEMET check error: {str(e)[:50]}"

aemet_corrections, aemet_status = check_aemet_freshness()
print(aemet_status)
if aemet_corrections:
    print(f"   📡 AEMET sectors loaded: {', '.join(aemet_corrections.keys())}")

# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO EEI v3.1 — INTACTO
# ═══════════════════════════════════════════════════════════════════════════

class EEI_v31:
    MU = 0.6
    V_RIDER = 16
    V_EFF_MIN = 4.8
    T_THRESHOLD = 20
    LAMBDA_BASE = 0.3
    LAMBDA_HR = 0.4
    R_THRESHOLD = 0.5
    ALPHA = 0.007

    @staticmethod
    def calcular_elevacion_solar(lat, lon, timestamp):
        jd = (timestamp.timestamp() / 86400.0) + 2440587.5
        jc = (jd - 2451545.0) / 36525.0
        l0 = (280.46646 + jc * (36000.76983 + jc * 0.0003032)) % 360
        m = 357.52911 + jc * (35999.05029 - 0.0001537 * jc)
        m_rad = math.radians(m)
        c = (math.sin(m_rad) * (1.914602 - jc * (0.004817 + 0.000014 * jc)) +
             math.sin(2 * m_rad) * (0.019993 - 0.000101 * jc) +
             math.sin(3 * m_rad) * 0.000289)
        true_long = l0 + c
        omega = 125.04 - 1934.136 * jc
        app_long = true_long - 0.00569 - 0.00478 * math.sin(math.radians(omega))
        e0 = 23.0 + (26.0 + ((21.448 - jc * (46.8150 + jc * (0.00059 - jc * 0.001813))) / 60.0)) / 60.0
        e = e0 + 0.00256 * math.cos(math.radians(omega))
        dec = math.degrees(math.asin(math.sin(math.radians(e)) * math.sin(math.radians(app_long))))
        y = math.tan(math.radians(e / 2)) ** 2
        eq_time = 4 * math.degrees(
            y * math.sin(2 * math.radians(l0)) -
            2 * 0.016708634 * math.sin(m_rad) +
            4 * 0.016708634 * y * math.sin(m_rad) * math.cos(2 * math.radians(l0)) -
            0.5 * y * y * math.sin(4 * math.radians(l0)) -
            1.25 * 0.016708634 * 0.016708634 * math.sin(2 * m_rad)
        )
        time_offset = eq_time + 4 * lon
        tst = timestamp.hour * 60 + timestamp.minute + timestamp.second / 60 + time_offset
        ha = (tst / 4) - 180
        lat_rad = math.radians(lat)
        dec_rad = math.radians(dec)
        ha_rad = math.radians(ha)
        sin_elev = (math.sin(lat_rad) * math.sin(dec_rad) +
                    math.cos(lat_rad) * math.cos(dec_rad) * math.cos(ha_rad))
        return math.degrees(math.asin(sin_elev))

    @staticmethod
    def calcular(T_a, v_meteo, HR, R_rate, I_sol, lat, lon, timestamp):
        v_eff = (v_meteo * EEI_v31.MU) + EEI_v31.V_RIDER
        if v_eff < EEI_v31.V_EFF_MIN:
            T_wc = T_a
        else:
            v_exp = v_eff ** 0.16
            T_wc = 13.12 + 0.6215 * T_a - 11.37 * v_exp + 0.3965 * T_a * v_exp
        delta_rain = 1 if R_rate > EEI_v31.R_THRESHOLD else 0
        if delta_rain == 0:
            P_wet = 0.0
        else:
            factor = EEI_v31.LAMBDA_BASE + (EEI_v31.LAMBDA_HR * HR / 100)
            P_wet = max(0, (EEI_v31.T_THRESHOLD - T_a) * factor)
        h_sol = EEI_v31.calcular_elevacion_solar(lat, lon, timestamp)
        if h_sol <= 0:
            G_sol = 0.0
        else:
            G_sol = I_sol * EEI_v31.ALPHA * math.sin(math.radians(h_sol))
        EEI = T_wc - P_wet + G_sol
        if EEI > 15:
            estado = {'nivel': 'SAFE', 'color': '#2ecc71'}
        elif EEI > 10:
            estado = {'nivel': 'CAUTION', 'color': '#f1c40f'}
        elif EEI > 5:
            estado = {'nivel': 'WARNING', 'color': '#e67e22'}
        elif EEI > 0:
            estado = {'nivel': 'DANGER', 'color': '#e74c3c'}
        else:
            estado = {'nivel': 'CRITICAL', 'color': '#8b0000'}
        componentes = {
            'T_wc': round(T_wc, 1), 'P_wet': round(P_wet, 1),
            'G_sol': round(G_sol, 1), 'h_sol': round(h_sol, 1), 'v_eff': round(v_eff, 1)
        }
        return round(EEI, 1), componentes, estado

# ═══════════════════════════════════════════════════════════════════════════
# FIX #5 + #6: CONVECTION LAYER — nuevo módulo
# ═══════════════════════════════════════════════════════════════════════════

def evaluate_convection(weathercode, cape, lifted_index, precip_rate):
    """
    Override prioritário sobre EEI térmico.
    Retorna (status_override, color_override, convection_level) o None si sin convección.

    Jerarquía:
      HAIL — DANGER   : code 96/99 (granizo confirmado por modelo)
      SEVERE STORM    : code 95/99 + CAPE > 2500
      THUNDER         : code 95-99 sin granizo
      CONVECTIVE RISK : CAPE > 1000 sin código de trovoada (célula en desarrollo)
      None            : sin convección detectada
    """
    has_thunder = 95 <= weathercode <= 99
    has_hail    = weathercode in [96, 99]
    cape = cape or 0
    li   = lifted_index or 0

    if has_hail:
        return "HAIL — DANGER", "#4a0000", "EXTREME"

    if has_thunder and cape > 2500:
        return "SEVERE STORM", "#8b0000", "SEVERE"

    if has_thunder:
        return "THUNDER", "#c0392b", "HIGH"

    # Célula convectiva em desenvolvimento sem código de trovoada ainda
    if cape > 2500 and li < -3:
        return "SEVERE CONVECTION", "#8b0000", "SEVERE"

    if cape > 1500 and li < -2:
        return "CONVECTIVE RISK", "#e74c3c", "MODERATE"

    if cape > 800:
        return "INSTABILITY", "#e67e22", "LOW"

    return None, None, "NONE"

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO
# ═══════════════════════════════════════════════════════════════════════════

OUTPUT_FOLDER = 'output/'
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

GPX_FILE = 'MQ_TRACK.gpx'

track_points = []
try:
    if os.path.exists(GPX_FILE):
        with open(GPX_FILE, 'r') as g:
            gpx = gpxpy.parse(g)
            for t in gpx.tracks:
                for s in t.segments:
                    for p in s.points:
                        track_points.append([p.latitude, p.longitude])
    else:
        track_points = [[41.27, -8.08], [41.27, -8.08]]
except:
    track_points = [[41.27, -8.08], [41.27, -8.08]]

# ═══════════════════════════════════════════════════════════════════════════
# SECTORES
# ═══════════════════════════════════════════════════════════════════════════

sectors = [
    {"id":1,"name":"AMARANTE",          "lat":41.2709,"lon":-8.0797,"alt":"65m",  "altitude_m":65,   "type":"FLAT",    "desc":"START / BASE"},
    {"id":2,"name":"S. DA ABOBOREIRA", "lat":41.1946,"lon":-8.0563,"alt":"760m", "altitude_m":760,  "type":"CLIMB",   "desc":"ENTRY POINT"},
    {"id":3,"name":"SERRA DO MARÃO",   "lat":41.2484,"lon":-7.8862,"alt":"1390m","altitude_m":1390, "type":"DESCEND", "desc":"HIGH MOUNTAIN"},
    {"id":4,"name":"GAVIÃO",           "lat":41.2777,"lon":-7.9462,"alt":"986m", "altitude_m":986,  "type":"CLIMB",   "desc":"THE FILTER"},
    {"id":5,"name":"SERRA DO ALVÃO",   "lat":41.3738,"lon":-7.8053,"alt":"1043m","altitude_m":1043, "type":"FLAT",    "desc":"PLATEAU"},
    {"id":6,"name":"SRA. GRAÇA",       "lat":41.4168,"lon":-7.9106,"alt":"950m", "altitude_m":950,  "type":"CLIMB",   "desc":"THE CLIMB"}
]

# ═══════════════════════════════════════════════════════════════════════════
# NASA POWER — PARALLEL
# ═══════════════════════════════════════════════════════════════════════════

def get_nasa_irradiance(lat, lon, date):
    date_str = date.strftime('%Y%m%d')
    try:
        url = "https://power.larc.nasa.gov/api/temporal/hourly/point"
        params = {
            'parameters': 'ALLSKY_SFC_SW_DWN', 'community': 'RE',
            'longitude': lon, 'latitude': lat,
            'start': date_str, 'end': date_str, 'format': 'JSON'
        }
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        if 'properties' not in data or 'parameter' not in data['properties']:
            return None
        hourly_dict = data['properties']['parameter']['ALLSKY_SFC_SW_DWN']
        hourly_list = []
        for h in range(24):
            key = f"{date_str}{h:02d}"
            value = hourly_dict.get(key, 0)
            hourly_list.append(max(0, value))
        return hourly_list
    except Exception:
        return None

def fetch_nasa_parallel(sectors, date):
    cache = {}
    def fetch_one(sec):
        try:
            data = get_nasa_irradiance(sec['lat'], sec['lon'], date)
            if data:
                return (sec['id'], data)
        except Exception:
            pass
        return None
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_one, s): s for s in sectors}
        for future in futures:
            try:
                result = future.result(timeout=10)
                if result:
                    cache[result[0]] = result[1]
            except (TimeoutError, Exception):
                pass
    return cache

# ═══════════════════════════════════════════════════════════════════════════
# WEATHER TEXT
# ═══════════════════════════════════════════════════════════════════════════

def get_weather_text_improved(code, temp, precip, snowfall):
    if snowfall > 0.1 or (temp <= 2 and precip > 0.5):
        if precip > 10: return "HEAVY SNOW"
        elif precip > 3: return "SNOW"
        else: return "LIGHT SNOW"
    if code == 0: return "CLEAR"
    if 1 <= code <= 3: return "CLOUDY"
    if code in [45, 48]: return "FOG"
    if 51 <= code <= 67: return "RAIN"
    if code in [71,73,75,77,85,86]: return "SNOW"
    if 80 <= code <= 82: return "SHOWERS"
    if code in [96, 99]: return "HAIL"
    if 95 <= code <= 99: return "THUNDER"
    return "OVCAST"

# ═══════════════════════════════════════════════════════════════════════════
# MICROCLIMA PORTUGAL
# ═══════════════════════════════════════════════════════════════════════════

def portugal_microclimate_adjust(sector, data):
    adjusted = data.copy()
    alt = sector['altitude_m']
    adjustments_log = []
    if sector['name'] in aemet_corrections:
        corr = aemet_corrections[sector['name']]
        if corr.get('temp_offset', 0) != 0:
            adjusted['temp'] += corr['temp_offset']
            adjustments_log.append(f"AEMET Temp {corr['temp_offset']:+.1f}°C")
        if corr.get('wind_factor', 1.0) != 1.0:
            original_wind = adjusted['wind']
            adjusted['wind'] *= corr['wind_factor']
            adjustments_log.append(f"AEMET Wind {original_wind:.1f}→{adjusted['wind']:.1f}")
    if alt > 1000:
        original_wind = adjusted['wind']
        adjusted['wind'] *= 1.60
        adjusted['temp'] -= 2
        adjustments_log.append(f"Ridge +60% wind ({original_wind:.1f}→{adjusted['wind']:.1f})")
    if 400 < alt < 800:
        adjusted['temp'] += 1.5
        adjustments_log.append("Valley +1.5°C")
    if alt < 800 and adjusted.get('hum', 0) > 85 and 8 <= adjusted.get('temp', 0) <= 12:
        adjusted['fog_alert'] = "NEBLINA DO NORTE"
        adjustments_log.append("Fog alert")
    if adjusted.get('rain', 0) > 5:
        adjusted['mtb_hazard'] = "TRAZADOS RESBALADIZOS"
        adjustments_log.append("MTB hazard")
    if adjustments_log:
        print(f"🔧 {sector['name']:20} | {' | '.join(adjustments_log)}")
    return adjusted

# ═══════════════════════════════════════════════════════════════════════════
# FIX #6: GET HOURLY DATA — agora inclui CAPE e Lifted Index
# ═══════════════════════════════════════════════════════════════════════════

def get_hourly_data(r, sec, h, nasa_irradiance_cache):
    h = min(h, 47)
    d = {
        'temp':          r['hourly']['temperature_2m'][h],
        'wind':          r['hourly']['windspeed_10m'][h],
        'rain':          r['hourly']['precipitation'][h],
        'hum':           r['hourly']['relativehumidity_2m'][h],
        'code':          r['hourly']['weathercode'][h],
        'irradiance':    r['hourly'].get('global_tilted_irradiance', [0]*48)[h],
        'snowfall':      r['hourly'].get('snowfall', [0]*48)[h],
        'freezing_level':r['hourly'].get('freezing_level_height', [9999]*48)[h],
        # FIX #6: novos campos convectivos
        'cape':          r['hourly'].get('cape', [0]*48)[h],
        'lifted_index':  r['hourly'].get('lifted_index', [0]*48)[h],
    }
    if sec['id'] in nasa_irradiance_cache and h < len(nasa_irradiance_cache[sec['id']]):
        d['irradiance'] = nasa_irradiance_cache[sec['id']][h]
        d['irradiance_source'] = 'NASA_POWER'
    else:
        d['irradiance_source'] = 'OPEN_METEO'
    return portugal_microclimate_adjust(sec, d)

# ═══════════════════════════════════════════════════════════════════════════
# CARD GENERATOR — FIX #5: convection override prioritário
# ═══════════════════════════════════════════════════════════════════════════

def generate_ui_card(sector, data_now, data_3h, data_6h, time_str):
    now_ts = datetime.datetime.utcnow()

    eei_now, comp_now, estado_now = EEI_v31.calcular(
        data_now['temp'], data_now['wind'], data_now['hum'],
        data_now['rain'], data_now['irradiance'],
        sector['lat'], sector['lon'], now_ts
    )
    eei_3h, _, _ = EEI_v31.calcular(
        data_3h['temp'], data_3h['wind'], data_3h['hum'],
        data_3h['rain'], data_3h['irradiance'],
        sector['lat'], sector['lon'], now_ts + datetime.timedelta(hours=3)
    )
    eei_6h, _, _ = EEI_v31.calcular(
        data_6h['temp'], data_6h['wind'], data_6h['hum'],
        data_6h['rain'], data_6h['irradiance'],
        sector['lat'], sector['lon'], now_ts + datetime.timedelta(hours=6)
    )

    status = estado_now['nivel']
    color  = estado_now['color']
    convection_level = "NONE"
    is_snow  = False
    snow_intensity = "LIGHT"
    is_mixed = False

    # ── FIX #5: CONVECTION LAYER — override PRIORITÁRIO, avalia primeiro ──
    conv_status, conv_color, convection_level = evaluate_convection(
        data_now['code'],
        data_now.get('cape', 0),
        data_now.get('lifted_index', 0),
        data_now['rain']
    )
    if conv_status is not None:
        status = conv_status
        color  = conv_color
    # ── Fim convection layer ──

    # Snow / mixed logic (só actua se convection não fez override)
    elif 0 < data_now['temp'] <= 3 and data_now['rain'] > 0.1:
        is_mixed = True
        if data_now['temp'] <= 1.5:
            status = "LIKELY SNOW"; color = "#3498db"
        else:
            status = "MIXED PRECIP"; color = "#e67e22"
    elif data_now['rain'] > 0.1 and data_now['temp'] <= 1:
        is_snow = True
        if data_now['rain'] > 10:   snow_intensity = "HEAVY"
        elif data_now['rain'] > 3:  snow_intensity = "MODERATE"
        else:                        snow_intensity = "LIGHT"
    elif sector['altitude_m'] > data_now['freezing_level'] and data_now['freezing_level'] < 9000:
        if data_now['rain'] > 0.1:
            is_snow = True
            if data_now['rain'] > 10:   snow_intensity = "HEAVY"
            elif data_now['rain'] > 3:  snow_intensity = "MODERATE"
            else:                        snow_intensity = "LIGHT"

    if is_snow:
        if snow_intensity == "LIGHT":
            status = "SNOW ALERT";   color = "#f1c40f"
        elif snow_intensity == "MODERATE":
            status = "SNOW WARNING"; color = "#e67e22"
        else:
            status = "BLIZZARD";     color = "#e74c3c"

    def get_arrow(curr, fut):
        diff = fut - curr
        if diff < -2: return "(-)"
        if diff > 2:  return "(+)"
        return "(=)"

    arrow_3h = get_arrow(eei_now, eei_3h)
    arrow_6h = get_arrow(eei_now, eei_6h)

    fig, ax = plt.subplots(figsize=(6, 3.4), facecolor='#0f172a')
    ax.set_facecolor('#0f172a')
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

    rect = patches.Rectangle((0,0), 0.03, 1, transform=ax.transAxes,
                              linewidth=0, facecolor=color)
    ax.add_patch(rect)

    plt.text(0.08, 0.80, sector['name'], color='white',
             fontsize=16, fontweight='bold', transform=ax.transAxes)
    plt.text(0.08, 0.68, f"{sector['desc']} | {sector['alt']}",
             color='#94a3b8', fontsize=8, fontweight='bold', transform=ax.transAxes)

    # Watermark
    if convection_level in ["EXTREME", "SEVERE"]:
        wm = "⚡ DANGER" if "HAIL" not in status else "⚡ HAIL"
        plt.text(0.5, 0.40, wm, color='white', alpha=0.12,
                 fontsize=38, fontweight='bold', ha='center', transform=ax.transAxes)
    elif convection_level in ["HIGH", "MODERATE"]:
        plt.text(0.5, 0.40, "⚡ THUNDER", color='white', alpha=0.10,
                 fontsize=32, fontweight='bold', ha='center', transform=ax.transAxes)
    elif is_snow or is_mixed:
        wm = "SNOW" if is_snow else "MIXED"
        plt.text(0.5, 0.40, wm, color='white', alpha=0.10,
                 fontsize=50, fontweight='bold', ha='center', transform=ax.transAxes)
    else:
        weather_label = get_weather_text_improved(
            data_now['code'], data_now['temp'], data_now['rain'], data_now['snowfall'])
        plt.text(0.08, 0.40, weather_label, color='white', alpha=0.10,
                 fontsize=40, fontweight='bold', transform=ax.transAxes)

    plt.text(0.92, 0.68, f"{int(data_now['temp'])}°", color='white',
             fontsize=38, fontweight='bold', ha='right', transform=ax.transAxes)

    mri_col = "#38bdf8" if eei_now < data_now['temp'] else "#fca5a5"
    if status in ["CRITICAL","DANGER","BLIZZARD","HAIL — DANGER","SEVERE STORM"]:
        mri_col = "#ffffff"
    plt.text(0.92, 0.55, f"MRI: {int(eei_now)}°", color=mri_col,
             fontsize=10, fontweight='bold', ha='right', transform=ax.transAxes)

    # FIX #5: mostrar CAPE no card quando relevante
    cape_val = data_now.get('cape', 0)
    if cape_val > 200:
        cape_col = "#e74c3c" if cape_val > 1000 else "#e67e22" if cape_val > 500 else "#f1c40f"
        plt.text(0.92, 0.45, f"CAPE {int(cape_val)} J/kg",
                 color=cape_col, fontsize=7, ha='right', transform=ax.transAxes)
    else:
        plt.text(0.92, 0.45, f"WIND {int(data_now['wind'])} km/h",
                 color='#94a3b8', fontsize=7, ha='right', transform=ax.transAxes)

    plt.text(0.08, 0.45, f"WIND {int(data_now['wind'])} km/h",
             color='#94a3b8', fontsize=7, ha='left', transform=ax.transAxes)

    bbox = dict(boxstyle="round,pad=0.4", fc=color, ec="none", alpha=0.9)
    plt.text(0.92, 0.25, f" {status} ", color='white', fontsize=9,
             ha='right', fontweight='bold', bbox=bbox, transform=ax.transAxes)

    plt.plot([0.05, 0.95], [0.15, 0.15], color='#334155',
             linewidth=1, transform=ax.transAxes)

    def get_precip_type(d, alt):
        if d['snowfall'] > 0.1 or (d['temp'] <= 2 and d['rain'] > 0.5): return "SNOW"
        elif 0 < d['temp'] <= 3 and d['rain'] > 0.1:
            return "SNOW?" if d['temp'] <= 1.5 else "MIXED"
        elif d['temp'] <= 1 and d['rain'] > 0.1: return "SNOW"
        elif alt > d['freezing_level'] and d['freezing_level'] < 9000 and d['rain'] > 0.1: return "SNOW"
        # FIX #5: incluir trovoada no forecast label
        elif 95 <= d['code'] <= 99: return "THUNDER" if d['code'] not in [96,99] else "HAIL"
        else: return get_weather_text_improved(d['code'], d['temp'], d['rain'], d['snowfall'])

    f_3h_type = get_precip_type(data_3h, sector['altitude_m'])
    f_6h_type = get_precip_type(data_6h, sector['altitude_m'])
    f_3h = f"+3H: {f_3h_type} {int(data_3h['temp'])}° {arrow_3h}"
    f_6h = f"+6H: {f_6h_type} {int(data_6h['temp'])}° {arrow_6h}"

    plt.text(0.05, 0.09, f_3h, color='#94a3b8', fontsize=9,
             fontweight='bold', ha='left', transform=ax.transAxes)
    plt.text(0.95, 0.09, f_6h, color='#94a3b8', fontsize=9,
             fontweight='bold', ha='right', transform=ax.transAxes)
    plt.text(0.5, 0.02, f"UPDATED: {time_str} (UTC) | MQ RIDER INDEX™ v3.1 + CONVECTION",
             color='#475569', fontsize=6, ha='center', transform=ax.transAxes)

    ax.axis('off')
    plt.savefig(f"{OUTPUT_FOLDER}MQ_SECTOR_{sector['id']}_STATUS.png",
                dpi=150, facecolor='#0f172a')
    plt.close()

    return status, int(eei_now), data_now['wind'], is_snow or is_mixed, snow_intensity, eei_3h, eei_6h, convection_level, cape_val

# ═══════════════════════════════════════════════════════════════════════════
# BANNER — FIX #7: CAPE e trovoada no summary
# ═══════════════════════════════════════════════════════════════════════════

def generate_dashboard_banner(status, min_eei, max_wind, worst_sector, time_str, snow_detected, convection_detected, max_cape):
    fig, ax = plt.subplots(figsize=(8, 2.5), facecolor='#0a0a0a')
    ax.set_facecolor('#0a0a0a')
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

    color = "#2ecc71"
    if "INSTABILITY" in status or "SNOW ALERT" in status or "MIXED" in status or "LIKELY" in status:
        color = "#f1c40f"
    if "WARNING" in status or "CONVECTIVE" in status or "THUNDER" in status:
        color = "#e67e22"
    if "SEVERE" in status or "BLIZZARD" in status or "DANGER" in status:
        color = "#e74c3c"
    if "HAIL" in status or "CRITICAL" in status:
        color = "#8b0000"

    rect = patches.Rectangle((0,0), 0.015, 1, transform=ax.transAxes,
                              linewidth=0, facecolor=color)
    ax.add_patch(rect)

    ax_radar = fig.add_axes([0.05, 0.15, 0.20, 0.70])
    ax_radar.set_facecolor('#0a0a0a')
    lats = [p[0] for p in track_points]
    lons = [p[1] for p in track_points]
    ax_radar.plot(lons, lats, color=color, linewidth=1.2, alpha=0.9)
    ax_radar.set_aspect('equal')
    ax_radar.axis('off')
    ax_radar.add_patch(patches.Circle((0.5, 0.5), 0.48, transform=ax_radar.transAxes,
                                      fill=False, edgecolor='#333', linewidth=1, linestyle=':'))

    plt.text(0.28, 0.70, "MQ METEO STATION", color='white',
             fontsize=14, fontweight='bold', transform=ax.transAxes)

    if color == "#2ecc71":
        hook = "ALL SECTORS: GREEN LIGHT"
        sub  = f"UPDATED: {time_str} UTC | MULTI-SOURCE + CONVECTION LAYER"
    elif convection_detected:
        hook = f"⚡ CONVECTION: {worst_sector} | CAPE {int(max_cape)} J/kg"
        sub  = f"UPDATED: {time_str} UTC | CONVECTION LAYER ACTIVE"
    elif snow_detected:
        hook = f"SNOW ALERT: {worst_sector}"
        sub  = f"UPDATED: {time_str} UTC | PHYSICS-BASED"
    else:
        hook = f"WARNING: {worst_sector}"
        sub  = f"UPDATED: {time_str} UTC | MULTI-SOURCE"

    plt.text(0.28, 0.50, hook, color=color, fontsize=10,
             fontweight='bold', transform=ax.transAxes)
    plt.text(0.28, 0.35, sub, color='#888', fontsize=8, transform=ax.transAxes)
    plt.plot([0.68, 0.68], [0.2, 0.8], color='#222', linewidth=1, transform=ax.transAxes)

    plt.text(0.76, 0.70, "MIN MRI", color='#666', fontsize=7,
             ha='center', transform=ax.transAxes)
    eei_c = "#38bdf8" if min_eei < 10 else "white"
    plt.text(0.76, 0.45, f"{min_eei}°", color=eei_c, fontsize=20,
             fontweight='bold', ha='center', transform=ax.transAxes)

    plt.text(0.90, 0.70, "MAX WIND", color='#666', fontsize=7,
             ha='center', transform=ax.transAxes)
    wind_c = "#e67e22" if max_wind > 30 else "white"
    plt.text(0.90, 0.45, f"{int(max_wind)}", color=wind_c, fontsize=20,
             fontweight='bold', ha='center', transform=ax.transAxes)
    plt.text(0.90, 0.32, "km/h", color='#666', fontsize=7,
             ha='center', transform=ax.transAxes)

    # FIX #7: CAPE badge no banner
    if max_cape > 200:
        cape_col = "#e74c3c" if max_cape > 1000 else "#e67e22"
        bbox_cape = dict(boxstyle="round,pad=0.3", fc="#111", ec=cape_col, alpha=1.0)
        plt.text(0.50, 0.15, f"⚡ CAPE MAX: {int(max_cape)} J/kg", color=cape_col,
                 fontsize=7, ha='center', bbox=bbox_cape, transform=ax.transAxes)

    bbox_btn = dict(boxstyle="round,pad=0.3", fc="#111", ec="#333", alpha=1.0)
    plt.text(0.96, 0.10, " ▶ ACCEDER A METEO STATION ", color='#aaa',
             fontsize=7, ha='right', bbox=bbox_btn, transform=ax.transAxes)

    ax.axis('off')
    plt.savefig(f"{OUTPUT_FOLDER}MQ_HOME_BANNER.png", facecolor='#0a0a0a', dpi=150)
    plt.close()

def generate_map():
    print("🗺️ GENERANDO MAPA...")
    center = track_points[len(track_points)//2] if len(track_points) > 10 else [41.30, -7.95]
    m = folium.Map(location=center, zoom_start=10, tiles='CartoDB dark_matter')
    folium.PolyLine(track_points, color="#00f2ff", weight=3,
                    opacity=0.9, tooltip="MQ TRACK").add_to(m)
    for s in sectors:
        popup = f"<b>{s['name']}</b><br>Alt: {s['alt']}<br>Type: {s['type']}"
        folium.CircleMarker([s['lat'], s['lon']], radius=6, color="#ff9900",
                            fill=True, fill_color="#ff9900", fill_opacity=0.9,
                            popup=popup, tooltip=s['name']).add_to(m)
    m.save(f"{OUTPUT_FOLDER}MQ_TACTICAL_MAP_CALIBRATED.html")

# ═══════════════════════════════════════════════════════════════════════════
# EXECUÇÃO PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════

print("\n🚀 OBTENIENDO DATOS METEOROLÓGICOS (MULTI-SOURCE + PARALLEL + CONVECTION)...")
now = datetime.datetime.now()
time_str = now.strftime("%H:%M")
current_hour = now.hour

worst_status = "STABLE"
worst_sector = ""
g_min_eei = 99
g_max_wind = 0
g_max_cape = 0
snow_detected       = False
convection_detected = False
json_sectors = []

print("☀️  Obteniendo irradiância NASA POWER (parallel)...")
start_time = time.time()
nasa_irradiance_cache = fetch_nasa_parallel(sectors, now)
elapsed = time.time() - start_time
nasa_success = len(nasa_irradiance_cache)
print(f"   ✅ NASA POWER: {nasa_success}/{len(sectors)} sectores OK ({elapsed:.1f}s)")

print("\n🌍 Procesando sectores...")
for sec in sectors:
    try:
        # FIX #6: adicionar cape e lifted_index ao request Open-Meteo
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={sec['lat']}&longitude={sec['lon']}"
            f"&hourly=temperature_2m,windspeed_10m,weathercode,precipitation,"
            f"relativehumidity_2m,global_tilted_irradiance,snowfall,"
            f"freezing_level_height,cape,lifted_index"
            f"&forecast_days=2"
        )
        r = requests.get(url, timeout=10).json()

        d_now = get_hourly_data(r, sec, current_hour,     nasa_irradiance_cache)
        d_3h  = get_hourly_data(r, sec, current_hour + 3, nasa_irradiance_cache)
        d_6h  = get_hourly_data(r, sec, current_hour + 6, nasa_irradiance_cache)

        # Freezing level sanity check
        if d_now['temp'] != 0:
            expected_fl = sec['altitude_m'] + (d_now['temp'] / 0.0065)
            if abs(d_now['freezing_level'] - expected_fl) > 500:
                d_now['freezing_level'] = expected_fl
                d_3h['freezing_level']  = expected_fl
                d_6h['freezing_level']  = expected_fl

        stat, eei_val, wind_val, is_snow, snow_int, eei_3h_val, eei_6h_val, conv_level, cape_val = generate_ui_card(
            sec, d_now, d_3h, d_6h, time_str
        )

        if eei_val < g_min_eei:    g_min_eei = eei_val
        if wind_val > g_max_wind:  g_max_wind = wind_val
        if cape_val > g_max_cape:  g_max_cape = cape_val
        if is_snow:                snow_detected = True

        # FIX #7: convection_detected flag para banner
        if conv_level not in ["NONE", None]:
            convection_detected = True

        if any(x in stat for x in ["ALERT","SNOW","WARNING","MIXED","LIKELY","THUNDER","STORM","HAIL","CONVECT","INSTAB"]):
            worst_status = stat
            worst_sector = sec['name']

        conv_marker = f"⚡[{conv_level}]" if conv_level != "NONE" else ""
        snow_marker = f"❄[{snow_int}]" if is_snow else ""
        fog_marker  = "🌫️" if d_now.get('fog_alert') else ""
        cape_marker = f"CAPE:{int(cape_val)}" if cape_val > 200 else ""
        print(f"✅ {sec['name']:20} | MRI:{eei_val:3d}° {conv_marker} {snow_marker} {fog_marker} {cape_marker}")

    except Exception as e:
        print(f"❌ {sec['name']:20} | {str(e)[:60]}")
        json_sectors.append({"id": sec['id'], "name": sec['name'], "error": str(e)[:60]})
        continue

    json_sectors.append({
        "id": sec['id'],
        "name": sec['name'],
        "coords": {"lat": sec['lat'], "lon": sec['lon'], "elevation": sec['altitude_m']},
        "terrain_type": sec['type'],
        "description": sec['desc'],
        "current": {
            "eei": eei_val,
            "status": stat,
            "temp": round(d_now['temp'], 1),
            "wind": round(d_now['wind'], 1),
            "rain": round(d_now['rain'], 1),
            "humidity": d_now['hum'],
            "irradiance": round(d_now['irradiance'], 1),
            "irradiance_source": d_now.get('irradiance_source', 'OPEN_METEO'),
            "freezing_level": round(d_now['freezing_level'], 0),
            "snow_detected": is_snow,
            "snow_intensity": snow_int if is_snow else None,
            # FIX #6 + #7: campos convectivos no JSON
            "cape": round(cape_val, 0),
            "lifted_index": round(d_now.get('lifted_index', 0), 1),
            "convection_level": conv_level,
            "microclimate_notes": d_now.get('fog_alert') or d_now.get('mtb_hazard') or None,
            "aemet_calibrated": sec['name'] in aemet_corrections
        },
        "forecast_3h": {
            "eei": round(eei_3h_val, 1),
            "temp": round(d_3h['temp'], 1),
            "wind": round(d_3h['wind'], 1),
            "rain": round(d_3h['rain'], 1),
            "cape": round(d_3h.get('cape', 0), 0)
        },
        "forecast_6h": {
            "eei": round(eei_6h_val, 1),
            "temp": round(d_6h['temp'], 1),
            "wind": round(d_6h['wind'], 1),
            "rain": round(d_6h['rain'], 1),
            "cape": round(d_6h.get('cape', 0), 0)
        }
    })

generate_dashboard_banner(worst_status, g_min_eei, g_max_wind, worst_sector,
                          time_str, snow_detected, convection_detected, g_max_cape)
generate_map()

# ═══════════════════════════════════════════════════════════════════════════
# JSON API
# ═══════════════════════════════════════════════════════════════════════════

print("\n📊 GENERANDO JSON API...")
status_data = {
    "timestamp_utc": now.isoformat(),
    "last_update": time_str,
    "event": "MQ2026",
    "model_version": "MQ Rider Index v3.1 + Convection Layer | V20.0",
    "data_sources": [
        "ECMWF (via Open-Meteo) - TIER 1",
        "NASA POWER (parallel execution) - TIER 2",
        f"AEMET (calibration {len(aemet_corrections)} sectors) - TIER 3",
        "Portugal Microclimate Algorithms - TIER 4",
        "Convection Layer (CAPE + LI + weathercode) - TIER 0 OVERRIDE"
    ],
    "api_version": "v20.0_convection",
    "summary": {
        "alert_level": worst_status,
        "worst_sector": worst_sector if worst_sector else "ALL SECTORS",
        "min_mri": g_min_eei,
        "max_wind": round(g_max_wind, 1),
        "max_cape": round(g_max_cape, 0),
        "snow_detected": snow_detected,
        "convection_detected": convection_detected,
        "nasa_power_active": nasa_success > 0,
        "nasa_power_coverage": f"{nasa_success}/{len(sectors)}",
        "aemet_calibrated_sectors": len(aemet_corrections),
        "aemet_status": aemet_status
    },
    "sectors": json_sectors,
    "convection_thresholds": {
        "HAIL_DANGER":       "weathercode 96/99",
        "SEVERE_STORM":      "weathercode 95-99 + CAPE > 2500 J/kg",
        "THUNDER":           "weathercode 95-99",
        "SEVERE_CONVECTION": "CAPE > 2500 + LI < -3",
        "CONVECTIVE_RISK":   "CAPE > 1500 + LI < -2",
        "INSTABILITY":       "CAPE > 800"
    },
    "usage": {
        "ghost_rider":  "Use sectors[].current.eei to adjust pace per sector",
        "bios":         "Use sectors[].current.eei + convection_level as stress multiplier",
        "convection":   "Check sectors[].current.convection_level before route decisions",
        "endpoint":     "https://mountainquest.pt/atmos/MQ_ATMOS_STATUS.json"
    }
}

try:
    json_path = f"{OUTPUT_FOLDER}MQ_ATMOS_STATUS.json"
    with open(json_path, 'w') as f:
        json.dump(status_data, f, indent=2)
    print(f"✅ JSON gerado: {json_path}")
except Exception as e:
    print(f"⚠️  JSON error: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# FTP
# ═══════════════════════════════════════════════════════════════════════════

print("\n" + "─"*70)
print("🚀 SUBINDO AO FTP...")
print("─"*70)

FTP_HOST = "ftp.nexplore.pt"

if "FTP_USER" in os.environ:
    FTP_USER = os.environ["FTP_USER"]
    FTP_PASS = os.environ["FTP_PASS"]
    try:
        session = ftplib.FTP()
        session.connect(FTP_HOST, 21, timeout=30)
        session.login(FTP_USER, FTP_PASS)
        session.set_pasv(True)
        print(f"📍 Conectado: {session.pwd()}")
        def upload(local, remote):
            if os.path.exists(local):
                with open(local, 'rb') as f:
                    session.storbinary(f'STOR {remote}', f)
                print(f"   ✓ {remote}")
            else:
                print(f"   ⚠️  {local} não existe")
        upload(f"{OUTPUT_FOLDER}MQ_HOME_BANNER.png",                 "MQ_HOME_BANNER.png")
        upload(f"{OUTPUT_FOLDER}MQ_TACTICAL_MAP_CALIBRATED.html",    "MQ_TACTICAL_MAP_CALIBRATED.html")
        upload(f"{OUTPUT_FOLDER}MQ_ATMOS_STATUS.json",               "MQ_ATMOS_STATUS.json")
        for i in range(1, 7):
            upload(f"{OUTPUT_FOLDER}MQ_SECTOR_{i}_STATUS.png", f"MQ_SECTOR_{i}_STATUS.png")
        session.quit()
        print("\n✅ FTP COMPLETO")
        print("   📡 https://mountainquest.pt/atmos/MQ_ATMOS_STATUS.json")
    except Exception as e:
        print(f"\n❌ FTP ERROR: {e}")
else:
    print("⚠️  MODO LOCAL — FTP_USER não definido")

# ═══════════════════════════════════════════════════════════════════════════
# SUMÁRIO FINAL
# ═══════════════════════════════════════════════════════════════════════════

print("\n" + "═"*70)
print("🎯 BELLATOR V20.0 — CONVECTION UPDATE")
print("═"*70)
print(f"📊 Modelo: MQ Rider Index v3.1 + Convection Layer")
print(f"📅 Timestamp: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"🌡️  MIN MRI:   {g_min_eei}°C")
print(f"💨 MAX WIND:  {int(g_max_wind)} km/h")
print(f"⚡ MAX CAPE:  {int(g_max_cape)} J/kg {'⚠️  CONVECTION RISK' if g_max_cape > 800 else ''}")
print(f"❄️  SNOW:      {'YES' if snow_detected else 'NO'}")
print(f"⛈️  CONVECTION:{'YES' if convection_detected else 'NO'}")
print(f"☀️  NASA POWER:{nasa_success}/{len(sectors)} sectores ({elapsed:.1f}s)")
print(f"📡 AEMET:     {len(aemet_corrections)} sectores calibrados")
print(f"⚠️  Status:   {worst_status}")
print("═"*70 + "\n")
