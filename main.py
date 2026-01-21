"""
═══════════════════════════════════════════════════════════════════════════
MQ ATMOS LAB: BELLATOR V19.2.1 (PRODUCTION READY)
═══════════════════════════════════════════════════════════════════════════
CHANGELOG V19.2.1 (sobre V19.2):
✅ FIXED: ZoneInfo fallback para Python <3.9 (compatibilidad)
✅ KEEP: Todos los fixes V19.2 (microclima, irradiance, timezone)
✅ KEEP: EEI_v31 100% intacto

MODELO: EEI = T_wc - P_wet + G_sol
AUTOR: Mountain Quest ATMOS LAB
FECHA: Enero 2026
ESTADO: PRODUCTION READY
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
from datetime import timedelta

try:
    from zoneinfo import ZoneInfo
    ZONEINFO_AVAILABLE = True
except ImportError:
    ZONEINFO_AVAILABLE = False

print("📡 INICIANDO SISTEMA V19.2.1 (PRODUCTION READY)...")

# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO EEI v3.1 (EMBEBIDO) - INTACTO
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
        P_wet = 0.0
        if delta_rain == 1:
            factor = EEI_v31.LAMBDA_BASE + (EEI_v31.LAMBDA_HR * HR / 100)
            P_wet = max(0, (EEI_v31.T_THRESHOLD - T_a) * factor)
        h_sol = EEI_v31.calcular_elevacion_solar(lat, lon, timestamp)
        G_sol = 0.0 if h_sol <= 0 else I_sol * EEI_v31.ALPHA * math.sin(math.radians(h_sol))
        EEI = T_wc - P_wet + G_sol
        if EEI > 15:
            estado = {'nivel': 'SAFE', 'color': '#2ecc71'}
        elif EEI > 10:
            estado = {'nivel': 'MODERATE', 'color': '#f1c40f'}
        elif EEI > 5:
            estado = {'nivel': 'HIGH', 'color': '#e67e22'}
        elif EEI > 0:
            estado = {'nivel': 'DANGER', 'color': '#e74c3c'}
        else:
            estado = {'nivel': 'CRITICAL', 'color': '#8b0000'}
        componentes = {
            'T_wc': round(T_wc, 1),
            'P_wet': round(P_wet, 1),
            'G_sol': round(G_sol, 1),
            'h_sol': round(h_sol, 1),
            'v_eff': round(v_eff, 1)
        }
        return round(EEI, 1), componentes, estado

# CONFIGURACIÓN
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

sectors = [
    {"id":1,"name":"AMARANTE","lat":41.2709,"lon":-8.0797,"alt":"65m","altitude_m":65,"type":"FLAT","desc":"START / BASE"},
    {"id":2,"name":"S. DA ABOBOREIRA","lat":41.1946,"lon":-8.0563,"alt":"760m","altitude_m":760,"type":"CLIMB","desc":"ENTRY POINT"},
    {"id":3,"name":"SERRA DO MARÃO","lat":41.2484,"lon":-7.8862,"alt":"1415m","altitude_m":1415,"type":"DESCEND","desc":"HIGH MOUNTAIN"},
    {"id":4,"name":"GAVIÃO","lat":41.2777,"lon":-7.9462,"alt":"900m","altitude_m":900,"type":"CLIMB","desc":"THE FILTER"},
    {"id":5,"name":"SERRA DO ALVÃO","lat":41.3738,"lon":-7.8053,"alt":"1200m","altitude_m":1200,"type":"FLAT","desc":"PLATEAU"},
    {"id":6,"name":"SRA. GRAÇA","lat":41.4168,"lon":-7.9106,"alt":"950m","altitude_m":950,"type":"CLIMB","desc":"THE CLIMB"}
]

def get_weather_text(code):
    if code == 0: return "CLEAR"
    if 1 <= code <= 3: return "CLOUDY"
    if code in [45, 48]: return "FOG"
    if 51 <= code <= 67: return "RAIN"
    if code in [71,73,75,77,85,86]: return "SNOW"
    if 80 <= code <= 82: return "STORM"
    if 95 <= code <= 99: return "THUNDER"
    return "OVCAST"

def portugal_microclimate_adjust(sector, data, is_daily=False):
    adjusted = data.copy()
    alt = sector['altitude_m']

    if alt > 1000:
        original_wind = adjusted['wind']
        adjusted['wind'] *= 1.60
        print(f"🌬️ {sector['name']:20} | Wind {original_wind:.1f} → {adjusted['wind']:.1f} km/h (+60% ridge)")

    if 400 < alt < 800:
        adjusted['temp'] += 1.5
        if is_daily:
            adjusted['temp_max'] += 1.5
            adjusted['temp_min'] += 1.5
        print(f"🔥 {sector['name']:20} | Temp +1.5°C (valley inversion)")

    if alt < 800 and adjusted.get('hum', 0) > 85 and 8 <= adjusted.get('temp', 0) <= 12:
        adjusted['fog_alert'] = "NEBLINA DO NORTE - VISIBILIDAD BAJA"

    precip_key = 'precip' if 'precip' in adjusted else 'precipitation_sum'
    if precip_key in adjusted and adjusted[precip_key] > 5:
        adjusted['mtb_hazard'] = "TRAZADOS RESBALADIZOS - RIESGO DESCENSO"

    return adjusted

def generate_ui_card(sector, data_now, data_3h, data_6h, time_str):
    now_ts = datetime.datetime.utcnow().replace(tzinfo=None)

    eei_now, comp_now, estado_now = EEI_v31.calcular(
        data_now['temp'], data_now['wind'], data_now['hum'],
        data_now['rain'], data_now['irradiance'],
        sector['lat'], sector['lon'], now_ts
    )
    eei_3h, _, _ = EEI_v31.calcular(
        data_3h['temp'], data_3h['wind'], data_3h['hum'],
        data_3h['rain'], data_3h['irradiance'],
        sector['lat'], sector['lon'], now_ts + timedelta(hours=3)
    )
    eei_6h, _, _ = EEI_v31.calcular(
        data_6h['temp'], data_6h['wind'], data_6h['hum'],
        data_6h['rain'], data_6h['irradiance'],
        sector['lat'], sector['lon'], now_ts + timedelta(hours=6)
    )

    status = estado_now['nivel']
    color = estado_now['color']

    is_snow = False
    snow_intensity = "LIGHT"
    is_mixed = False
    watermark_base = ""

    if 0 < data_now['temp'] <= 3 and data_now['rain'] > 0.1:
        is_mixed = True
        if data_now['temp'] <= 1.5:
            status = "LIKELY SNOW"
            color = "#3498db"
            watermark_base = "SNOW PROBABLE"
        else:
            status = "MIXED PRECIP"
            color = "#e67e22"
            watermark_base = "RAIN/SNOW MIX"
    elif data_now['rain'] > 0.1 and data_now['temp'] <= 1:
        is_snow = True
        if data_now['rain'] > 10:
            snow_intensity = "HEAVY"
        elif data_now['rain'] > 3:
            snow_intensity = "MODERATE"
        else:
            snow_intensity = "LIGHT"
    elif sector['altitude_m'] > data_now['freezing_level'] and data_now['freezing_level'] < 9000:
        if data_now['rain'] > 0.1:
            is_snow = True
            if data_now['rain'] > 10:
                snow_intensity = "HEAVY"
            elif data_now['rain'] > 3:
                snow_intensity = "MODERATE"
            else:
                snow_intensity = "LIGHT"

    if is_snow:
        if snow_intensity == "LIGHT":
            status = "SNOW ALERT"
            color = "#f1c40f"
            watermark_base = "❄"
        elif snow_intensity == "MODERATE":
            status = "SNOW WARNING"
            color = "#e67e22"
            watermark_base = "SNOW"
        else:
            status = "BLIZZARD"
            color = "#e74c3c"
            watermark_base = "BLIZZARD"

    def get_arrow(curr, fut):
        diff = fut - curr
        if diff < -2: return "(-)"
        if diff > 2: return "(+)"
        return "(=)"

    arrow_3h = get_arrow(eei_now, eei_3h)
    arrow_6h = get_arrow(eei_now, eei_6h)

    fig, ax = plt.subplots(figsize=(6, 3.4), facecolor='#0f172a')
    ax.set_facecolor('#0f172a')
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

    rect = patches.Rectangle((0, 0), 0.03, 1, transform=ax.transAxes, linewidth=0, facecolor=color)
    ax.add_patch(rect)

    plt.text(0.08, 0.80, sector['name'], color='white', fontsize=16, fontweight='bold', transform=ax.transAxes)
    plt.text(0.08, 0.68, f"{sector['desc']} | {sector['alt']}", color='#94a3b8', fontsize=8, fontweight='bold', transform=ax.transAxes)

    if is_snow or is_mixed:
        plt.text(0.5, 0.40, watermark_base, color='white', alpha=0.10, fontsize=55, fontweight='bold', ha='center', transform=ax.transAxes)
    else:
        plt.text(0.08, 0.40, get_weather_text(data_now['code']), color='white', alpha=0.10, fontsize=40, fontweight='bold', transform=ax.transAxes)

    plt.text(0.92, 0.68, f"{int(data_now['temp'])}°", color='white', fontsize=38, fontweight='bold', ha='right', transform=ax.transAxes)

    mri_col = "#38bdf8" if eei_now < data_now['temp'] else "#fca5a5"
    if status in ["CRITICAL", "DANGER", "BLIZZARD"]:
        mri_col = "#ffffff"
    plt.text(0.92, 0.55, f"MRI: {int(eei_now)}°", color=mri_col, fontsize=10, fontweight='bold', ha='right', transform=ax.transAxes)

    plt.text(0.92, 0.45, f"WIND {int(data_now['wind'])} km/h", color='#94a3b8', fontsize=7, ha='right', transform=ax.transAxes)

    bbox = dict(boxstyle="round,pad=0.4", fc=color, ec="none", alpha=0.9)
    plt.text(0.92, 0.25, f" {status} ", color='white', fontsize=9, ha='right', fontweight='bold', bbox=bbox, transform=ax.transAxes)

    plt.plot([0.05, 0.95], [0.15, 0.15], color='#334155', linewidth=1, transform=ax.transAxes)

    def get_precip_type(d, alt):
        if 0 < d['temp'] <= 3 and d['rain'] > 0.1:
            return "SNOW?" if d['temp'] <= 1.5 else "MIXED"
        elif d['temp'] <= 1 and d['rain'] > 0.1:
            return "SNOW"
        elif alt > d['freezing_level'] and d['freezing_level'] < 9000 and d['rain'] > 0.1:
            return "SNOW"
        else:
            return get_weather_text(d['code'])

    f_3h_type = get_precip_type(data_3h, sector['altitude_m'])
    f_6h_type = get_precip_type(data_6h, sector['altitude_m'])
    f_3h = f"+3H: {f_3h_type} {int(data_3h['temp'])}° {arrow_3h}"
    f_6h = f"+6H: {f_6h_type} {int(data_6h['temp'])}° {arrow_6h}"

    plt.text(0.05, 0.09, f_3h, color='#94a3b8', fontsize=9, fontweight='bold', ha='left', transform=ax.transAxes)
    plt.text(0.95, 0.09, f_6h, color='#94a3b8', fontsize=9, fontweight='bold', ha='right', transform=ax.transAxes)

    plt.text(0.5, 0.02, f"UPDATED: {time_str} (UTC) | MQ RIDER INDEX™ v3.1", color='#475569', fontsize=6, ha='center', transform=ax.transAxes)

    ax.axis('off')
    plt.savefig(f"{OUTPUT_FOLDER}MQ_SECTOR_{sector['id']}_STATUS.png", dpi=150, facecolor='#0f172a')
    plt.close()

    return status, int(eei_now), data_now['wind'], is_snow or is_mixed, snow_intensity, eei_3h, eei_6h

def generate_dashboard_banner(status, min_eei, max_wind, worst_sector, time_str, snow_detected):
    fig, ax = plt.subplots(figsize=(8, 2.5), facecolor='#0a0a0a')
    ax.set_facecolor('#0a0a0a')
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
    color = "#2ecc71"
    if "ALERT" in status or "SNOW" in status or "MIXED" in status or "LIKELY" in status:
        color = "#f1c40f"
    if "HIGH" in status or "WARNING" in status:
        color = "#e67e22"
    if "CRITICAL" in status or "BLIZZARD" in status:
        color = "#e74c3c"
    rect = patches.Rectangle((0, 0), 0.015, 1, transform=ax.transAxes, linewidth=0, facecolor=color)
    ax.add_patch(rect)
    ax_radar = fig.add_axes([0.05, 0.15, 0.20, 0.70])
    ax_radar.set_facecolor('#0a0a0a')
    lats = [p[0] for p in track_points]
    lons = [p[1] for p in track_points]
    ax_radar.plot(lons, lats, color=color, linewidth=1.2, alpha=0.9)
    ax_radar.set_aspect('equal')
    ax_radar.axis('off')
    ax_radar.add_patch(patches.Circle((0.5, 0.5), 0.48, transform=ax_radar.transAxes, fill=False, edgecolor='#333', linewidth=1, linestyle=':'))
    plt.text(0.28, 0.70, "MQ METEO STATION", color='white', fontsize=14, fontweight='bold', transform=ax.transAxes)
    if color == "#2ecc71":
        hook = "ALL SECTORS: GREEN LIGHT"
        sub = f"UPDATED: {time_str} UTC | MQ RIDER INDEX™"
    elif snow_detected:
        hook = f"SNOW ALERT: {worst_sector}"
        sub = f"UPDATED: {time_str} UTC | PHYSICS-BASED"
    else:
        hook = f"WARNING: {worst_sector}"
        sub = f"UPDATED: {time_str} UTC | MQ RIDER INDEX™"
    plt.text(0.28, 0.50, hook, color=color, fontsize=10, fontweight='bold', transform=ax.transAxes)
    plt.text(0.28, 0.35, sub, color='#888', fontsize=8, transform=ax.transAxes)
    plt.plot([0.68, 0.68], [0.2, 0.8], color='#222', linewidth=1, transform=ax.transAxes)
    plt.text(0.76, 0.70, "MIN MRI", color='#666', fontsize=7, ha='center', transform=ax.transAxes)
    eei_c = "#38bdf8" if min_eei < 10 else "white"
    plt.text(0.76, 0.45, f"{min_eei}°", color=eei_c, fontsize=20, fontweight='bold', ha='center', transform=ax.transAxes)
    plt.text(0.90, 0.70, "MAX WIND", color='#666', fontsize=7, ha='center', transform=ax.transAxes)
    wind_c = "#e67e22" if max_wind > 30 else "white"
    plt.text(0.90, 0.45, f"{int(max_wind)}", color=wind_c, fontsize=20, fontweight='bold', ha='center', transform=ax.transAxes)
    plt.text(0.90, 0.32, "km/h", color='#666', fontsize=7, ha='center', transform=ax.transAxes)
    bbox_btn = dict(boxstyle="round,pad=0.3", fc="#111", ec="#333", alpha=1.0)
    plt.text(0.96, 0.10, " ▶ ACCEDER A METEO STATION ", color='#aaa', fontsize=7, ha='right', bbox=bbox_btn, transform=ax.transAxes)
    ax.axis('off')
    plt.savefig(f"{OUTPUT_FOLDER}MQ_HOME_BANNER.png", facecolor='#0a0a0a', dpi=150)
    plt.close()

def generate_map():
    print("🗺️ GENERANDO MAPA...")
    center = track_points[len(track_points)//2] if len(track_points) > 10 else [41.30, -7.95]
    m = folium.Map(location=center, zoom_start=10, tiles='CartoDB dark_matter')
    folium.PolyLine(track_points, color="#00f2ff", weight=3, opacity=0.9, tooltip="MQ TRACK").add_to(m)
    for s in sectors:
        popup = f"<b>{s['name']}</b><br>Alt: {s['alt']}<br>Type: {s['type']}"
        folium.CircleMarker([s['lat'], s['lon']], radius=6, color="#ff9900", fill=True, fill_color="#ff9900", fill_opacity=0.9, popup=popup, tooltip=s['name']).add_to(m)
    m.save(f"{OUTPUT_FOLDER}MQ_TACTICAL_MAP_CALIBRATED.html")

def generate_5day_forecast():
    print("\n" + "═"*70)
    print("📊 GENERANDO FORECAST 5 DÍAS V19.2.1...")
    print("═"*70)
    now_utc = datetime.datetime.utcnow()
    
    # FIX: Timezone handling con fallback
    if ZONEINFO_AVAILABLE:
        local_str = now_utc.astimezone(ZoneInfo("Europe/Lisbon")).isoformat()
    else:
        # Fallback manual: UTC+1 (Europe/Lisbon en invierno, UTC+0 en verano DST)
        # Simplificado: asumimos UTC+1 (invierno estándar Portugal)
        local_str = (now_utc + timedelta(hours=1)).isoformat() + "+01:00"
    
    forecast_data = {
        "generated_at": now_utc.isoformat() + "Z",
        "generated_at_local": local_str,
        "event": "Mountain Quest 2026",
        "event_date": "2026-06-20",
        "metadata": {
            "model": "MQ Rider Index v3.1 (Portugal Microclimate Tuned)",
            "forecast_days": 5,
            "data_sources": ["Open-Meteo (ECMWF IFS)", "Local Adjustments"],
            "version": "v19.2.1"
        },
        "days": []
    }

    for day_offset in range(5):
        date_utc = now_utc + timedelta(days=day_offset)
        day_label = "TODAY" if day_offset == 0 else f"D+{day_offset}"
        print(f" → {day_label} ({date_utc.strftime('%Y-%m-%d')})")
        day_data = {
            "date": date_utc.strftime("%Y-%m-%d"),
            "day_name": date_utc.strftime("%A"),
            "day_label": day_label,
            "sectors": []
        }
        for sec in sectors:
            try:
                url = f"https://api.open-meteo.com/v1/forecast?latitude={sec['lat']}&longitude={sec['lon']}&daily=temperature_2m_max,temperature_2m_min,windspeed_10m_max,precipitation_sum,relativehumidity_2m_mean,shortwave_radiation_sum&timezone=Europe%2FLisbon&forecast_days={day_offset+1}"
                r = requests.get(url, timeout=10).json()
                day_idx = day_offset

                temp_max = r['daily']['temperature_2m_max'][day_idx]
                temp_min = r['daily']['temperature_2m_min'][day_idx]
                temp_avg = (temp_max + temp_min) / 2
                wind_max = r['daily']['windspeed_10m_max'][day_idx]
                precip_sum = r['daily']['precipitation_sum'][day_idx]
                humidity = r['daily']['relativehumidity_2m_mean'][day_idx]
                radiation_sum = r['daily']['shortwave_radiation_sum'][day_idx] or 0

                # Irradiancia: MJ/m²/día → W/m² promedio 24h → ×3 para diurno aprox
                avg_24h = radiation_sum * 1_000_000 / 86_400 if radiation_sum > 0 else 0
                irradiance_avg = min(avg_24h * 3.0, 1000.0)

                precip_rate = precip_sum / 24 if precip_sum > 0 else 0
                precip_intensity = "LIGHT" if precip_sum < 5 else "MODERATE" if precip_sum < 15 else "HEAVY"

                daily_data = {
                    'temp': temp_avg,
                    'temp_max': temp_max,
                    'temp_min': temp_min,
                    'wind': wind_max,
                    'precip': precip_sum,
                    'hum': humidity,
                    'irradiance': irradiance_avg,
                    'precip_rate': precip_rate,
                    'precip_intensity': precip_intensity
                }

                adjusted = portugal_microclimate_adjust(sec, daily_data, is_daily=True)

                midday_ts = date_utc.replace(hour=12, minute=0, second=0)
                eei, _, estado = EEI_v31.calcular(
                    adjusted['temp'], adjusted['wind'], adjusted['hum'],
                    adjusted['precip_rate'], adjusted['irradiance'],
                    sec['lat'], sec['lon'], midday_ts
                )

                status = estado['nivel']
                status_class = status.lower()
                notes = []
                if 'fog_alert' in adjusted:
                    notes.append(adjusted['fog_alert'])
                    status += " + FOG"
                if 'mtb_hazard' in adjusted:
                    notes.append(adjusted['mtb_hazard'])
                    status += " + MTB HAZARD"

                sector_forecast = {
                    "name": sec['name'],
                    "km": sec['type'],
                    "elevation": sec['altitude_m'],
                    "temp": round(adjusted['temp'], 1),
                    "temp_max": round(adjusted['temp_max'], 1),
                    "temp_min": round(adjusted['temp_min'], 1),
                    "wind_speed": round(adjusted['wind'], 1),
                    "precipitation_sum": round(adjusted['precip'], 1),
                    "precipitation_rate_mmh": round(adjusted['precip_rate'], 2),
                    "precipitation_intensity": adjusted['precip_intensity'],
                    "humidity": round(adjusted['hum'], 1),
                    "irradiance_avg_wm2": round(adjusted['irradiance'], 1),
                    "eei": round(eei, 1),
                    "status": status,
                    "status_class": status_class,
                    "microclimate_notes": " | ".join(notes) if notes else None
                }
                day_data["sectors"].append(sector_forecast)

                print(f"   {sec['name']:20} | EEI {eei:5.1f} | Irrad {irradiance_avg:4.0f} W/m² | Wind {adjusted['wind']:5.1f}")

            except Exception as e:
                print(f" ⚠️ {sec['name']} D+{day_offset}: {str(e)[:60]}")
                day_data["sectors"].append({
                    "name": sec['name'],
                    "km": sec['type'],
                    "elevation": sec['altitude_m'],
                    "temp": 10.0,
                    "temp_max": 15.0,
                    "temp_min": 5.0,
                    "wind_speed": 20.0,
                    "precipitation_sum": 0.0,
                    "precipitation_rate_mmh": 0.0,
                    "precipitation_intensity": "NONE",
                    "humidity": 70.0,
                    "irradiance_avg_wm2": 400.0,
                    "eei": 8.0,
                    "status": "MODERATE",
                    "status_class": "moderate",
                    "microclimate_notes": "FALLBACK - API ERROR"
                })

        forecast_data["days"].append(day_data)

    json_path = f"{OUTPUT_FOLDER}forecast_5day.json"
    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(forecast_data, f, indent=2, ensure_ascii=False)
        print(f"\n✅ forecast_5day.json OK ({os.path.getsize(json_path)} bytes)")
        return json_path
    except Exception as e:
        print(f"⚠️ Error JSON forecast: {e}")
        return None

# EJECUCIÓN PRINCIPAL
print("🚀 OBTENIENDO DATOS OPEN-METEO...")
now_utc = datetime.datetime.utcnow()
time_str = now_utc.strftime("%H:%M")
current_hour = now_utc.hour
worst_status = "STABLE"
worst_sector = ""
g_min_eei = 99
g_max_wind = 0
snow_detected = False
json_sectors = []

for sec in sectors:
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={sec['lat']}&longitude={sec['lon']}&hourly=temperature_2m,windspeed_10m,weathercode,precipitation,relativehumidity_2m,global_tilted_irradiance,snowfall,freezing_level_height&timezone=Europe%2FLisbon&forecast_days=2"
        r = requests.get(url, timeout=10).json()

        def get_data(h):
            d = {
                'temp': r['hourly']['temperature_2m'][h],
                'wind': r['hourly']['windspeed_10m'][h],
                'rain': r['hourly']['precipitation'][h],
                'hum': r['hourly']['relativehumidity_2m'][h],
                'code': r['hourly']['weathercode'][h],
                'irradiance': r['hourly'].get('global_tilted_irradiance', [0]*48)[h],
                'snowfall': r['hourly'].get('snowfall', [0]*48)[h],
                'freezing_level': r['hourly'].get('freezing_level_height', [9999]*48)[h]
            }
            return portugal_microclimate_adjust(sec, d)

        d_now = get_data(current_hour)
        d_3h = get_data(current_hour + 3)
        d_6h = get_data(current_hour + 6)

        if d_now['temp'] != 0:
            expected_fl = sec['altitude_m'] + (d_now['temp'] / 0.0065)
            if abs(d_now['freezing_level'] - expected_fl) > 500:
                delta = d_now['freezing_level'] - expected_fl
                print(f"⚠️ {sec['name']:20} FL API {int(d_now['freezing_level'])}m vs Expected {int(expected_fl)}m (Δ{int(delta):+d}m) → corregido")
                d_now['freezing_level'] = expected_fl
                d_3h['freezing_level'] = expected_fl + (d_3h['temp'] - d_now['temp']) / 0.0065
                d_6h['freezing_level'] = expected_fl + (d_6h['temp'] - d_now['temp']) / 0.0065

        stat, eei_val, wind_val, is_snow, snow_int, eei_3h_val, eei_6h_val = generate_ui_card(
            sec, d_now, d_3h, d_6h, time_str
        )

        if eei_val < g_min_eei:
            g_min_eei = eei_val
        if wind_val > g_max_wind:
            g_max_wind = wind_val
        if is_snow:
            snow_detected = True
        if any(x in stat for x in ["ALERT", "SNOW", "HIGH", "MIXED", "LIKELY"]):
            worst_status = stat
            worst_sector = sec['name']

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
                "freezing_level": round(d_now['freezing_level'], 0),
                "snow_detected": is_snow,
                "snow_intensity": snow_int if is_snow else None,
                "microclimate_notes": d_now.get('fog_alert') or d_now.get('mtb_hazard') or None
            },
            "forecast_3h": {
                "eei": round(eei_3h_val, 1),
                "temp": round(d_3h['temp'], 1),
                "wind": round(d_3h['wind'], 1),
                "rain": round(d_3h['rain'], 1)
            },
            "forecast_6h": {
                "eei": round(eei_6h_val, 1),
                "temp": round(d_6h['temp'], 1),
                "wind": round(d_6h['wind'], 1),
                "rain": round(d_6h['rain'], 1)
            }
        })

        snow_marker = f"❄ [{snow_int}]" if is_snow else ""
        fog_marker = f"🌫️ {d_now.get('fog_alert', '')[:15]}" if d_now.get('fog_alert') else ""
        print(f"✅ {sec['name']:20} | MRI {eei_val:3d}° {snow_marker} {fog_marker}")

    except Exception as e:
        print(f"❌ {sec['name']:20} | {str(e)[:50]}")

generate_dashboard_banner(worst_status, g_min_eei, g_max_wind, worst_sector, time_str, snow_detected)
generate_map()

print("\n📊 GENERANDO MQ_ATMOS_STATUS.JSON...")
status_data = {
    "timestamp_utc": now_utc.isoformat() + "Z",
    "last_update": time_str,
    "event": "MQ2026",
    "model_version": "v3.1 + Portugal Microclimate v19.2.1",
    "data_sources": ["ECMWF via Open-Meteo", "Local Tuning"],
    "api_version": "v19.2.1",
    "summary": {
        "alert_level": worst_status,
        "worst_sector": worst_sector or "ALL SECTORS",
        "min_mri": g_min_eei,
        "max_wind": round(g_max_wind, 1),
        "snow_detected": snow_detected
    },
    "sectors": json_sectors,
    "microclimate_adjustments": [
        "Ridge wind boost +60% >1000m",
        "Valley inversion +1.5°C (400-800m)",
        "Neblina do Norte fog detection",
        "MTB hazard >5mm/day"
    ],
    "surgical_fixes": [
        "Physics snow detection",
        "Mixed precip 0-3°C",
        "Ridge wind boost",
        "Freezing level validation"
    ],
    "usage": {
        "endpoint": "https://mountainquest.pt/atmos/MQ_ATMOS_STATUS.json",
        "forecast": "https://mountainquest.pt/atmos/forecast_5day.json"
    }
}
json_path_status = f"{OUTPUT_FOLDER}MQ_ATMOS_STATUS.json"
with open(json_path_status, 'w') as f:
    json.dump(status_data, f, indent=2)
print(f"✅ MQ_ATMOS_STATUS.json generado")

forecast_json_path = generate_5day_forecast()

print("\n" + "─"*70)
print("🚀 SUBIENDO A FTP...")
print("─"*70)
FTP_HOST = "ftp.nexplore.pt"
if "FTP_USER" in os.environ and "FTP_PASS" in os.environ:
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
                print(f" ✓ {remote}")
            else:
                print(f" ⚠️ {local} NO existe")

        upload(f"{OUTPUT_FOLDER}MQ_HOME_BANNER.png", "MQ_HOME_BANNER.png")
        upload(f"{OUTPUT_FOLDER}MQ_TACTICAL_MAP_CALIBRATED.html", "MQ_TACTICAL_MAP_CALIBRATED.html")
        upload(json_path_status, "MQ_ATMOS_STATUS.json")
        for i in range(1, 7):
            upload(f"{OUTPUT_FOLDER}MQ_SECTOR_{i}_STATUS.png", f"MQ_SECTOR_{i}_STATUS.png")
        if forecast_json_path and os.path.exists(forecast_json_path):
            upload(forecast_json_path, "forecast_5day.json")

        session.quit()
        print("\n✅ FTP COMPLETADO")
        print(" 📡 https://mountainquest.pt/atmos/MQ_ATMOS_STATUS.json")
        if forecast_json_path:
            print(" 📊 https://mountainquest.pt/atmos/forecast_5day.json")
    except Exception as e:
        print(f"❌ FTP ERROR: {e}")
else:
    print("⚠️ MODO LOCAL - sin FTP")

print("\n" + "═"*70)
print("🎯 BELLATOR V19.2.1 (PRODUCTION READY) COMPLETADO")
print(f"🌡️ MIN MRI: {g_min_eei}°C | 💨 MAX WIND: {int(g_max_wind)} km/h | ❄️ SNOW: {'YES' if snow_detected else 'NO'}")
print("═"*70 + "\n")
