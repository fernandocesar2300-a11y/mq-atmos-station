# -*- coding: utf-8 -*-
# === MQ ATMOS LAB: BELLATOR V15.3 (GITHUB AUTOPILOT) ===
# Adaptado para GitHub Actions. Sin Google Drive.
# Las claves FTP se leen de los "Secretos" del repositorio por seguridad.

import gpxpy
import gpxpy.gpx
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import requests
import datetime
import os
import ftplib
import folium

# 1. CONFIGURACIÓN DE ENTORNO
print("📡 INICIANDO PROTOCOLO AUTOMÁTICO (GITHUB)...")
OUTPUT_FOLDER = 'output/'
if not os.path.exists(OUTPUT_FOLDER): os.makedirs(OUTPUT_FOLDER)
GPX_FILE = 'MQ_TRACK.gpx' # El archivo está en la raíz del repo

# 2. CARGA DE TRACK
track_points = []
try:
    if os.path.exists(GPX_FILE):
        with open(GPX_FILE, 'r') as g:
            gpx = gpxpy.parse(g)
            for t in gpx.tracks:
                for s in t.segments:
                    for p in s.points: track_points.append([p.latitude, p.longitude])
        print(f"✅ GPX Cargado: {len(track_points)} puntos.")
    else:
        # Fallback
        track_points = [[41.27,-8.08],[41.25,-7.88],[41.42,-7.91],[41.27,-8.08]]
        print("⚠️ NO SE ENCUENTRA EL GPX. USANDO RUTAS DE EMERGENCIA.")
except: track_points = [[41.27,-8.08],[41.27,-8.08]]

# 3. SECTORES
sectors = [
    {"id": 1, "name": "AMARANTE", "lat": 41.2709, "lon": -8.0797, "alt": "65m", "altitude_m": 65, "type": "FLAT", "desc": "START / BASE"},
    {"id": 2, "name": "S. DA ABOBOREIRA", "lat": 41.1946, "lon": -8.0563, "alt": "760m", "altitude_m": 760, "type": "CLIMB", "desc": "ENTRY POINT"},
    {"id": 3, "name": "SERRA DO MARÃO", "lat": 41.2484, "lon": -7.8862, "alt": "1415m", "altitude_m": 1415, "type": "DESCEND", "desc": "HIGH MOUNTAIN"},
    {"id": 4, "name": "GAVIÃO", "lat": 41.2777, "lon": -7.9462, "alt": "900m", "altitude_m": 900, "type": "CLIMB", "desc": "THE FILTER"},
    {"id": 5, "name": "SERRA DO ALVÃO", "lat": 41.3738, "lon": -7.8053, "alt": "1200m", "altitude_m": 1200, "type": "FLAT", "desc": "PLATEAU"},
    {"id": 6, "name": "SRA. GRAÇA", "lat": 41.4168, "lon": -7.9106, "alt": "950m", "altitude_m": 950, "type": "CLIMB", "desc": "THE CLIMB"}
]

# 4. LÓGICA RSI
def get_weather_word(code):
    if code == 0: return "SUNNY"
    if 1 <= code <= 3: return "CLOUDS"
    if code in [45, 48]: return "FOG"
    if 51 <= code <= 67: return "RAIN"
    if 71 <= code <= 77: return "SNOW"
    if 80 <= code <= 82: return "STORM"
    if 85 <= code <= 86: return "SNOW"
    return "CLOUDY"

def calculate_mq_rsi(temp, wind, humidity, altitude, rain, gtype, code):
    veff = wind + (35.0 if gtype == "DESCEND" else 15.0)
    rsi = temp - (0.25 * (veff**0.684) * ((34.8 - temp)**0.31))
    if rain > 0.5: rsi -= 6.0
    status, color, msg = "STABLE", "#2ecc71", "CONDITIONS OK"
    if rsi < 5: status, color, msg = "COLD ALERT", "#f1c40f", "LOW TEMP RISK"
    if rsi < 0: status, color, msg = "HYPOTHERMIA", "#e74c3c", "EXTREME COLD"
    if rsi > 35: status, color, msg = "HEAT WARNING", "#e67e22", "HEAT RISK"
    is_snow = False
    if code in [71, 73, 75, 77, 85, 86]:
        is_snow = True
        status = "SNOW ALERT"; color = "#e67e22"; msg = "ICE/SNOW ON TRACK"
        if code in [75, 86]: status = "BLIZZARD"; color = "#e74c3c"; msg = "EXTREME CAUTION"
    return int(rsi), status, color, msg, is_snow

# 5. GENERADORES
def generate_ui_card(sector, data, trend):
    rsi, status, color, msg, is_snow = calculate_mq_rsi(data['temp'], data['wind'], data['hum'], sector['altitude_m'], data['rain'], sector['type'], data['code'])
    fig, ax = plt.subplots(figsize=(6, 3.2), facecolor='#0f172a'); ax.set_facecolor='#0f172a'
    rect = patches.Rectangle((0, 0), 0.03, 1, transform=ax.transAxes, linewidth=0, facecolor=color); ax.add_patch(rect)
    plt.text(0.08, 0.78, sector['name'], color='white', fontsize=16, fontweight='bold', transform=ax.transAxes)
    plt.text(0.08, 0.65, f"{sector['desc']} | {sector['alt']}", color='#94a3b8', fontsize=8, fontweight='bold', transform=ax.transAxes)
    if is_snow: plt.text(0.5, 0.25, "SNOW", color='white', alpha=0.10, fontsize=55, fontweight='bold', ha='center', transform=ax.transAxes)
    else: plt.text(0.08, 0.25, get_weather_word(data['code']), color='white', alpha=0.08, fontsize=35, fontweight='bold', transform=ax.transAxes)
    plt.text(0.92, 0.65, f"{int(data['temp'])}°", color='white', fontsize=38, fontweight='bold', ha='right', transform=ax.transAxes)
    rsi_col = "#38bdf8" if rsi < data['temp'] else "#fca5a5"
    plt.text(0.92, 0.52, f"MQ RSI: {rsi}°", color=rsi_col, fontsize=10, fontweight='bold', ha='right', transform=ax.transAxes)
    plt.text(0.92, 0.42, f"WIND {int(data['wind'])} km/h", color='#94a3b8', fontsize=7, ha='right', transform=ax.transAxes)
    bbox = dict(boxstyle="round,pad=0.4", fc=color, ec="none", alpha=0.2)
    plt.text(0.92, 0.22, f" {status} ", color=color, fontsize=9, ha='right', fontweight='bold', bbox=bbox, transform=ax.transAxes)
    plt.plot([0.05, 0.95], [0.10, 0.10], color='#334155', linewidth=1, transform=ax.transAxes)
    plt.text(0.05, 0.02, trend, color="#94a3b8", fontsize=9, fontweight='bold', ha='left', va='bottom', transform=ax.transAxes)
    plt.text(0.95, 0.02, "DATA: ECMWF/COPERNICUS", color='#475569', fontsize=7, ha='right', va='bottom', fontfamily='monospace', transform=ax.transAxes)
    ax.axis('off'); plt.savefig(f"{OUTPUT_FOLDER}MQ_SECTOR_{sector['id']}_STATUS.png", dpi=150, bbox_inches='tight', facecolor='#0f172a', pad_inches=0.1); plt.close()
    return status, rsi, data['wind']

def generate_dashboard_banner(status, min_rsi, max_wind, worst_sector):
    fig, ax = plt.subplots(figsize=(8, 2.5), facecolor='#0a0a0a'); ax.set_facecolor='#0a0a0a'
    color = "#2ecc71"
    if "ALERT" in status or "SNOW" in status: color = "#e67e22"
    if "CRITICAL" in status or "BLIZZARD" in status: color = "#e74c3c"
    rect = patches.Rectangle((0, 0), 0.015, 1, transform=ax.transAxes, linewidth=0, facecolor=color); ax.add_patch(rect)
    ax_radar = fig.add_axes([0.05, 0.15, 0.20, 0.70]); ax_radar.set_facecolor='#0a0a0a'
    lats = [p[0] for p in track_points]; lons = [p[1] for p in track_points]
    ax_radar.plot(lons, lats, color=color, linewidth=1.2, alpha=0.9)
    ax_radar.set_aspect('equal'); ax_radar.axis('off')
    ax_radar.add_patch(patches.Circle((0.5, 0.5), 0.48, transform=ax_radar.transAxes, fill=False, edgecolor='#333', linewidth=1, linestyle=':'))
    plt.text(0.28, 0.70, "MQ METEO STATION", color='white', fontsize=14, fontweight='bold', transform=ax.transAxes)
    if color == "#2ecc71": hook = "ALL SECTORS: GREEN LIGHT"; sub = "OPTIMAL CONDITIONS"
    else: hook = f"WARNING: {worst_sector}"; sub = "CHECK TACTICAL MAP"
    plt.text(0.28, 0.50, hook, color=color, fontsize=10, fontweight='bold', transform=ax.transAxes)
    plt.text(0.28, 0.35, sub, color='#888', fontsize=8, transform=ax.transAxes)
    plt.plot([0.68, 0.68], [0.2, 0.8], color='#222', linewidth=1, transform=ax.transAxes)
    plt.text(0.76, 0.70, "MIN RSI", color='#666', fontsize=7, ha='center', transform=ax.transAxes)
    rsi_c = "#38bdf8" if min_rsi < 10 else "white"
    plt.text(0.76, 0.45, f"{min_rsi}°", color=rsi_c, fontsize=20, fontweight='bold', ha='center', transform=ax.transAxes)
    plt.text(0.90, 0.70, "MAX WIND", color='#666', fontsize=7, ha='center', transform=ax.transAxes)
    wind_c = "#e67e22" if max_wind > 30 else "white"
    plt.text(0.90, 0.45, f"{int(max_wind)}", color=wind_c, fontsize=20, fontweight='bold', ha='center', transform=ax.transAxes)
    plt.text(0.90, 0.32, "km/h", color='#666', fontsize=7, ha='center', transform=ax.transAxes)
    bbox_btn = dict(boxstyle="round,pad=0.3", fc="#111", ec="#333", alpha=1.0)
    plt.text(0.96, 0.10, " ▶ ACCEDER A METEO STATION ", color='#aaa', fontsize=7, ha='right', bbox=bbox_btn, transform=ax.transAxes)
    ax.axis('off'); plt.savefig(f"{OUTPUT_FOLDER}MQ_HOME_BANNER.png", facecolor='#0a0a0a', dpi=150, bbox_inches='tight', pad_inches=0.0); plt.close()

def generate_map():
    print("🗺️ GENERANDO MAPA...")
    center = track_points[len(track_points)//2] if len(track_points) > 10 else [41.30, -7.95]
    m = folium.Map(location=center, zoom_start=10, tiles='CartoDB dark_matter')
    folium.PolyLine(track_points, color="#00f2ff", weight=3, opacity=0.9, tooltip="MQ TRACK").add_to(m)
    for s in sectors: folium.CircleMarker([s['lat'], s['lon']], radius=6, color="#ff9900", fill=True, fill_color="#ff9900", fill_opacity=0.9, popup=s['name'], tooltip=s['name']).add_to(m)
    m.save(f"{OUTPUT_FOLDER}MQ_TACTICAL_MAP_CALIBRATED.html")

# === EJECUCIÓN ===
print("🚀 OBTENIENDO DATOS...")
current_hour = datetime.datetime.now().hour
worst_status = "STABLE"; worst_sector = ""; g_min_rsi = 99; g_max_wind = 0

for sec in sectors:
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={sec['lat']}&longitude={sec['lon']}&hourly=temperature_2m,windspeed_10m,weathercode,precipitation,relativehumidity_2m,global_tilted_irradiance&forecast_days=1"
        r = requests.get(url).json()
        h = current_hour
        d = {'temp': r['hourly']['temperature_2m'][h], 'wind': r['hourly']['windspeed_10m'][h], 'rain': r['hourly']['precipitation'][h], 'hum': r['hourly']['relativehumidity_2m'][h], 'code': r['hourly']['weathercode'][h], 'ghi': r['hourly']['global_tilted_irradiance'][h] or 0}
        if sec['altitude_m'] > 1000: d['wind'] *= 1.35; d['temp'] -= 2
        stat, rsi_val, wind_val = generate_ui_card(sec, d, "TREND: STABLE")
        if rsi_val < g_min_rsi: g_min_rsi = rsi_val
        if wind_val > g_max_wind: g_max_wind = wind_val
        if "ALERT" in stat or "SNOW" in stat: worst_status = "ALERT"; worst_sector = sec['name']
    except: pass

generate_dashboard_banner(worst_status, g_min_rsi, g_max_wind, worst_sector)
generate_map()

# === TRANSMISIÓN WEB SEGURA ===
print("Subiendo a FTP...")
FTP_HOST = "ftp.nexplore.pt"
# LEEMOS LA CONTRASEÑA DE LOS SECRETOS DE GITHUB (NO LA ESCRIBIMOS AQUÍ)
FTP_USER = os.environ["FTP_USER"]
FTP_PASS = os.environ["FTP_PASS"]

def upload(local, remote):
    try:
        session = ftplib.FTP()
        session.connect(FTP_HOST, 21); session.login(FTP_USER, FTP_PASS); session.set_pasv(True)
        with open(local, 'rb') as f: session.storbinary(f'STOR {remote}', f)
        print(f"🚀 SUBIDO: {remote}")
        session.quit()
    except Exception as e: print(f"❌ ERROR FTP: {e}")

upload(f"{OUTPUT_FOLDER}MQ_HOME_BANNER.png", "MQ_HOME_BANNER.png")
upload(f"{OUTPUT_FOLDER}MQ_TACTICAL_MAP_CALIBRATED.html", "MQ_TACTICAL_MAP_CALIBRATED.html")
for i in range(1, 7): upload(f"{OUTPUT_FOLDER}MQ_SECTOR_{i}_STATUS.png", f"MQ_SECTOR_{i}_STATUS.png")
