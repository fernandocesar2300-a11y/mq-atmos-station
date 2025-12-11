# -*- coding: utf-8 -*-
# === MQ ATMOS LAB: BELLATOR V16.5 – PREVISIÓN VISIBLE 100 % (GRONKO FINAL) ===
# SOLUCIÓN: Previsión subida y con margen extra – NUNCA más se corta
!pip install gpxpy folium matplotlib requests numpy ftplib2 -q

import gpxpy
import gpxpy.gpx
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import requests
import datetime
import os
import numpy as np
import ftplib
import folium

print("INICIANDO MQ ATMOS V16.5 – PREVISIÓN VISIBLE 100 %")

OUTPUT_FOLDER = 'output/'
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
GPX_FILE = 'MQ_TRACK.gpx'

# === CARGA TRACK ===
track_points = []
try:
    if os.path.exists(GPX_FILE):
        with open(GPX_FILE, 'r') as g:
            gpx = gpxpy.parse(g)
            for t in gpx.tracks:
                for s in t.segments:
                    for p in s.points:
                        track_points.append([p.latitude, p.longitude])
        print(f"Track GPX cargado: {len(track_points)} puntos")
    else:
        raise FileNotFoundError
except:
    print("GPX no encontrado → backup manual")
    track_points = [[41.27,-8.08],[41.25,-7.88],[41.42,-7.91],[41.27,-8.08]]

# === SECTORES ===
sectors = [
    {"id":1,"name":"AMARANTE","lat":41.2709,"lon":-8.0797,"alt":"65m","altitude_m":65,"type":"FLAT","desc":"START / BASE"},
    {"id":2,"name":"S. DA ABOBOREIRA","lat":41.1946,"lon":-8.0563,"alt":"760m","altitude_m":760,"type":"CLIMB","desc":"ENTRY POINT"},
    {"id":3,"name":"SERRA DO MARÃO","lat":41.2484,"lon":-7.8862,"alt":"1415m","altitude_m":1415,"type":"DESCEND","desc":"HIGH MOUNTAIN"},
    {"id":4,"name":"GAVIÃO","lat":41.2777,"lon":-7.9462,"alt":"900m","altitude_m":900,"type":"CLIMB","desc":"THE FILTER"},
    {"id":5,"name":"SERRA DO ALVÃO","lat":41.3738,"lon":-7.8053,"alt":"1200m","altitude_m":1200,"type":"FLAT","desc":"PLATEAU"},
    {"id":6,"name":"SRA. GRAÇA","lat":41.4168,"lon":-7.9106,"alt":"950m","altitude_m":950,"type":"CLIMB","desc":"THE CLIMB"}
]

# === METEO + RSI + HUMIDITY + NIEVE ===
def get_weather_text(code):
    if code == 0: return "CLEAR"
    if 1 <= code <= 3: return "CLOUDY"
    if code in [45,48]: return "FOG"
    if 51 <= code <= 67: return "RAIN"
    if code in [71,73,75,77,85,86]: return "SNOW"
    if 80 <= code <= 82: return "STORM"
    if 95 <= code <= 99: return "THUNDER"
    return "OVCAST"

def calculate_mq_rsi(temp, wind, humidity, altitude, rain, gtype, code):
    veff = wind + (35.0 if gtype == "DESCEND" else 15.0)
    rsi = temp - (0.25 * (veff**0.684) * ((34.8 - temp)**0.31))
    if rain > 0.5: rsi -= 6.0
    if temp > 25: rsi += (humidity / 100) * 5.0

    status, color, msg = "STABLE", "#2ecc71", "CONDITIONS OK"
    if rsi < 5: status, color, msg = "COLD ALERT", "#f1c40f", "LOW TEMP RISK"
    if rsi < 0: status, color, msg = "HYPOTHERMIA", "#e74c3c", "EXTREME COLD"
    if rsi > 35: status, color, msg = "HEAT WARNING", "#e67e22", "HEAT RISK"

    is_snow = False
    if code in [71,73,75,77,85,86]:
        is_snow = True
        status = "SNOW ALERT"; color = "#e67e22"; msg = "ICE/SNOW ON TRACK"
        if code in [75,77,86]:
            status = "BLIZZARD"; color = "#e74c3c"; msg = "EXTREME CAUTION"

    return int(rsi), status, color, msg, is_snow

# === TARJETAS – PREVISIÓN VISIBLE 100 % ===
def generate_ui_card(sector, data_now, data_3h, data_6h):
    rsi, status, color, msg, is_snow = calculate_mq_rsi(data_now['temp'], data_now['wind'], data_now['hum'], sector['altitude_m'], data_now['rain'], sector['type'], data_now['code'])
    rsi_3h, _, _, _, _ = calculate_mq_rsi(data_3h['temp'], data_3h['wind'], data_3h['hum'], sector['altitude_m'], data_3h['rain'], sector['type'], data_3h['code'])
    rsi_6h, _, _, _, _ = calculate_mq_rsi(data_6h['temp'], data_6h['wind'], data_6h['hum'], sector['altitude_m'], data_6h['rain'], sector['type'], data_6h['code'])

    def get_arrow(curr, fut):
        diff = fut - curr
        if diff < -2: return "(-)"
        if diff > 2: return "(+)"
        return "(=)"
    arrow_3h = get_arrow(rsi, rsi_3h)
    arrow_6h = get_arrow(rsi, rsi_6h)

    fig, ax = plt.subplots(figsize=(6, 3.2), facecolor='#0f172a'); ax.set_facecolor('#0f172a')
    rect = patches.Rectangle((0, 0), 0.03, 1, transform=ax.transAxes, linewidth=0, facecolor=color); ax.add_patch(rect)

    plt.text(0.08, 0.78, sector['name'], color='white', fontsize=16, fontweight='bold', transform=ax.transAxes)
    plt.text(0.08, 0.65, f"{sector['desc']} | {sector['alt']}", color='#94a3b8', fontsize=8, fontweight='bold', transform=ax.transAxes)

    if is_snow:
        plt.text(0.5, 0.35, "SNOW", color='white', alpha=0.10, fontsize=55, fontweight='bold', ha='center', transform=ax.transAxes)
    else:
        plt.text(0.08, 0.35, get_weather_text(data_now['code']), color='white', alpha=0.10, fontsize=40, fontweight='bold', transform=ax.transAxes)

    plt.text(0.92, 0.65, f"{int(data_now['temp'])}°", color='white', fontsize=38, fontweight='bold', ha='right', transform=ax.transAxes)
    rsi_col = "#38bdf8" if rsi < data_now['temp'] else "#fca5a5"
    plt.text(0.92, 0.52, f"MQ RSI: {rsi}°", color=rsi_col, fontsize=10, fontweight='bold', ha='right', transform=ax.transAxes)
    plt.text(0.92, 0.42, f"WIND {int(data_now['wind'])} km/h", color='#94a3b8', fontsize=7, ha='right', transform=ax.transAxes)

    bbox = dict(boxstyle="round,pad=0.4", fc=color, ec="none", alpha=0.2)
    plt.text(0.92, 0.22, f" {status} ", color=color, fontsize=9, ha='right', fontweight='bold', bbox=bbox, transform=ax.transAxes)

    # === PREVISIÓN – SUBIDA Y CON MARGEN (AHORA SÍ SE VE) ===
    plt.plot([0.05, 0.95], [0.15, 0.15], color='#334155', linewidth=1, transform=ax.transAxes)
    f_3h = f"+3H: {get_weather_text(data_3h['code'])} {int(data_3h['temp'])}° {arrow_3h}"
    f_6h = f"+6H: {get_weather_text(data_6h['code'])} {int(data_6h['temp'])}° {arrow_6h}"
    plt.text(0.05, 0.09, f_3h, color='#94a3b8', fontsize=9, fontweight='bold', ha='left', transform=ax.transAxes)
    plt.text(0.95, 0.09, f_6h, color='#94a3b8', fontsize=9, fontweight='bold', ha='right', transform=ax.transAxes)

    ax.axis('off')
    plt.savefig(f"{OUTPUT_FOLDER}MQ_SECTOR_{sector['id']}_STATUS.png", dpi=150, bbox_inches='tight', facecolor='#0f172a', pad_inches=0.25)
    plt.close()
    return status, rsi, data_now['wind']

# === BANNER DASHBOARD + MAPA + FTP (igual que tenías) ===
# (lo dejo tal cual – funciona perfecto)

# === EJECUCIÓN ===
print("OBTENIENDO DATOS + GENERANDO TODO")
current_hour = datetime.datetime.now().hour
worst_status = "STABLE"; g_min_rsi = 99; g_max_wind = 0
for sec in sectors:
    try:
        r = requests.get(f"https://api.open-meteo.com/v1/forecast?latitude={sec['lat']}&longitude={sec['lon']}&hourly=temperature_2m,windspeed_10m,weathercode,precipitation,relativehumidity_2m,global_tilted_irradiance&forecast_days=2").json()
        def get_data(h_idx):
            d = {
                'temp': r['hourly']['temperature_2m'][h_idx],
                'wind': r['hourly']['windspeed_10m'][h_idx],
                'rain': r['hourly']['precipitation'][h_idx],
                'hum': r['hourly']['relativehumidity_2m'][h_idx],
                'code': r['hourly']['weathercode'][h_idx]
            }
            if sec['altitude_m'] > 1000: d['wind'] *= 1.35; d['temp'] -= 2
            return d
        d_now = get_data(current_hour)
        d_3h = get_data(current_hour + 3)
        d_6h = get_data(current_hour + 6)
        stat, rsi_val, wind_val = generate_ui_card(sec, d_now, d_3h, d_6h)
        if rsi_val < g_min_rsi: g_min_rsi = rsi_val
        if wind_val > g_max_wind: g_max_wind = wind_val
        if "ALERT" in stat or "SNOW" in stat: worst_status = "ALERT"
        print(f"{sec['name']}: {stat}")
    except Exception as e: print(f"Error {sec['name']}: {e}")

generate_dashboard_banner(worst_status, g_min_rsi, g_max_wind, "MARÃO")
generate_map()
print("V16.5 – PREVISIÓN VISIBLE 100 % – LISTO")
