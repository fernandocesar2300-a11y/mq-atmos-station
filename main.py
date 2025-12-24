"""
═══════════════════════════════════════════════════════════════════════════
MQ ATMOS LAB: BELLATOR V18.1 (EEI v3.1 + SNOW ALTITUDE LOGIC)
═══════════════════════════════════════════════════════════════════════════

CHANGELOG V18.1:
✅ CRITICAL FIX: Detección de nieve basada en cota vs altitud de sector
✅ Añadido snowfall y freezing_level_height desde Open-Meteo
✅ Alertas de nieve calibradas para Serra do Marão (800-1415m)
✅ Lógica mountain-aware para precipitación mixta

MODELO:
EEI = T_wc - P_wet + G_sol
Donde:
  T_wc  = Convección JAG/TI con vector cinético
  P_wet = Pérdida conductiva húmeda (modulada por HR%)
  G_sol = Ganancia radiante solar (con ángulo astronómico)

AUTOR: Mountain Quest ATMOS LAB
FECHA: Diciembre 2024
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
from concurrent.futures import ThreadPoolExecutor, as_completed

print("📡 INICIANDO SISTEMA V18.1 (EEI v3.1 + SNOW ALTITUDE)...")

# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO EEI v3.1 (EMBEBIDO)
# ═══════════════════════════════════════════════════════════════════════════

class EEI_v31:
    """
    Effective Exposure Index v3.1
    Basado en JAG/TI (Osczevski & Bluestein, 2001)
    """
    
    # Constantes del modelo
    MU = 0.6                    # Coeficiente incidencia vectorial
    V_RIDER = 16                # km/h - Velocidad media terreno mixto
    V_EFF_MIN = 4.8             # km/h - Umbral flujo laminar
    T_THRESHOLD = 20            # °C - Umbral equilibrio térmico húmedo
    LAMBDA_BASE = 0.3           # Factor base pérdida conductiva
    LAMBDA_HR = 0.4             # Factor humedad relativa
    R_THRESHOLD = 0.5           # mm/h - Umbral precipitación efectiva
    ALPHA = 0.007               # Coef. absorción solar W/m² → °C
    
    @staticmethod
    def calcular_elevacion_solar(lat, lon, timestamp):
        """Calcula elevación solar (algoritmo NOAA)"""
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
        """
        Calcula EEI completo
        
        Args:
            T_a: Temperatura aire (°C)
            v_meteo: Viento (km/h)
            HR: Humedad relativa (%)
            R_rate: Precipitación (mm/h)
            I_sol: Irradiancia solar (W/m²)
            lat: Latitud
            lon: Longitud
            timestamp: datetime UTC
        
        Returns:
            (eei, componentes, estado)
        """
        # 1. Velocidad efectiva
        v_eff = (v_meteo * EEI_v31.MU) + EEI_v31.V_RIDER
        
        # 2. Convección JAG/TI
        if v_eff < EEI_v31.V_EFF_MIN:
            T_wc = T_a
        else:
            v_exp = v_eff ** 0.16
            T_wc = 13.12 + 0.6215 * T_a - 11.37 * v_exp + 0.3965 * T_a * v_exp
        
        # 3. Pérdida conductiva húmeda
        delta_rain = 1 if R_rate > EEI_v31.R_THRESHOLD else 0
        if delta_rain == 0:
            P_wet = 0.0
        else:
            factor = EEI_v31.LAMBDA_BASE + (EEI_v31.LAMBDA_HR * HR / 100)
            P_wet = max(0, (EEI_v31.T_THRESHOLD - T_a) * factor)
        
        # 4. Ganancia radiante solar
        h_sol = EEI_v31.calcular_elevacion_solar(lat, lon, timestamp)
        if h_sol <= 0:
            G_sol = 0.0
        else:
            G_sol = I_sol * EEI_v31.ALPHA * math.sin(math.radians(h_sol))
        
        # 5. EEI final
        EEI = T_wc - P_wet + G_sol
        
        # 6. Estado
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
            'T_wc': round(T_wc, 1),
            'P_wet': round(P_wet, 1),
            'G_sol': round(G_sol, 1),
            'h_sol': round(h_sol, 1),
            'v_eff': round(v_eff, 1)
        }
        
        return round(EEI, 1), componentes, estado

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ═══════════════════════════════════════════════════════════════════════════

OUTPUT_FOLDER = 'output/'
if not os.path.exists(OUTPUT_FOLDER): 
    os.makedirs(OUTPUT_FOLDER)

GPX_FILE = 'MQ_TRACK.gpx'

# ═══════════════════════════════════════════════════════════════════════════
# CARGA TRACK
# ═══════════════════════════════════════════════════════════════════════════

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
        track_points = [[41.27,-8.08],[41.27,-8.08]]
except: 
    track_points = [[41.27,-8.08],[41.27,-8.08]]

# ═══════════════════════════════════════════════════════════════════════════
# SECTORES
# ═══════════════════════════════════════════════════════════════════════════

sectors = [
    {"id":1,"name":"AMARANTE","lat":41.2709,"lon":-8.0797,"alt":"65m","altitude_m":65,"type":"FLAT","desc":"START / BASE"},
    {"id":2,"name":"S. DA ABOBOREIRA","lat":41.1946,"lon":-8.0563,"alt":"760m","altitude_m":760,"type":"CLIMB","desc":"ENTRY POINT"},
    {"id":3,"name":"SERRA DO MARÃO","lat":41.2484,"lon":-7.8862,"alt":"1415m","altitude_m":1415,"type":"DESCEND","desc":"HIGH MOUNTAIN"},
    {"id":4,"name":"GAVIÃO","lat":41.2777,"lon":-7.9462,"alt":"900m","altitude_m":900,"type":"CLIMB","desc":"THE FILTER"},
    {"id":5,"name":"SERRA DO ALVÃO","lat":41.3738,"lon":-7.8053,"alt":"1200m","altitude_m":1200,"type":"FLAT","desc":"PLATEAU"},
    {"id":6,"name":"SRA. GRAÇA","lat":41.4168,"lon":-7.9106,"alt":"950m","altitude_m":950,"type":"CLIMB","desc":"THE CLIMB"}
]

# ═══════════════════════════════════════════════════════════════════════════
# WEATHERCODE MAPPING
# ═══════════════════════════════════════════════════════════════════════════

def get_weather_text(code):
    """Convierte weathercode a texto"""
    if code == 0: return "CLEAR"
    if 1 <= code <= 3: return "CLOUDY"
    if code in [45, 48]: return "FOG"
    if 51 <= code <= 67: return "RAIN"
    if code in [71,73,75,77,85,86]: return "SNOW"
    if 80 <= code <= 82: return "STORM"
    if 95 <= code <= 99: return "THUNDER"
    return "OVCAST"

# ═══════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS (SAFE GET & PARALLEL WORKERS)
# ═══════════════════════════════════════════════════════════════════════════

def safe_get(lst, idx, default):
    """Safe list access with bounds checking"""
    if not lst or idx >= len(lst) or idx < 0:
        return default
    return lst[idx]

def fetch_sector_data(sector, session, current_hour):
    """Worker function for parallel weather fetch"""
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={sector['lat']}&longitude={sector['lon']}&hourly=temperature_2m,windspeed_10m,weathercode,precipitation,relativehumidity_2m,global_tilted_irradiance,snowfall,freezing_level_height&forecast_days=2"
        r = session.get(url, timeout=10).json()
        
        # Check if response has error
        if 'error' in r:
            return (sector, None, f"API Error: {r.get('reason', 'Unknown')}")

        def get_data(h):
            hourly = r.get('hourly', {})
            return {
                'temp': safe_get(hourly.get('temperature_2m', []), h, 0.0),
                'wind': safe_get(hourly.get('windspeed_10m', []), h, 0.0),
                'rain': safe_get(hourly.get('precipitation', []), h, 0.0),
                'hum': safe_get(hourly.get('relativehumidity_2m', []), h, 50.0),
                'code': safe_get(hourly.get('weathercode', []), h, 0),
                'irradiance': safe_get(hourly.get('global_tilted_irradiance', []), h, 0),
                'snowfall': safe_get(hourly.get('snowfall', []), h, 0.0),
                'freezing_level': safe_get(hourly.get('freezing_level_height', []), h, 9999)
            }
        
        d_now = get_data(current_hour)
        d_3h = get_data(current_hour + 3)
        d_6h = get_data(current_hour + 6)
        
        # Ajuste altitud
        if sector['altitude_m'] > 1000:
            d_now['wind'] *= 1.35
            d_now['temp'] -= 2
            
        processed_data = {
            'now': d_now,
            '3h': d_3h,
            '6h': d_6h
        }
        
        return (sector, processed_data, None)

    except Exception as e:
        return (sector, None, str(e))

def upload_file(filepath, remote_name, ftp_host, ftp_user, ftp_pass):
    """Worker function for parallel FTP uploads (connect-upload-close pattern)"""
    ftp = ftplib.FTP()
    try:
        ftp.connect(ftp_host, 21, timeout=30)
        ftp.login(ftp_user, ftp_pass)
        ftp.set_pasv(True)
        with open(filepath, 'rb') as f:
            ftp.storbinary(f'STOR {remote_name}', f)
        return (remote_name, True, None)
    except Exception as e:
        return (remote_name, False, str(e))
    finally:
        try:
            ftp.quit()
        except:
            pass

# ═══════════════════════════════════════════════════════════════════════════
# GENERADOR DE TARJETAS (CON EEI v3.1 + SNOW ALTITUDE LOGIC)
# ═══════════════════════════════════════════════════════════════════════════

def generate_ui_card(sector, data_now, data_3h, data_6h, time_str):
    """
    Genera tarjeta de sector usando EEI v3.1 + Snow Altitude Detection
    Mantiene diseño visual de V17.1
    """
    now_ts = datetime.datetime.utcnow()
    
    # Calcular EEI para now, +3h, +6h
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
    
    # Estado y color
    status = estado_now['nivel']
    color = estado_now['color']
    
    # ═════════════════════════════════════════════════════════════════════
    # CRITICAL FIX: Detección de nieve basada en ALTITUD vs COTA
    # ═════════════════════════════════════════════════════════════════════
    is_snow = False
    snow_intensity = "LIGHT"
    
   # Condición 1: Altitud del sector está SOBRE la cota de nieve
    if sector['altitude_m'] > data_now['freezing_level']:
        is_snow = True
        
    # Condición 2: Weathercode indica nieve (backup)
    if data_now['code'] in [71, 73, 75, 77, 85, 86]:
        is_snow = True
        
    # Condición 3: Hay snowfall activo
    if data_now['snowfall'] > 0.1:
        is_snow = True
        if data_now['snowfall'] > 1.0:
            snow_intensity = "MODERATE"
        if data_now['snowfall'] > 5.0:
            snow_intensity = "HEAVY"
    
    if is_snow:
        if snow_intensity == "LIGHT":
            status = "SNOW ALERT"
            color = "#f1c40f"  # Amarillo
        elif snow_intensity == "MODERATE":
            status = "SNOW WARNING"
            color = "#e67e22"  # Naranja
        else:  # HEAVY
            status = "BLIZZARD"
            color = "#e74c3c"  # Rojo
    
    # Flechas de tendencia
    def get_arrow(curr, fut):
        diff = fut - curr
        if diff < -2: return "(-)"
        if diff > 2: return "(+)"
        return "(=)"
    
    arrow_3h = get_arrow(eei_now, eei_3h)
    arrow_6h = get_arrow(eei_now, eei_6h)
    
    # GENERAR TARJETA
    fig, ax = plt.subplots(figsize=(6, 3.4), facecolor='#0f172a')
    ax.set_facecolor('#0f172a') # FIX: Method call
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
    
    # Barra lateral
    rect = patches.Rectangle((0, 0), 0.03, 1, transform=ax.transAxes, 
                            linewidth=0, facecolor=color)
    ax.add_patch(rect)
    
    # Header
    plt.text(0.08, 0.80, sector['name'], color='white', 
            fontsize=16, fontweight='bold', transform=ax.transAxes)
    plt.text(0.08, 0.68, f"{sector['desc']} | {sector['alt']}", 
            color='#94a3b8', fontsize=8, fontweight='bold', transform=ax.transAxes)
    
    # Watermark
    if is_snow:
        watermark_text = "❄" if snow_intensity == "LIGHT" else "SNOW"
        plt.text(0.5, 0.40, watermark_text, color='white', alpha=0.10, 
                fontsize=55, fontweight='bold', ha='center', transform=ax.transAxes)
    else:
        plt.text(0.08, 0.40, get_weather_text(data_now['code']), 
                color='white', alpha=0.10, fontsize=40, fontweight='bold', 
                transform=ax.transAxes)
    
    # Temperatura actual
    plt.text(0.92, 0.68, f"{int(data_now['temp'])}°", color='white', 
            fontsize=38, fontweight='bold', ha='right', transform=ax.transAxes)
    
    # MRI (MQ Rider Index)
    mri_col = "#38bdf8" if eei_now < data_now['temp'] else "#fca5a5"
    if status in ["CRITICAL", "DANGER", "BLIZZARD"]:
        mri_col = "#ffffff"  # Blanco para alertas críticas
    plt.text(0.92, 0.55, f"MRI: {int(eei_now)}°", color=mri_col, 
            fontsize=10, fontweight='bold', ha='right', transform=ax.transAxes)
    
    # Viento
    plt.text(0.92, 0.45, f"WIND {int(data_now['wind'])} km/h", 
            color='#94a3b8', fontsize=7, ha='right', transform=ax.transAxes)
    
    # Status badge (texto siempre blanco para máxima visibilidad)
    bbox = dict(boxstyle="round,pad=0.4", fc=color, ec="none", alpha=0.9)
    plt.text(0.92, 0.25, f" {status} ", color='white', fontsize=9, 
            ha='right', fontweight='bold', bbox=bbox, transform=ax.transAxes)
    
    # Separador
    plt.plot([0.05, 0.95], [0.15, 0.15], color='#334155', 
            linewidth=1, transform=ax.transAxes)
    
    # Forecast
    f_3h = f"+3H: {get_weather_text(data_3h['code'])} {int(data_3h['temp'])}° {arrow_3h}"
    f_6h = f"+6H: {get_weather_text(data_6h['code'])} {int(data_6h['temp'])}° {arrow_6h}"
    plt.text(0.05, 0.09, f_3h, color='#94a3b8', fontsize=9, 
            fontweight='bold', ha='left', transform=ax.transAxes)
    plt.text(0.95, 0.09, f_6h, color='#94a3b8', fontsize=9, 
            fontweight='bold', ha='right', transform=ax.transAxes)
    
    # Timestamp + branding
    plt.text(0.5, 0.02, f"UPDATED: {time_str} (UTC) | MQ RIDER INDEX™ v3.1", 
            color='#475569', fontsize=6, ha='center', transform=ax.transAxes)
    
    ax.axis('off')
    plt.savefig(f"{OUTPUT_FOLDER}MQ_SECTOR_{sector['id']}_STATUS.png", 
                dpi=150, facecolor='#0f172a')
    plt.close()
    
    return status, int(eei_now), data_now['wind'], is_snow, snow_intensity

# ═══════════════════════════════════════════════════════════════════════════
# BANNER PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════

def generate_dashboard_banner(status, min_eei, max_wind, worst_sector, time_str, snow_detected):
    """Genera banner principal - mismo diseño V17.1 + snow awareness"""
    fig, ax = plt.subplots(figsize=(8, 2.5), facecolor='#0a0a0a')
    ax.set_facecolor('#0a0a0a') # FIX: Method call
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
    
    color = "#2ecc71"
    if "ALERT" in status or "SNOW" in status: 
        color = "#f1c40f"
    if "WARNING" in status:
        color = "#e67e22"
    if "CRITICAL" in status or "BLIZZARD" in status: 
        color = "#e74c3c"
    
    # Barra lateral
    rect = patches.Rectangle((0, 0), 0.015, 1, transform=ax.transAxes, 
                            linewidth=0, facecolor=color)
    ax.add_patch(rect)
    
    # Radar
    ax_radar = fig.add_axes([0.05, 0.15, 0.20, 0.70])
    ax_radar.set_facecolor('#0a0a0a') # FIX: Method call
    lats = [p[0] for p in track_points]
    lons = [p[1] for p in track_points]
    ax_radar.plot(lons, lats, color=color, linewidth=1.2, alpha=0.9)
    ax_radar.set_aspect('equal')
    ax_radar.axis('off')
    ax_radar.add_patch(patches.Circle((0.5, 0.5), 0.48, transform=ax_radar.transAxes, 
                                     fill=False, edgecolor='#333', linewidth=1, linestyle=':'))
    
    # Título
    plt.text(0.28, 0.70, "MQ METEO STATION", color='white', 
            fontsize=14, fontweight='bold', transform=ax.transAxes)
    
    # Hook
    if color == "#2ecc71":
        hook = "ALL SECTORS: GREEN LIGHT"
        sub = f"UPDATED: {time_str} UTC | MQ RIDER INDEX™"
    elif snow_detected:
        hook = f"SNOW ALERT: {worst_sector}"
        sub = f"UPDATED: {time_str} UTC | ALTITUDE-AWARE SYSTEM"
    else:
        hook = f"WARNING: {worst_sector}"
        sub = f"UPDATED: {time_str} UTC | MQ RIDER INDEX™"
    
    plt.text(0.28, 0.50, hook, color=color, fontsize=10, 
            fontweight='bold', transform=ax.transAxes)
    plt.text(0.28, 0.35, sub, color='#888', fontsize=8, transform=ax.transAxes)
    
    # Separador
    plt.plot([0.68, 0.68], [0.2, 0.8], color='#222', linewidth=1, transform=ax.transAxes)
    
    # MIN MRI
    plt.text(0.76, 0.70, "MIN MRI", color='#666', fontsize=7, 
            ha='center', transform=ax.transAxes)
    eei_c = "#38bdf8" if min_eei < 10 else "white"
    plt.text(0.76, 0.45, f"{min_eei}°", color=eei_c, fontsize=20, 
            fontweight='bold', ha='center', transform=ax.transAxes)
    
    # MAX WIND
    plt.text(0.90, 0.70, "MAX WIND", color='#666', fontsize=7, 
            ha='center', transform=ax.transAxes)
    wind_c = "#e67e22" if max_wind > 30 else "white"
    plt.text(0.90, 0.45, f"{int(max_wind)}", color=wind_c, fontsize=20, 
            fontweight='bold', ha='center', transform=ax.transAxes)
    plt.text(0.90, 0.32, "km/h", color='#666', fontsize=7, 
            ha='center', transform=ax.transAxes)
    
    # Botón
    bbox_btn = dict(boxstyle="round,pad=0.3", fc="#111", ec="#333", alpha=1.0)
    plt.text(0.96, 0.10, " ▶ ACCEDER A METEO STATION ", color='#aaa', 
            fontsize=7, ha='right', bbox=bbox_btn, transform=ax.transAxes)
    
    ax.axis('off')
    plt.savefig(f"{OUTPUT_FOLDER}MQ_HOME_BANNER.png", facecolor='#0a0a0a', dpi=150)
    plt.close()

# ═══════════════════════════════════════════════════════════════════════════
# MAPA TÁCTICO
# ═══════════════════════════════════════════════════════════════════════════

def generate_map():
    """Genera mapa táctico"""
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
# EJECUCIÓN PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════

print("🚀 OBTENIENDO DATOS OPEN-METEO...")
now = datetime.datetime.utcnow() # FIX: UTC
time_str = now.strftime("%H:%M")
current_hour = now.hour

worst_status = "STABLE"
worst_sector = ""
g_min_eei = 99
g_max_wind = 0
snow_detected = False

# HTTP Session reuse
http_session = requests.Session()

try:
    # Parallel Fetch
    print("   Starting parallel fetch (4 workers)...")
    results = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_sector_data, s, http_session, current_hour): s for s in sectors}
        for future in as_completed(futures):
            results.append(future.result())
            
    # Sort by sector ID to maintain order
    results.sort(key=lambda x: x[0]['id'] if x[0] else 999)
    
    # Sequential processing
    for sector, data, error in results:
        if error or not data:
            print(f"❌ {sector['name']:20} | Error: {error if error else 'No data'}")
            continue

        d_now = data['now']
        d_3h = data['3h']
        d_6h = data['6h']
        
        # Generate card (Matplotlib is sequential)
        stat, eei_val, wind_val, is_snow, snow_int = generate_ui_card(sector, d_now, d_3h, d_6h, time_str)
        
        # Update Stats
        if eei_val < g_min_eei:
            g_min_eei = eei_val
        if wind_val > g_max_wind:
            g_max_wind = wind_val
        if is_snow:
            snow_detected = True
        if "ALERT" in stat or "SNOW" in stat or "WARNING" in stat:
            worst_status = stat
            worst_sector = sector['name']
        
        snow_marker = f"❄ [{snow_int}]" if is_snow else ""
        print(f"✅ {sector['name']:20} | MRI: {eei_val:3d}°C {snow_marker}")

finally:
    http_session.close()

# Generar banner y mapa
generate_dashboard_banner(worst_status, g_min_eei, g_max_wind, worst_sector, time_str, snow_detected)
generate_map()

# ═══════════════════════════════════════════════════════════════════════════
# GENERAR JSON DE ESTADO (PARA WIDGET)
# ═══════════════════════════════════════════════════════════════════════════

print("📊 GENERANDO JSON DE ESTADO...")
import json

status_data = {
    "last_update": time_str,
    "alert_level": worst_status,
    "min_mri": g_min_eei,
    "worst_sector": worst_sector if worst_sector else "ALL SECTORS",
    "status": worst_status,
    "max_wind": int(g_max_wind),
    "snow_detected": snow_detected,
    "timestamp_utc": now.isoformat(),
    "model_version": "MQ Rider Index v3.1 + Snow Altitude",
    "data_sources": ["ECMWF", "Copernicus", "NOAA GFS"]
}

try:
    with open(f"{OUTPUT_FOLDER}MQ_ATMOS_STATUS.json", 'w') as f:
        json.dump(status_data, f, indent=2)
    print("✅ JSON de estado generado")
except Exception as e:
    print(f"⚠️  Error generando JSON: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# FTP UPLOAD
# ═══════════════════════════════════════════════════════════════════════════

print("\n" + "─"*70)
print("🚀 SUBIENDO A FTP...")
print("─"*70)

FTP_HOST = "ftp.nexplore.pt"

if "FTP_USER" in os.environ:
    FTP_USER = os.environ["FTP_USER"]
    FTP_PASS = os.environ["FTP_PASS"]
    
    # List of files to upload
    files_to_upload = [
        (f"{OUTPUT_FOLDER}MQ_HOME_BANNER.png", "MQ_HOME_BANNER.png"),
        (f"{OUTPUT_FOLDER}MQ_TACTICAL_MAP_CALIBRATED.html", "MQ_TACTICAL_MAP_CALIBRATED.html"),
        (f"{OUTPUT_FOLDER}MQ_ATMOS_STATUS.json", "MQ_ATMOS_STATUS.json")
    ]
    for i in range(1, 7):
        files_to_upload.append((f"{OUTPUT_FOLDER}MQ_SECTOR_{i}_STATUS.png", f"MQ_SECTOR_{i}_STATUS.png"))

    print(f"   Starting parallel upload ({len(files_to_upload)} files)...")

    with ThreadPoolExecutor(max_workers=4) as executor:
        # Submit all upload tasks
        futures = {executor.submit(upload_file, local, remote, FTP_HOST, FTP_USER, FTP_PASS): remote for local, remote in files_to_upload}
        
        for future in as_completed(futures):
            remote_name, success, error = future.result()
            if success:
                print(f"   ✓ {remote_name}")
            else:
                print(f"   ❌ {remote_name} - {error}")
                
    print("\n✅ FTP UPLOAD COMPLETADO")

else:
    print("⚠️  MODO LOCAL (Variables FTP_USER/FTP_PASS no encontradas)")
    print("   Archivos generados en carpeta 'output/'")

# ═══════════════════════════════════════════════════════════════════════════
# FIN
# ═══════════════════════════════════════════════════════════════════════════

print("\n" + "═"*70)
print("🎯 BELLATOR V18.1 COMPLETADO")
print("═"*70)
print(f"📊 Modelo: MQ Rider Index v3.1 (JAG/TI adapted for MTB)")
print(f"📅 Timestamp: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"🌡️  MIN MRI: {g_min_eei}°C")
print(f"💨 MAX WIND: {int(g_max_wind)} km/h")
print(f"❄️  SNOW: {'DETECTED' if snow_detected else 'NONE'}")
print(f"⚠️  Status: {worst_status}")
print("═"*70)
print("\n✨ MQ RIDER INDEX™ v3.1 + SNOW ALTITUDE LOGIC")
print("   Technical Base: Osczevski & Bluestein (2001) - JAG/TI Standard")
print("   Mountain Adaptation: Altitude-aware snow detection (65-1415m)")
print("═"*70 + "\n")
