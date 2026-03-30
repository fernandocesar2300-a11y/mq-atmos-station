"""
═══════════════════════════════════════════════════════════════════════════
MQ ATMOS LAB: IPMA CALIBRATION v1.0
═══════════════════════════════════════════════════════════════════════════
Genera aemet_corrections.json con offsets reais de estações IPMA portuguesas.

PIPELINE:
1. Fetch lista de estações IPMA (coordenadas)
2. Para cada sector MQ → encontra estação IPMA mais próxima (haversine)
3. Fetch observações horárias da estação
4. Compara com Open-Meteo para mesma coordenada/hora
5. Calcula temp_offset e wind_factor
6. Escreve aemet_corrections.json (formato compatível com main.py V19.x)

EXECUÇÃO: python3 ipma_calibration.py
FREQUÊNCIA: 1×/dia é suficiente (cron separado ou manual)
OUTPUT: aemet_corrections.json (lido automaticamente por main.py)

NOTA: IPMA API é pública, sem API key.
AUTOR: Mountain Quest ATMOS LAB
═══════════════════════════════════════════════════════════════════════════
"""

import requests
import json
import math
import datetime
import os

print("📡 IPMA CALIBRATION v1.0 — INICIANDO...")

# ═══════════════════════════════════════════════════════════════════════════
# SECTORES MQ (espelho de main.py)
# ═══════════════════════════════════════════════════════════════════════════

SECTORS = [
    {"id": 1, "name": "AMARANTE",         "lat": 41.2709, "lon": -8.0797, "altitude_m": 65},
    {"id": 2, "name": "S. DA ABOBOREIRA", "lat": 41.1946, "lon": -8.0563, "altitude_m": 760},
    {"id": 3, "name": "SERRA DO MARÃO",   "lat": 41.2484, "lon": -7.8862, "altitude_m": 1415},
    {"id": 4, "name": "GAVIÃO",           "lat": 41.2777, "lon": -7.9462, "altitude_m": 900},
    {"id": 5, "name": "SERRA DO ALVÃO",   "lat": 41.3738, "lon": -7.8053, "altitude_m": 1200},
    {"id": 6, "name": "SRA. GRAÇA",       "lat": 41.4168, "lon": -7.9106, "altitude_m": 950},
]

# ═══════════════════════════════════════════════════════════════════════════
# HAVERSINE
# ═══════════════════════════════════════════════════════════════════════════

def haversine(lat1, lon1, lat2, lon2):
    """Distância em km entre dois pontos geográficos."""
    R = 6371
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (math.sin(d_lat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(d_lon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# ═══════════════════════════════════════════════════════════════════════════
# FETCH ESTAÇÕES IPMA
# ═══════════════════════════════════════════════════════════════════════════

def fetch_ipma_stations():
    """
    Fetch lista completa de estações IPMA com coordenadas.
    Endpoint público, sem autenticação.
    """
    url = "https://api.ipma.pt/open-data/observation/meteorology/stations/stations.json"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        stations = []
        for feature in data:
            props = feature.get("properties", {})
            coords = feature.get("geometry", {}).get("coordinates", [])
            if len(coords) >= 2:
                stations.append({
                    "id": props.get("idEstacao"),
                    "name": props.get("localEstacao", ""),
                    "lon": coords[0],
                    "lat": coords[1]
                })
        print(f"✅ Estações IPMA carregadas: {len(stations)}")
        return stations
    except Exception as e:
        print(f"❌ Erro fetch estações: {e}")
        return []

# ═══════════════════════════════════════════════════════════════════════════
# MATCH SECTOR → ESTAÇÃO MAIS PRÓXIMA
# ═══════════════════════════════════════════════════════════════════════════

def find_nearest_station(sector, stations, max_dist_km=80):
    """
    Retorna a estação IPMA mais próxima do sector.
    Limite: 80km (cobre Trás-os-Montes + Norte de Portugal com margem).
    """
    best = None
    best_dist = float('inf')
    for st in stations:
        dist = haversine(sector['lat'], sector['lon'], st['lat'], st['lon'])
        if dist < best_dist:
            best_dist = dist
            best = st
    if best and best_dist <= max_dist_km:
        return best, best_dist
    return None, None

# ═══════════════════════════════════════════════════════════════════════════
# FETCH OBSERVAÇÕES IPMA (ÚLTIMA HORA)
# ═══════════════════════════════════════════════════════════════════════════

def fetch_ipma_observations():
    """
    Fetch observações horárias de todas as estações (última hora disponível).
    Retorna dict: {idEstacao: {temp, wind, rain, hum}}
    """
    url = "https://api.ipma.pt/open-data/observation/meteorology/stations/observations.json"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()

        obs = {}
        # Formato: {timestamp: {idEstacao: {params}}}
        # Pegar o timestamp mais recente
        if not data:
            return {}

        latest_ts = sorted(data.keys())[-1]
        latest = data[latest_ts]

        for station_id, params in latest.items():
            if params is None:
                continue
            try:
                obs[int(station_id)] = {
                    "temp":     params.get("temperatura"),
                    "wind":     params.get("intensidadeVentoKM"),
                    "rain":     params.get("precAcumulada"),
                    "hum":      params.get("humidade"),
                    "timestamp": latest_ts
                }
            except (ValueError, TypeError):
                continue

        valid = sum(1 for v in obs.values() if v.get("temp") is not None)
        print(f"✅ Observações IPMA: {len(obs)} estações ({valid} com temperatura válida)")
        return obs

    except Exception as e:
        print(f"❌ Erro fetch observações: {e}")
        return {}

# ═══════════════════════════════════════════════════════════════════════════
# FETCH OPEN-METEO (referência para calcular offset)
# ═══════════════════════════════════════════════════════════════════════════

def fetch_openmeteo_now(lat, lon):
    """
    Fetch valores Open-Meteo actuais para coordenadas dadas.
    Usado como baseline para calcular offset IPMA - Open-Meteo.
    """
    try:
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            f"&hourly=temperature_2m,windspeed_10m,relativehumidity_2m"
            f"&forecast_days=1"
        )
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()

        hour = datetime.datetime.utcnow().hour
        return {
            "temp": data["hourly"]["temperature_2m"][hour],
            "wind": data["hourly"]["windspeed_10m"][hour],
            "hum":  data["hourly"]["relativehumidity_2m"][hour],
        }
    except Exception as e:
        print(f"   ⚠️  Open-Meteo error ({lat},{lon}): {e}")
        return None

# ═══════════════════════════════════════════════════════════════════════════
# CALCULAR OFFSETS
# ═══════════════════════════════════════════════════════════════════════════

def calculate_offsets(ipma_obs, openmeteo_ref):
    """
    Calcula temp_offset e wind_factor comparando IPMA vs Open-Meteo.

    temp_offset = T_ipma - T_openmeteo
        → valor positivo: IPMA regista mais quente que o modelo
        → main.py aplica: adjusted['temp'] += temp_offset

    wind_factor = V_ipma / V_openmeteo
        → >1.0: IPMA regista mais vento que o modelo
        → main.py aplica: adjusted['wind'] *= wind_factor
    """
    offsets = {}

    # Temperatura
    if (ipma_obs.get("temp") is not None and
            openmeteo_ref.get("temp") is not None):
        offsets["temp_offset"] = round(
            float(ipma_obs["temp"]) - float(openmeteo_ref["temp"]), 2
        )
    else:
        offsets["temp_offset"] = 0.0

    # Vento
    if (ipma_obs.get("wind") is not None and
            openmeteo_ref.get("wind") is not None and
            float(openmeteo_ref["wind"]) > 0.5):  # evita divisão por zero
        raw_factor = float(ipma_obs["wind"]) / float(openmeteo_ref["wind"])
        # Clamp: factor entre 0.5 e 2.5 (evita outliers de estações com falha)
        offsets["wind_factor"] = round(max(0.5, min(2.5, raw_factor)), 3)
    else:
        offsets["wind_factor"] = 1.0

    return offsets

# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

print("\n📡 Carregando estações IPMA...")
stations = fetch_ipma_stations()
if not stations:
    print("❌ Sem estações — abortando.")
    exit(1)

print("\n📊 Carregando observações horárias IPMA...")
observations = fetch_ipma_observations()
if not observations:
    print("⚠️  Sem observações disponíveis — aemet_corrections.json não será actualizado.")
    exit(1)

print("\n🔧 Calculando offsets por sector...")
corrections = {}
now_utc = datetime.datetime.utcnow()

for sec in SECTORS:
    station, dist_km = find_nearest_station(sec, stations)

    if not station:
        print(f"⚠️  {sec['name']:20} | Sem estação IPMA dentro de 80km")
        continue

    station_id = station["id"]
    ipma_obs = observations.get(station_id)

    if not ipma_obs or ipma_obs.get("temp") is None:
        print(f"⚠️  {sec['name']:20} | Estação {station['name']} sem dados válidos")
        continue

    # Open-Meteo nas coordenadas da estação IPMA (não do sector)
    # → offset representa desvio real no local de medição
    om_ref = fetch_openmeteo_now(station["lat"], station["lon"])
    if not om_ref:
        print(f"⚠️  {sec['name']:20} | Open-Meteo falhou para estação {station['name']}")
        continue

    offsets = calculate_offsets(ipma_obs, om_ref)

    corrections[sec["name"]] = {
        "last_update": now_utc.isoformat(),
        "source": "IPMA",
        "station_id": station_id,
        "station_name": station["name"],
        "station_dist_km": round(dist_km, 1),
        "ipma_temp": ipma_obs.get("temp"),
        "ipma_wind": ipma_obs.get("wind"),
        "ipma_hum": ipma_obs.get("hum"),
        "om_temp": om_ref.get("temp"),
        "om_wind": om_ref.get("wind"),
        "temp_offset": offsets["temp_offset"],
        "wind_factor": offsets["wind_factor"],
    }

    print(
        f"✅ {sec['name']:20} | "
        f"Estação: {station['name'][:25]:25} ({dist_km:.1f}km) | "
        f"T_ipma={ipma_obs.get('temp')}° T_om={om_ref.get('temp'):.1f}° "
        f"offset={offsets['temp_offset']:+.2f}° | "
        f"wind_factor={offsets['wind_factor']:.3f}"
    )

# ═══════════════════════════════════════════════════════════════════════════
# ESCREVER JSON
# ═══════════════════════════════════════════════════════════════════════════

if corrections:
    output_path = "aemet_corrections.json"
    with open(output_path, "w") as f:
        json.dump(corrections, f, indent=2, ensure_ascii=False)

    print(f"\n✅ {output_path} escrito — {len(corrections)}/{len(SECTORS)} sectores calibrados")
    print(f"   Timestamp: {now_utc.isoformat()}")
else:
    print("\n⚠️  Nenhuma correcção calculada — ficheiro não actualizado")

print("\n" + "═"*70)
print("🎯 IPMA CALIBRATION v1.0 — CONCLUÍDO")
print("═"*70)
