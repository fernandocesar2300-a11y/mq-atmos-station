"""
MQ RIDER INDEX™ · EEI v3.2
Wind chill (JAG/TI, Osczevski & Bluestein 2001) with the rider's own airspeed,
humidity-modulated wet loss, solar gain from GHI (already horizontal; no second
sin(h) factor). Solar elevation is used only to gate night.
"""
import datetime
import math

from ..clock import UTC

MU = 0.6; V_RIDER = 16.0; V_EFF_MIN = 4.8; T_THRESHOLD = 20.0
LAMBDA_BASE = 0.3; LAMBDA_HR = 0.4; R_THRESHOLD = 0.5; ALPHA = 0.007

LEVELS = [(15, "GOOD CONDITIONS", "#2ecc71"), (10, "CAUTION", "#f1c40f"),
          (5, "WARNING", "#e67e22"), (0, "DANGER", "#e74c3c"), (-999, "CRITICAL", "#8b0000")]


def solar_elevation(lat: float, lon: float, ts: datetime.datetime) -> float:
    ts = ts.astimezone(UTC)
    jd = (ts.timestamp() / 86400.0) + 2440587.5
    jc = (jd - 2451545.0) / 36525.0
    l0 = (280.46646 + jc * (36000.76983 + jc * 0.0003032)) % 360
    m = 357.52911 + jc * (35999.05029 - 0.0001537 * jc); mr = math.radians(m)
    c = (math.sin(mr) * (1.914602 - jc * (0.004817 + 0.000014 * jc)) +
         math.sin(2 * mr) * (0.019993 - 0.000101 * jc) + math.sin(3 * mr) * 0.000289)
    tl = l0 + c; om = 125.04 - 1934.136 * jc
    al = tl - 0.00569 - 0.00478 * math.sin(math.radians(om))
    e0 = 23.0 + (26.0 + ((21.448 - jc * (46.8150 + jc * (0.00059 - jc * 0.001813))) / 60.0)) / 60.0
    e = e0 + 0.00256 * math.cos(math.radians(om))
    dec = math.degrees(math.asin(math.sin(math.radians(e)) * math.sin(math.radians(al))))
    y = math.tan(math.radians(e / 2)) ** 2
    et = 4 * math.degrees(y * math.sin(2 * math.radians(l0)) - 2 * 0.016708634 * math.sin(mr) +
                          4 * 0.016708634 * y * math.sin(mr) * math.cos(2 * math.radians(l0)) -
                          0.5 * y * y * math.sin(4 * math.radians(l0)) - 1.25 * 0.016708634 ** 2 * math.sin(2 * mr))
    tst = ts.hour * 60 + ts.minute + ts.second / 60 + et + 4 * lon
    ha = (tst / 4) - 180
    se = (math.sin(math.radians(lat)) * math.sin(math.radians(dec)) +
          math.cos(math.radians(lat)) * math.cos(math.radians(dec)) * math.cos(math.radians(ha)))
    return math.degrees(math.asin(max(-1.0, min(1.0, se))))


def eei(T_a, v_meteo, HR, R_rate, I_sol, lat, lon, ts, altitude_m=0):
    v_eff = (v_meteo or 0.0) * MU + V_RIDER
    T_wc = T_a if v_eff < V_EFF_MIN else (13.12 + 0.6215 * T_a - 11.37 * v_eff ** 0.16 + 0.3965 * T_a * v_eff ** 0.16)
    P_wet = 0.0 if (R_rate or 0) <= R_THRESHOLD else max(0.0, (T_THRESHOLD - T_a) * (LAMBDA_BASE + LAMBDA_HR * (HR or 50) / 100))
    h = solar_elevation(lat, lon, ts)
    alpha = ALPHA * (1.0 + altitude_m * 0.0001)
    G_sol = 0.0 if h <= 0 else (I_sol or 0.0) * alpha
    value = T_wc - P_wet + G_sol
    for thr, name, color in LEVELS:
        if value > thr:
            return round(value, 1), {"level": name, "color": color}, {"T_wc": round(T_wc, 1), "P_wet": round(P_wet, 1), "G_sol": round(G_sol, 1), "h_sol": round(h, 1)}
    return round(value, 1), {"level": "CRITICAL", "color": "#8b0000"}, {}
