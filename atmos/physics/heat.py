"""
Heat stress: WBGT (Yaglou & Minard 1957) with Stull (2011) wet-bulb + sun
offset as an approximation of the natural wet-bulb, and a dry-bulb safety net.
NOT a Liljegren energy-balance solution; conservative by ~1 °C in full sun.
Thresholds: ACSM, continuous activity, non-acclimatised, high latitude.
"""
import math

WBGT_CAUTION, WBGT_WARNING, WBGT_DANGER = 23.0, 28.0, 30.0
DRY_CAUTION, DRY_WARNING, DRY_DANGER = 30.0, 35.0, 40.0
SUN_OFFSET_C, SUN_OFFSET_GHI = 1.0, 400.0
PALETTE = {"HEAT CAUTION": "#f39c12", "HEAT WARNING": "#e74c3c", "HEAT DANGER": "#7d0a0a"}
RANK = {"NONE": 0, "HEAT CAUTION": 1, "HEAT WARNING": 2, "HEAT DANGER": 3}


def wet_bulb_stull(T_a, HR):
    rh = max(5.0, min(99.0, HR if HR is not None else 50.0))
    return (T_a * math.atan(0.151977 * math.sqrt(rh + 8.313659)) + math.atan(T_a + rh)
            - math.atan(rh - 1.676331) + 0.00391838 * rh ** 1.5 * math.atan(0.023101 * rh) - 4.686035)


def wbgt(T_a, HR, I_sol, v_kmh):
    I = I_sol or 0.0
    Tnwb = wet_bulb_stull(T_a, HR) + (SUN_OFFSET_C if I > SUN_OFFSET_GHI else 0.0)
    v = max(0.5, (v_kmh or 0.0) / 3.6)
    Tg = T_a + (I * 0.025) / (1.0 + 1.6 * v ** 0.85)
    return round(0.7 * Tnwb + 0.2 * Tg + 0.1 * T_a, 1)


def level(wbgt_v, T_a):
    b = ("HEAT DANGER" if wbgt_v >= WBGT_DANGER else "HEAT WARNING" if wbgt_v >= WBGT_WARNING
         else "HEAT CAUTION" if wbgt_v >= WBGT_CAUTION else "NONE")
    c = ("HEAT DANGER" if T_a >= DRY_DANGER else "HEAT WARNING" if T_a >= DRY_WARNING
         else "HEAT CAUTION" if T_a >= DRY_CAUTION else "NONE")
    return b if RANK[b] >= RANK[c] else c


def evaluate(T_a, HR, I_sol, v_kmh, in_window: bool):
    """(status, color, level, wbgt). wbgt always returned; label only inside the riding window."""
    w = wbgt(T_a, HR, I_sol, v_kmh)
    if not in_window:
        return None, None, "NONE", w
    lv = level(w, T_a)
    return (lv, PALETTE[lv], lv, w) if lv != "NONE" else (None, None, "NONE", w)
