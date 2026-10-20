"""
Storm evaluator. Tiers: IPMA (institutional) > AEMET > hail > thunderstorm >
lightning potential > CAPE/LI/CIN. Hail and thunderstorm reach EXTREME/SEVERE
only with dual-model confirmation; a single model is downgraded.
"""
from typing import List, Optional

from ..sources import ipma as ipma_src

COLORS = {"EXTREME": "#4a0000", "SEVERE": "#8b0000", "HIGH": "#c0392b", "MODERATE": "#e67e22", "LOW": "#e67e22"}


def evaluate(ipma_areas: List[str], tz: str, code: int, cape, li, cin, lp,
             aemet_active=False, aemet_severity: Optional[str] = None,
             storm_confirmed=False, hail_confirmed=False):
    cape = cape or 0; li = li or 0; cin = cin or 0; lp = lp or 0

    active, level, texts = ipma_src.active_for_areas(ipma_areas, tz)
    if active:
        s, lv = ipma_src.to_storm_status(level, texts)
        if s:
            return s, COLORS[lv], lv, texts

    if aemet_active:
        if aemet_severity == "Extreme":
            return "EXTREME STORM — AEMET", COLORS["EXTREME"], "EXTREME", []
        return "SEVERE STORM — AEMET", COLORS["SEVERE"], "SEVERE", []

    if code in (96, 99):
        return (("HAIL — DO NOT RIDE", COLORS["EXTREME"], "EXTREME", []) if hail_confirmed
                else ("HAIL POSSIBLE", COLORS["HIGH"], "HIGH", []))
    if 95 <= code <= 99:
        if storm_confirmed:
            if cape > 2500 or lp > 0.5:
                return "SEVERE THUNDERSTORM", COLORS["SEVERE"], "SEVERE", []
            return "THUNDERSTORM", COLORS["HIGH"], "HIGH", []
        return "STORM POSSIBLE", COLORS["MODERATE"], "MODERATE", []

    if lp > 0.5: return "LIGHTNING RISK — HIGH", COLORS["SEVERE"], "SEVERE", []
    if lp > 0.3: return "LIGHTNING RISK", "#e74c3c", "MODERATE", []
    if lp > 0.1: return "LIGHTNING POSSIBLE", COLORS["LOW"], "LOW", []

    if cin >= -200:
        if cape > 2500 and li < -4: return "STORM IMMINENT", COLORS["SEVERE"], "SEVERE", []
        if cape > 1500 and li < -2: return "STORM PROBABLE", "#e74c3c", "MODERATE", []
    if cape > 800:
        return "ATMOSPHERIC INSTABILITY", COLORS["LOW"], "LOW", []
    return None, None, "NONE", []
