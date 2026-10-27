import datetime
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from atmos import consensus
from atmos.clock import UTC
from atmos.config import load_event
from atmos.physics import eei, heat, microclimate as mc, storm
from atmos.products import field as field_prod, status as status_prod
from atmos.sources import ipma, openmeteo

EVENT = load_event(str(pathlib.Path(__file__).resolve().parents[1] / "events" / "mq.yaml"))
TS = datetime.datetime(2027, 6, 19, 12, 0, tzinfo=UTC)   # 13h Lisboa, event day


def synth_models(hours=48, t0=TS.replace(hour=0), ifs_t=12.0, icon_t=14.0, identical=False, code=3):
    times = [(t0 + datetime.timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M") for h in range(hours)]
    base = {"temperature_2m": ifs_t, "relative_humidity_2m": 80, "precipitation": 0.0, "snowfall": 0.0,
            "weather_code": code, "wind_speed_10m": 20.0, "wind_gusts_10m": 45.0, "shortwave_radiation": 300.0,
            "freezing_level_height": 3000.0, "cape": 100.0}
    ifs = {k: [v] * hours for k, v in base.items()}
    icon = {k: [v] * hours for k, v in base.items()}
    icon["temperature_2m"] = [ifs_t if identical else icon_t] * hours
    icon["wind_gusts_10m"] = [45.0 if identical else 70.0] * hours
    for k in openmeteo.VARS_STORM:
        icon[k] = [0.0] * hours; ifs[k] = [None] * hours
    return {"time": times, "models": {openmeteo.MODEL_IFS: ifs, openmeteo.MODEL_ICON: icon},
            "rejected": {}, "missing": {openmeteo.MODEL_IFS: openmeteo.VARS_STORM, openmeteo.MODEL_ICON: []}}


def test_plausibility():
    assert openmeteo.plausible("temperature_2m", -999) is None
    assert openmeteo.plausible("wind_gusts_10m", 300) is None
    assert openmeteo.plausible("temperature_2m", 12.3) == 12.3


def test_rank_order():
    r = mc.rank_status
    assert r("HAIL — DO NOT RIDE") > r("SEVERE THUNDERSTORM") > r("HAIL POSSIBLE") > r("WARNING") > r("CAUTION") > r("GOOD CONDITIONS") == 0
    assert r("HEAT WARNING") == 60 and r("HEAT CAUTION") == 40


def test_consensus_divergence_and_identity():
    m = synth_models()
    c, d = consensus.merge(consensus.slice_hour(m["models"], 12))
    assert d["status"] == "UNCERTAIN"       # temp spread 2 °C, gust spread 25 km/h
    m2 = synth_models(identical=True)
    assert openmeteo.models_identical(m2["models"])
    c2, d2 = consensus.merge(consensus.slice_hour(m2["models"], 12), identical=True)
    assert d2["status"] == "INVALID" and c2["temp"] == 12.0 and d2["sources_active"] == 1


def test_consensus_thresholds():
    m = synth_models(ifs_t=12.0, icon_t=13.0)
    m["models"][openmeteo.MODEL_ICON]["wind_gusts_10m"] = [50.0] * 48
    _, d = consensus.merge(consensus.slice_hour(m["models"], 12))
    assert d["status"] == "AGREE"
    m["models"][openmeteo.MODEL_ICON]["wind_gusts_10m"] = [80.0] * 48
    _, d = consensus.merge(consensus.slice_hour(m["models"], 12))
    assert d["status"] == "DIVERGENT"


def test_consensus_missing_values():
    m = synth_models()
    for mod in m["models"].values():
        mod["temperature_2m"] = [None] * 48
    c, _ = consensus.merge(consensus.slice_hour(m["models"], 12))
    assert c["temp"] is None


def test_eei_day_night_and_ghi():
    e_day, _, parts = eei.eei(10, 20, 80, 0, 500, 41.3, -7.8, TS, 1000)
    e_night, _, _ = eei.eei(10, 20, 80, 0, 500, 41.3, -7.8, TS.replace(hour=2), 1000)
    assert e_day > e_night and parts["G_sol"] > 0


def test_heat_window():
    s, _, lv, w = heat.evaluate(33, 40, 800, 10, True)
    assert lv != "NONE" and w > 23
    s, _, lv, w = heat.evaluate(33, 40, 800, 10, False)
    assert lv == "NONE" and w > 0


def test_storm_dual_confirmation(monkeypatch):
    monkeypatch.setattr(ipma, "active_for_areas", lambda areas, tz: (False, None, []))
    assert storm.evaluate([], "Europe/Lisbon", 96, 100, 0, 0, 0, hail_confirmed=False)[2] == "HIGH"
    assert storm.evaluate([], "Europe/Lisbon", 96, 100, 0, 0, 0, hail_confirmed=True)[2] == "EXTREME"
    assert storm.evaluate([], "Europe/Lisbon", 95, 3000, -5, 0, 12.0, storm_confirmed=False)[2] == "MODERATE"
    assert storm.evaluate([], "Europe/Lisbon", 95, 3000, -5, 0, 12.0, storm_confirmed=True)[2] == "SEVERE"


def test_lpi_is_joules_not_fraction(monkeypatch):
    monkeypatch.setattr(ipma, "active_for_areas", lambda areas, tz: (False, None, []))
    assert openmeteo.plausible("lightning_potential", 14.2) == 14.2
    assert storm.evaluate([], "Europe/Lisbon", 3, 500, 0, 0, 0.4)[2] == "NONE"
    assert storm.evaluate([], "Europe/Lisbon", 3, 500, 0, 0, 2.0)[2] == "LOW"
    assert storm.evaluate([], "Europe/Lisbon", 3, 500, 0, 0, 6.0)[2] == "MODERATE"
    assert storm.evaluate([], "Europe/Lisbon", 3, 500, 0, 0, 14.0)[2] == "SEVERE"


def test_storm_ipma_override(monkeypatch):
    monkeypatch.setattr(ipma, "active_for_areas", lambda areas, tz: (True, "orange", ["[ORANGE] Trovoada: granizo"]))
    s, _, lv, _ = storm.evaluate(["VRL"], "Europe/Lisbon", 0, 0, 0, 0, 0)
    assert lv == "SEVERE" and "HAIL" in s


def test_microclimate_valley_window_and_irradiance():
    p = EVENT.points[1]  # 760 m
    d = {"temp": 10.0, "wind": 10.0, "gust": 15.0, "hum": 50, "rain": 0, "irradiance": 0, "code": 0}
    day = mc.apply(p, d, TS, "Europe/Lisbon")
    night = mc.apply(p, d, TS.replace(hour=4), "Europe/Lisbon")
    assert day["temp"] == 11.5 and night["temp"] == 10.0
    assert day["irradiance_source"] == "CODE_ESTIMATE" and day["irradiance"] > 0
    assert mc.apply(p, d, TS, "Europe/Lisbon", nasa_val=420)["irradiance"] == 420
    assert mc.apply(p, {**d, "irradiance": 300}, TS, "Europe/Lisbon", nasa_val=0)["irradiance"] == 300
    ridge = mc.apply(EVENT.points[4], d, TS, "Europe/Lisbon")   # ALVÃO 1043 FLAT → +30%
    assert ridge["wind"] == 13.0 and ridge["temp"] == 10.0      # no temperature re-correction


def test_status_product_worst_is_max(monkeypatch):
    monkeypatch.setattr(ipma, "active_for_areas", lambda areas, tz: (False, None, []))
    per = {}
    for p in EVENT.points:
        m = synth_models(ifs_t=15, icon_t=15.5, code=96 if p.id == 3 else 3)
        m["models"][openmeteo.MODEL_ICON]["wind_gusts_10m"] = [45.0] * 48
        if p.id == 3:
            m["models"][openmeteo.MODEL_ICON]["weather_code"] = [96] * 48
        if p.id == 6:
            m["models"][openmeteo.MODEL_IFS]["temperature_2m"] = [2.0] * 48
            m["models"][openmeteo.MODEL_ICON]["temperature_2m"] = [2.0] * 48
            m["models"][openmeteo.MODEL_IFS]["precipitation"] = [1.0] * 48
            m["models"][openmeteo.MODEL_ICON]["precipitation"] = [1.0] * 48
        per[p.id] = status_prod.evaluate_point(EVENT, p, m, None, (False, "AEMET off", None), None, TS)
    st = status_prod.build(EVENT, per, [], (False, "", None), False, "0/6", "0/6", TS)
    assert st["summary"]["worst_sector"] == "SERRA DO MARÃO" and st["summary"]["alert_rank"] == 90
    assert per[6]["current"]["status"] in ("RAIN/SNOW MIX", "SNOW LIKELY")
    assert st["schema_version"] == 3 and len(st["sectors"]) == 6
    json.dumps(st)


def test_status_sector_missing_listed():
    st = status_prod.build(EVENT, {}, [{"id": 1, "name": "AMARANTE", "error": "x"}], (False, "", None), False, "0/6", "0/6", TS)
    assert st["summary"]["sectors_ok"] == 0 and st["summary"]["sectors_failed"][0]["name"] == "AMARANTE"
    assert st["summary"]["alert_level"] == "STABLE"


def test_field_product_with_ensemble():
    p = EVENT.points[2]
    m = synth_models(hours=24 * 7, t0=TS.replace(hour=0))
    times = m["time"]
    ens = {"time": times, "n_members": 10, "members": {
        "temperature_2m": [[3.0 + k * 0.5 for k in range(10)]] * len(times),
        "precipitation": [[0.0] * 7 + [1.0] * 3] * len(times),
        "wind_gusts_10m": [[40.0 + k * 3 for k in range(10)]] * len(times),
        "wind_speed_10m": [[20.0] * 10] * len(times)}}
    fp = field_prod.build_point(EVENT, p, m, ens, TS)
    assert len(fp["days"]) == 7 and fp["days"][0]["is_event_day"]
    day = fp["days"][0]
    assert day["astro"]["sunrise"] and day["astro"]["civil_dusk"]
    h = day["hours"][0]
    assert h["ensemble"]["p_rain"] == 0.3 and h["ensemble"]["p_cold"] == 0.5
    assert EVENT.window.start_local <= int(h["time_local"][:2]) <= EVENT.window.end_local
    fd = field_prod.build(EVENT, {p.id: fp}, [], TS)
    json.dumps(fd)
    assert fd["points_ok"] == 1 and fd["product"] == "field"


def test_calibration_guards(tmp_path):
    from atmos import calibration
    import json as _j
    now = datetime.datetime.now(UTC).replace(tzinfo=None).isoformat()
    data = {
        "A": {"last_update": now, "station_dist_km": 5, "temp_offset": 1.2, "wind_factor": 0.8, "station_name": "X"},
        "B": {"last_update": now, "station_dist_km": 40, "temp_offset": 1.0, "wind_factor": 1.0},
        "C": {"last_update": now, "station_dist_km": 5, "temp_offset": -110.0, "wind_factor": 1.0},
        "D": {"last_update": "2020-01-01T00:00:00", "station_dist_km": 5, "temp_offset": 1.0, "wind_factor": 1.0},
    }
    p = tmp_path / "c.json"; p.write_text(_j.dumps(data))
    ok, rep = calibration.load(str(p))
    assert set(ok) == {"A"} and "away" in rep["B"] and "range" in rep["C"] and "stale" in rep["D"]
    d = mc.apply(EVENT.points[0], {"temp": 10.0, "wind": 10.0, "gust": 20.0, "hum": 50, "rain": 0, "irradiance": 100, "code": 0},
                 TS, "Europe/Lisbon", calib=ok["A"])
    assert d["temp"] == 11.2 and d["wind"] == 8.0 and d["gust"] == 16.0
