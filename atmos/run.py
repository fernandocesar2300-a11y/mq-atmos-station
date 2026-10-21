"""
CLI:  python -m atmos.run --event events/mq.yaml --products status,field [--publish] [--cards]

Flow per run:
  heartbeat /start → fetch (parallel per point) → evaluate → build products →
  write output/<event>/ → publish atomically → heartbeat ok. Any uncaught
  failure → heartbeat /fail. Total source failure → republish the previous
  product marked degraded=true (never silence, never fresh-looking stale data).
"""
import argparse
import datetime
import json
import logging
import pathlib
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor

from . import ENGINE_VERSION
from .clock import now_utc
from .config import load_event
from . import calibration
from .products import field as field_prod, status as status_prod
from .publish import ftp, heartbeat
from .sources import aemet, ipma, nasa, openmeteo

log = logging.getLogger("atmos")


def _setup_logging(out_dir: pathlib.Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=[
        logging.StreamHandler(sys.stdout), logging.FileHandler(out_dir / "atmos_log.txt", mode="w", encoding="utf-8")])


def _load_previous(path: pathlib.Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _degraded_copy(prev: dict, reason: str, ts_now):
    d = dict(prev); d["degraded"] = True; d["degraded_reason"] = reason
    d["degraded_at_utc"] = ts_now.isoformat()   # timestamp_utc stays the original
    return d


def _track(gpx_path):
    if not gpx_path or not pathlib.Path(gpx_path).exists():
        return []
    try:
        import gpxpy
        with open(gpx_path) as g:
            gpx = gpxpy.parse(g)
        return [[p.latitude, p.longitude] for t in gpx.tracks for s in t.segments for p in s.points]
    except Exception:
        return []


def run(event_path: str, products: list, publish: bool, cards: bool, out_root: str = "output") -> int:
    event = load_event(event_path)
    out_dir = pathlib.Path(out_root) / event.slug
    _setup_logging(out_dir)
    ts_now = now_utc()
    heartbeat.ping("/start", f"{event.slug} {ts_now.isoformat()}")
    log.info("DSS FIELD · ATMOS %s · event=%s · products=%s", ENGINE_VERSION, event.slug, ",".join(products))

    want_field = "field" in products; want_status = "status" in products
    forecast_days = 7 if want_field else 2

    # ── FETCH ──
    fetched, ens, failed = {}, {}, []

    def _one(p):
        f = openmeteo.fetch_models(p.lat, p.lon, forecast_days)
        e = openmeteo.fetch_ensemble(p.lat, p.lon, forecast_days) if want_field else None
        return p.id, f, e

    with ThreadPoolExecutor(max_workers=3) as ex:
        for fut, p in [(ex.submit(_one, p), p) for p in event.points]:
            try:
                pid, f, e = fut.result(timeout=90)
                fetched[pid], ens[pid] = f, e
                if f["missing"].get(openmeteo.MODEL_IFS) or f["missing"].get(openmeteo.MODEL_ICON):
                    log.info("%s missing vars: %s", p.name, f["missing"])
                if any(f["rejected"].values()):
                    log.warning("%s rejected values: %s", p.name, f["rejected"])
            except Exception as e:
                log.error("%s fetch failed: %s", p.name, str(e)[:160])
                failed.append({"id": p.id, "name": p.name, "error": str(e)[:160]})

    if not fetched:
        reason = "all point fetches failed"
        log.error(reason)
        written = []
        for prod in products:
            path = out_dir / f"{prod}.json"
            prev = _load_previous(path)
            if prev:
                path.write_text(json.dumps(_degraded_copy(prev, reason, ts_now), indent=2), encoding="utf-8")
                written.append((str(path), f"{prod}.json"))
                log.warning("%s republished DEGRADED (original %s)", prod, prev.get("timestamp_utc"))
        if publish and written:
            log.info(ftp.publish(written + [(str(out_dir / "atmos_log.txt"), "atmos_log.txt")], event.publish_dir))
        heartbeat.ping("/fail", reason)
        return 2

    aemet_res = aemet.storm_alerts(event.aemet_area)
    log.info("AEMET: %s", aemet_res[1])
    ipma_fc = {}
    if want_status:
        for p in event.points:
            loc = ipma.nearest_location(p.lat, p.lon)
            r = ipma.point_forecast(p.lat, p.lon, p.altitude_m, event.timezone, p.ipma_station_gid, p.ipma_station_alt)
            log.info("IPMA anchor %-20s nearest=%s (%s) → %s", p.name,
                     loc.get("local") if loc else None, loc.get("globalIdLocal") if loc else None,
                     (r["source"] + (" T=%.1f" % r["temp"] if r.get("temp") is not None else " T=unanchored")) if r else "NO DATA")
            if r:
                ipma_fc[p.id] = r
        nasa_cache = nasa.fetch_all(event.points, ts_now)
    else:
        nasa_cache = {}

    files = []
    calib, calib_report = ({}, {})
    if event.calibration_path:
        calib, calib_report = calibration.load(event.calibration_path)
        for k, v in calib_report.items():
            log.info("calibration %-20s %s", k, v)

    # ── STATUS ──
    if want_status:
        per_point = {}
        for p in event.points:
            if p.id not in fetched:
                continue
            try:
                per_point[p.id] = status_prod.evaluate_point(event, p, fetched[p.id], ipma_fc.get(p.id), aemet_res, nasa_cache.get(p.id), ts_now, calib.get(p.name))
                c = per_point[p.id]["current"]
                log.info("%-20s MRI %3d° G%3d %-24s [%s] %s", p.name, c["eei"], c["gust"], c["status"], per_point[p.id]["consensus"]["status"][:3], " ".join(c["microclimate_log"]))
            except Exception as e:
                log.error("%s evaluate failed: %s", p.name, traceback.format_exc(limit=2))
                failed.append({"id": p.id, "name": p.name, "error": str(e)[:160]})
        ipma_active = any(ipma.active_for_areas(p.ipma_areas, event.timezone)[0] for p in event.points)
        st = status_prod.build(event, per_point, failed, aemet_res, ipma_active,
                               f"{len(nasa_cache)}/{len(event.points)}", f"{len(ipma_fc)}/{len(event.points)}", ts_now)
        path = out_dir / "status.json"; path.write_text(json.dumps(st, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info("STATUS: %s rank %s @ %s · ok %d/%d", st["summary"]["alert_level"], st["summary"]["alert_rank"],
                 st["summary"]["worst_sector"], st["summary"]["sectors_ok"], st["summary"]["sectors_expected"])
        if st["consensus_engine"]["models_identical_detected"]:
            log.error("MODELS IDENTICAL — consensus INVALID")
        if cards:
            from .render import cards as cards_mod
            for sec in st["sectors"]:
                cp = out_dir / f"sector_{sec['id']}.png"; cards_mod.sector_card(sec, str(cp), ENGINE_VERSION); files.append((str(cp), f"sector_{sec['id']}.png"))
            bp = out_dir / "banner.png"; cards_mod.banner(st, str(bp), _track(event.gpx_path), ENGINE_VERSION); files.append((str(bp), "banner.png"))
        files.append((str(path), "status.json"))

    # ── FIELD ──
    if want_field:
        per_point = {}
        ffailed = [f for f in failed if f["id"] not in fetched]
        for p in event.points:
            if p.id not in fetched:
                continue
            try:
                per_point[p.id] = field_prod.build_point(event, p, fetched[p.id], ens.get(p.id), ts_now)
                log.info("%-20s field: %d days · ensemble %s", p.name, len(per_point[p.id]["days"]), per_point[p.id]["ensemble_members"])
            except Exception as e:
                log.error("%s field failed: %s", p.name, traceback.format_exc(limit=2))
                ffailed.append({"id": p.id, "name": p.name, "error": str(e)[:160]})
        fd = field_prod.build(event, per_point, ffailed, ts_now)
        path = out_dir / "field.json"; path.write_text(json.dumps(fd, indent=2, ensure_ascii=False), encoding="utf-8")
        files.append((str(path), "field.json"))

    # history for calibration (event day ± 1)
    if event.event_date:
        try:
            ed = datetime.date.fromisoformat(event.event_date)
            if abs((ts_now.date() - ed).days) <= 1:
                hist = out_dir / "history"; hist.mkdir(exist_ok=True)
                for prod in products:
                    src = out_dir / f"{prod}.json"
                    if src.exists():
                        (hist / f"{prod}_{ts_now.strftime('%Y%m%dT%H%M')}.json").write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        except Exception:
            pass

    # Legacy names (V21 consumers: Ghost Rider, BIOS, old panel). Remove after migration.
    if want_status and (out_dir / "status.json").exists():
        files.append((str(out_dir / "status.json"), "MQ_ATMOS_STATUS.json"))
        if cards:
            for i in range(1, len(event.points) + 1):
                cp = out_dir / f"sector_{i}.png"
                if cp.exists():
                    files.append((str(cp), f"MQ_SECTOR_{i}_STATUS.png"))
            if (out_dir / "banner.png").exists():
                files.append((str(out_dir / "banner.png"), "MQ_HOME_BANNER.png"))
    web = pathlib.Path("web/track-status.html")
    if web.exists():
        files.append((str(web), "track-status.html"))

    files.append((str(out_dir / "atmos_log.txt"), "atmos_log.txt"))
    if publish:
        try:
            log.info(ftp.publish(files, event.publish_dir))
        except ftp.PublishError as e:
            log.error("PUBLISH FAILED: %s", e); heartbeat.ping("/fail", f"publish: {e}"); return 3
    heartbeat.ping("", f"{event.slug} ok {len(failed)} failed")
    return 0 if not failed else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--event", required=True); ap.add_argument("--products", default="status,field")
    ap.add_argument("--publish", action="store_true"); ap.add_argument("--cards", action="store_true")
    ap.add_argument("--out", default="output")
    a = ap.parse_args()
    try:
        sys.exit(run(a.event, [p.strip() for p in a.products.split(",")], a.publish, a.cards, a.out))
    except SystemExit:
        raise
    except Exception as e:
        heartbeat.ping("/fail", f"uncaught: {e}")
        raise


if __name__ == "__main__":
    main()
