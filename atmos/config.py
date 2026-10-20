"""Event configuration loaded from events/<name>.yaml. No event data in code."""
import dataclasses
import pathlib
from typing import List, Optional

import yaml


@dataclasses.dataclass
class Point:
    id: int
    name: str
    lat: float
    lon: float
    altitude_m: int
    terrain: str = "FLAT"           # FLAT | CLIMB | DESCEND
    description: str = ""
    ipma_areas: List[str] = dataclasses.field(default_factory=list)
    ipma_station_gid: Optional[int] = None
    ipma_station_alt: Optional[int] = None


@dataclasses.dataclass
class Window:
    start_local: int = 8
    end_local: int = 20


@dataclasses.dataclass
class Event:
    slug: str
    name: str
    timezone: str
    points: List[Point]
    window: Window
    event_date: Optional[str] = None      # ISO date; field.json highlights it
    gpx_path: Optional[str] = None
    publish_dir: str = "atmos"            # remote directory under FTP root
    aemet_area: Optional[str] = None      # AEMET CAP area code, e.g. "61"
    calibration_path: Optional[str] = None  # aemet_corrections.json produced by ipma_calibration.py
    thresholds: dict = dataclasses.field(default_factory=dict)
    labels: dict = dataclasses.field(default_factory=dict)

    @property
    def sorted_points(self):
        return sorted(self.points, key=lambda p: p.id)


def load_event(path: str) -> Event:
    raw = yaml.safe_load(pathlib.Path(path).read_text(encoding="utf-8"))
    pts = [Point(**p) for p in raw["points"]]
    win = Window(**raw.get("window", {}))
    return Event(
        slug=raw["slug"], name=raw["name"], timezone=raw.get("timezone", "Europe/Lisbon"),
        points=pts, window=win, event_date=(str(raw["event_date"]) if raw.get("event_date") else None), gpx_path=raw.get("gpx_path"),
        publish_dir=raw.get("publish_dir", "atmos"), aemet_area=raw.get("aemet_area"), calibration_path=raw.get("calibration_path"),
        thresholds=raw.get("thresholds", {}), labels=raw.get("labels", {}),
    )
