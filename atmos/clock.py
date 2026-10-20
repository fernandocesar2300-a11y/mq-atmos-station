"""One clock. All internal timestamps are UTC-aware; local time only at the edges."""
import datetime
from zoneinfo import ZoneInfo

UTC = datetime.timezone.utc


def now_utc() -> datetime.datetime:
    return datetime.datetime.now(UTC)


def to_local(dt: datetime.datetime, tz_name: str) -> datetime.datetime:
    return dt.astimezone(ZoneInfo(tz_name))


def parse_local_naive(s: str, tz_name: str) -> datetime.datetime:
    """Parse an ISO string with no offset as local time of tz_name; return UTC."""
    dt = datetime.datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(tz_name))
    return dt.astimezone(UTC)


def parse_iso_utc(s: str) -> datetime.datetime:
    """Parse ISO strings returned by Open-Meteo with timezone=UTC (no offset)."""
    dt = datetime.datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)
