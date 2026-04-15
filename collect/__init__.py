"""
collect/__init__.py - Collector registry.

Usage:
    from collect import get_all_collectors, run_collector

    collectors = get_all_collectors(config)
    for name, collector in collectors.items():
        records = collector.collect(last_run_ts)
"""

from collect.base import BaseCollector, CollectorError
from collect.notes import NotesCollector
from collect.git_collector import GitCollector
from collect.calendar_collector import CalendarCollector
from collect.google_calendar import GoogleCalendarCollector
from collect.google_timeline import GoogleTimelineCollector


_REGISTRY: dict[str, type[BaseCollector]] = {
    "notes": NotesCollector,
    "git": GitCollector,
    "calendar": CalendarCollector,
    "google_calendar": GoogleCalendarCollector,
    "google_timeline": GoogleTimelineCollector,
}


def get_all_collectors(config: dict) -> dict[str, BaseCollector]:
    """Instantiate and return all registered collectors."""
    return {name: cls(config) for name, cls in _REGISTRY.items()}


def get_collector(name: str, config: dict) -> BaseCollector:
    if name not in _REGISTRY:
        raise ValueError(f"Unknown collector: {name}. Available: {list(_REGISTRY.keys())}")
    return _REGISTRY[name](config)


__all__ = [
    "BaseCollector",
    "CollectorError",
    "NotesCollector",
    "GitCollector",
    "CalendarCollector",
    "GoogleCalendarCollector",
    "GoogleTimelineCollector",
    "get_all_collectors",
    "get_collector",
]
