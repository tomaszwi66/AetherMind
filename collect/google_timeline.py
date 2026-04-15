"""
collect/google_timeline.py - Parses Google Takeout Timeline JSON exports.

Handles both formats:
- Old (pre-2024): top-level key 'timelineObjects' with placeVisit/activitySegment
- New (2024+): top-level key 'semanticSegments' with visit/activity

Uses ijson streaming for large files (>50MB) to avoid memory spikes.
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from loguru import logger

from collect.base import BaseCollector

# Location category mapping for tag inference
_PLACE_TAG_PATTERNS = {
    "gym": "health", "siłownia": "health", "fitness": "health", "sport": "health",
    "hospital": "health", "clinic": "health", "pharmacy": "health", "szpital": "health",
    "restaurant": "social", "cafe": "social", "coffee": "social", "bar": "social",
    "pub": "social", "kawiarnia": "social", "restauracja": "social",
    "office": "work", "biuro": "work", "cowork": "work",
    "library": "focus", "biblioteka": "focus",
    "airport": "travel", "lotnisko": "travel", "train": "travel", "station": "travel",
    "hotel": "travel",
    "school": "education", "university": "education", "uczelnia": "education",
    "park": "nature", "forest": "nature", "lake": "nature",
    "supermarket": "errands", "market": "errands", "sklep": "errands", "mall": "errands",
}


def _infer_place_tags(place_name: str) -> list[str]:
    name_lower = place_name.lower()
    return [tag for keyword, tag in _PLACE_TAG_PATTERNS.items() if keyword in name_lower]


def _ms_to_iso(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()
    except (ValueError, OSError, OverflowError):
        return None


def _parse_old_format(filepath: Path, min_duration: int, cutoff_ts: Optional[float]) -> list[dict]:
    """Parse old Google Timeline format (timelineObjects array)."""
    import json

    file_size = os.path.getsize(filepath)
    records = []

    if file_size > 50 * 1024 * 1024:
        # Streaming parse for large files
        try:
            import ijson
            with open(filepath, "rb") as f:
                objects = ijson.items(f, "timelineObjects.item")
                for obj in objects:
                    rec = _parse_timeline_object(obj, min_duration, cutoff_ts)
                    if rec:
                        records.append(rec)
        except ImportError:
            logger.warning("[timeline] ijson not available for large file - loading fully")
            with open(filepath, encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            for obj in data.get("timelineObjects", []):
                rec = _parse_timeline_object(obj, min_duration, cutoff_ts)
                if rec:
                    records.append(rec)
    else:
        try:
            with open(filepath, encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            for obj in data.get("timelineObjects", []):
                rec = _parse_timeline_object(obj, min_duration, cutoff_ts)
                if rec:
                    records.append(rec)
        except Exception as e:
            logger.warning(f"[timeline] Failed to parse {filepath.name}: {e}")

    return records


def _parse_timeline_object(obj: dict, min_duration: int, cutoff_ts: Optional[float]) -> Optional[dict]:
    if "placeVisit" in obj:
        pv = obj["placeVisit"]
        loc = pv.get("location", {})
        place_name = loc.get("name") or loc.get("address", "Unknown place")

        dur = pv.get("duration", {})
        start_ms = dur.get("startTimestampMs") or dur.get("startTimestamp")
        end_ms = dur.get("endTimestampMs") or dur.get("endTimestamp")

        # Handle ISO string timestamps (newer old-format variants)
        if isinstance(start_ms, str):
            try:
                from dateutil import parser as dp
                start_dt = dp.parse(start_ms)
                start_ms = int(start_dt.timestamp() * 1000)
            except Exception:
                start_ms = None
        if isinstance(end_ms, str):
            try:
                from dateutil import parser as dp
                end_dt = dp.parse(end_ms)
                end_ms = int(end_dt.timestamp() * 1000)
            except Exception:
                end_ms = None

        if start_ms is None:
            return None

        start_ts = int(start_ms) / 1000
        if cutoff_ts and start_ts <= cutoff_ts:
            return None

        duration_min = 0
        if end_ms:
            duration_min = max(0, int((int(end_ms) - int(start_ms)) / 60000))

        if duration_min < min_duration and min_duration > 0:
            return None

        ts_iso = _ms_to_iso(int(start_ms))
        if not ts_iso:
            return None

        lat = loc.get("latitudeE7", 0) / 1e7 if loc.get("latitudeE7") else None
        lon = loc.get("longitudeE7", 0) / 1e7 if loc.get("longitudeE7") else None
        confidence = pv.get("visitConfidence") or pv.get("placeConfidence") or 1.0
        if isinstance(confidence, str):
            confidence_map = {"HIGH_CONFIDENCE": 0.9, "MEDIUM_CONFIDENCE": 0.6, "LOW_CONFIDENCE": 0.3}
            confidence = confidence_map.get(confidence, 0.7)

        return {
            "raw_timestamp": ts_iso,
            "raw_summary": f"Visited {place_name}",
            "source": "google_timeline",
            "place_name": place_name,
            "address": loc.get("address", ""),
            "duration_minutes": duration_min,
            "lat": lat,
            "lon": lon,
            "visit_confidence": float(confidence),
            "place_tags": _infer_place_tags(place_name),
        }

    elif "activitySegment" in obj:
        seg = obj["activitySegment"]
        activity_type = seg.get("activityType", "UNKNOWN")
        dur = seg.get("duration", {})
        start_ms = dur.get("startTimestampMs") or dur.get("startTimestamp")

        if not start_ms or isinstance(start_ms, str):
            return None

        start_ts = int(start_ms) / 1000
        if cutoff_ts and start_ts <= cutoff_ts:
            return None

        distance_m = seg.get("distance", 0)
        end_ms = dur.get("endTimestampMs") or dur.get("endTimestamp")
        duration_min = max(0, int((int(end_ms or start_ms) - int(start_ms)) / 60000))

        if duration_min < min_duration and min_duration > 0:
            return None

        activity_labels = {
            "WALKING": "Walked", "RUNNING": "Ran", "CYCLING": "Cycled",
            "IN_VEHICLE": "Traveled by vehicle", "IN_BUS": "Traveled by bus",
            "IN_TRAIN": "Traveled by train", "IN_SUBWAY": "Traveled by subway",
            "FLYING": "Flew", "SKIING": "Skied", "STILL": "Was stationary",
        }
        label = activity_labels.get(activity_type, f"Activity: {activity_type}")
        if distance_m > 0:
            label += f" ({distance_m/1000:.1f} km)"

        return {
            "raw_timestamp": _ms_to_iso(int(start_ms)),
            "raw_summary": label,
            "source": "google_timeline",
            "place_name": activity_type,
            "address": "",
            "duration_minutes": duration_min,
            "lat": None,
            "lon": None,
            "visit_confidence": 1.0,
            "place_tags": ["transport"] if "VEHICLE" in activity_type or "TRAIN" in activity_type else ["activity"],
        }

    return None


def _parse_new_format(filepath: Path, min_duration: int, cutoff_ts: Optional[float]) -> list[dict]:
    """Parse new Google Timeline format (semanticSegments array)."""
    import json

    records = []
    try:
        with open(filepath, encoding="utf-8", errors="replace") as f:
            data = json.load(f)

        for segment in data.get("semanticSegments", []):
            if "visit" not in segment:
                continue
            visit = segment["visit"]
            candidate = visit.get("topCandidate", {})
            place_name = candidate.get("placeId", "Unknown place")
            # Newer format may include semanticType
            semantic_type = candidate.get("semanticType", "")
            if semantic_type:
                place_name = semantic_type.replace("_", " ").title()

            start_time = segment.get("startTime", "")
            end_time = segment.get("endTime", "")

            if not start_time:
                continue

            try:
                from dateutil import parser as dp
                start_dt = dp.parse(start_time)
                start_ts = start_dt.timestamp()
            except Exception:
                continue

            if cutoff_ts and start_ts <= cutoff_ts:
                continue

            duration_min = 0
            if end_time:
                try:
                    end_dt = dp.parse(end_time)
                    duration_min = max(0, int((end_dt - start_dt).total_seconds() / 60))
                except Exception:
                    pass

            if duration_min < min_duration and min_duration > 0:
                continue

            probability = visit.get("probability", 1.0)

            records.append(
                {
                    "raw_timestamp": start_dt.isoformat(),
                    "raw_summary": f"Visited {place_name}",
                    "source": "google_timeline",
                    "place_name": place_name,
                    "address": "",
                    "duration_minutes": duration_min,
                    "lat": None,
                    "lon": None,
                    "visit_confidence": float(probability),
                    "place_tags": _infer_place_tags(place_name),
                }
            )
    except Exception as e:
        logger.warning(f"[timeline] Failed to parse new-format {filepath.name}: {e}")

    return records


class GoogleTimelineCollector(BaseCollector):
    source_name = "google_timeline"

    def collect(self, last_run_timestamp: Optional[str] = None) -> list[dict]:
        timeline_dir = Path(self.config["paths"]["input_dir"]) / "Semantic_Location_History"
        min_duration = self.collect_config.get("min_duration_minutes", 5)

        if not timeline_dir.exists():
            logger.info(f"[timeline] Directory not found: {timeline_dir} - skipping")
            return []

        cutoff_ts: Optional[float] = None
        if last_run_timestamp:
            try:
                from dateutil import parser as dp
                cutoff_ts = dp.parse(last_run_timestamp).timestamp()
            except Exception:
                pass

        json_files = sorted(timeline_dir.rglob("*.json"))
        if not json_files:
            logger.info("[timeline] No JSON files found")
            return []

        logger.info(f"[timeline] Processing {len(json_files)} JSON files")
        all_records = []

        for filepath in json_files:
            try:
                # Detect format
                import json as _json
                with open(filepath, encoding="utf-8", errors="replace") as f:
                    peek = f.read(200)

                if "timelineObjects" in peek:
                    records = _parse_old_format(filepath, min_duration, cutoff_ts)
                elif "semanticSegments" in peek:
                    records = _parse_new_format(filepath, min_duration, cutoff_ts)
                else:
                    logger.debug(f"[timeline] Unknown format in {filepath.name}, skipping")
                    continue

                all_records.extend(records)
                logger.debug(f"[timeline] {filepath.name}: {len(records)} records")
            except Exception as e:
                logger.warning(f"[timeline] Error processing {filepath.name}: {e}")

        logger.info(f"[timeline] Total collected: {len(all_records)} location events")
        return all_records
