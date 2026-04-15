"""
normalize.py - Convert raw collector records into canonical events.

Input:  raw/*.json files (output of each collector)
Output: data/events.jsonl (append-only, one JSON per line)
        SQLite events table (via storage.py)

Run:
    python normalize.py                # Process all raw files
    python normalize.py --dry-run      # Print events without saving
    python normalize.py --source git   # Only process one source
"""

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import click
import yaml
from dateutil import parser as dateutil_parser
from loguru import logger

import storage

# ─── Canonical types ──────────────────────────────────────────────────────────

VALID_TYPES = {"work", "location", "health", "social", "note", "unknown"}
VALID_SOURCES = {"git", "notes", "calendar", "google_calendar", "google_timeline", "manual"}

SOURCE_TO_TYPE = {
    "git": "work",
    "notes": "note",
    "calendar": "note",
    "google_calendar": "note",
    "google_timeline": "location",
    "manual": "note",
}

# Location subtypes based on place tags
PLACE_TAG_TO_TYPE = {
    "health": "health",
    "social": "social",
    "work": "work",
}


# ─── Tag extraction ───────────────────────────────────────────────────────────

def extract_tags(raw: dict, source: str) -> list[str]:
    tags = {source}

    if source == "git":
        repo = raw.get("repo_name", "")
        if repo:
            tags.add(repo.lower().replace(" ", "-"))
        tags.add("coding")

    elif source == "notes":
        for tag in raw.get("hashtags", []):
            tags.add(tag.lstrip("#").lower())
        filename = raw.get("filename", "")
        if filename:
            stem = Path(filename).stem.lower()
            # Date-named files (2026-04-14) shouldn't become tags
            if not stem[:4].isdigit():
                tags.add(stem)

    elif source in ("calendar", "google_calendar"):
        title = raw.get("title", "")
        # Extract meaningful words from title
        stop_words = {"a", "an", "the", "at", "in", "on", "with", "for", "to", "of", "and"}
        for word in title.lower().split():
            word = word.strip(".,!?()[]")
            if len(word) > 3 and word not in stop_words:
                tags.add(word)
        if raw.get("is_recurring"):
            tags.add("recurring")
        if raw.get("location"):
            tags.add("in-person")
        if raw.get("has_attendees"):
            tags.add("meeting")
        if raw.get("calendar_name"):
            cal_tag = raw["calendar_name"].lower().replace(" ", "-")[:20]
            tags.add(f"cal-{cal_tag}")

    elif source == "google_timeline":
        for tag in raw.get("place_tags", []):
            tags.add(tag)
        tags.add("location")

    return sorted(tags - {""})


# ─── Importance scoring ───────────────────────────────────────────────────────

def score_importance(raw: dict, source: str, config: dict) -> float:
    imp_cfg = config["normalize"]["importance"]

    base_map = {
        "git": imp_cfg.get("git_commit_base", 0.6),
        "notes": imp_cfg.get("note_base", 0.7),
        "calendar": imp_cfg.get("calendar_base", 0.5),
        "google_timeline": imp_cfg.get("location_base", 0.4),
        "manual": 0.8,
    }
    score = base_map.get(source, 0.5)

    summary = raw.get("raw_summary", "").lower()

    # Keyword boost (+0.1 per hit, max +0.2)
    keywords = imp_cfg.get("keyword_boost", [])
    hits = sum(1 for kw in keywords if kw in summary)
    score += min(hits * 0.1, 0.2)

    # Length signal
    if len(raw.get("raw_summary", "")) > 150:
        score += 0.05

    # Source-specific
    if source == "git":
        lines = raw.get("lines_changed", 0)
        if lines > 200:
            score += 0.1
        elif lines > 50:
            score += 0.05
        if raw.get("branch") in ("main", "master"):
            score += 0.05

    elif source == "google_timeline":
        duration = raw.get("duration_minutes", 0)
        if duration > 60:
            score += 0.1
        confidence = raw.get("visit_confidence", 1.0)
        score *= max(0.3, float(confidence))

    elif source == "calendar":
        if raw.get("is_recurring"):
            score -= 0.1
        if raw.get("is_all_day"):
            score -= 0.05

    return max(0.0, min(1.0, round(score, 3)))


# ─── Summary building ─────────────────────────────────────────────────────────

def build_summary(raw: dict, source: str) -> str:
    if source == "git":
        return raw.get("raw_summary", "")[:200]

    elif source == "notes":
        text = raw.get("raw_summary", raw.get("full_content", ""))
        # Skip bare date lines
        first = text.strip().split("\n")[0].strip()
        if len(first) > 5 and not first[:10].replace("-", "").isdigit():
            return first[:200]
        # Fall back to full content snippet
        return raw.get("full_content", text)[:200].strip()

    elif source in ("calendar", "google_calendar"):
        title = raw.get("title", raw.get("raw_summary", "Untitled"))
        loc = raw.get("location", "")
        duration = raw.get("duration_minutes")
        cal = raw.get("calendar_name", "")
        parts = [title]
        if loc:
            parts.append(f"@ {loc}")
        if duration:
            parts.append(f"({duration}min)")
        if cal and cal.lower() not in title.lower():
            parts.append(f"[{cal}]")
        return " ".join(parts)[:200]

    elif source == "google_timeline":
        place = raw.get("place_name", "Unknown place")
        duration = raw.get("duration_minutes", 0)
        summary = f"Visited {place}"
        if duration:
            summary += f" ({duration}min)"
        return summary[:200]

    return raw.get("raw_summary", "")[:200]


# ─── Type detection ───────────────────────────────────────────────────────────

def detect_type(raw: dict, source: str) -> str:
    # Location source: use place_tags to refine type
    if source == "google_timeline":
        for tag in raw.get("place_tags", []):
            if tag in PLACE_TAG_TO_TYPE:
                return PLACE_TAG_TO_TYPE[tag]
        return "location"

    return SOURCE_TO_TYPE.get(source, "unknown")


# ─── ID and hash generation ───────────────────────────────────────────────────

def make_content_hash(date_only: str, source: str, summary: str) -> str:
    """Deterministic deduplication hash. Uses date (not full time) to tolerate
    slight timestamp differences on re-import."""
    normalized = summary.lower().strip()[:200]
    raw_str = f"{date_only}|{source}|{normalized}"
    return hashlib.sha256(raw_str.encode("utf-8")).hexdigest()[:32]


def make_event_id(timestamp: str, source: str, content_hash: str) -> str:
    date_part = timestamp[:10].replace("-", "")
    time_part = timestamp[11:19].replace(":", "")
    return f"evt_{date_part}_{time_part}_{content_hash[:6]}"


# ─── Core normalizer ─────────────────────────────────────────────────────────

def normalize_record(raw: dict, source: str, config: dict) -> Optional[dict]:
    """Convert a single raw record into a canonical event dict. Returns None if invalid."""
    raw_ts = raw.get("raw_timestamp")
    if not raw_ts:
        return None

    try:
        dt = dateutil_parser.parse(str(raw_ts))
        # Normalize to local naive datetime
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S")
        timestamp_unix = int(dt.timestamp())
        date_only = timestamp[:10]
    except (ValueError, OverflowError, OSError) as e:
        logger.debug(f"Could not parse timestamp '{raw_ts}': {e}")
        return None

    summary = build_summary(raw, source)
    if not summary or len(summary.strip()) < 3:
        return None

    content_hash = make_content_hash(date_only, source, summary)
    event_id = make_event_id(timestamp, source, content_hash)
    event_type = detect_type(raw, source)
    tags = extract_tags(raw, source)
    importance = score_importance(raw, source, config)

    # Metadata: keep source-specific fields (exclude fields already in canonical schema)
    exclude_keys = {"raw_timestamp", "raw_summary", "source"}
    metadata = {k: v for k, v in raw.items() if k not in exclude_keys}

    return {
        "id": event_id,
        "timestamp": timestamp,
        "timestamp_unix": timestamp_unix,
        "type": event_type,
        "source": source,
        "summary": summary,
        "metadata": metadata,
        "tags": tags,
        "importance": importance,
        "content_hash": content_hash,
    }


def normalize_all(
    raw_dir: str,
    events_file: str,
    conn,
    config: dict,
    source_filter: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """
    Load all raw/*.json files, normalize, and write to events.jsonl + SQLite.
    Returns stats dict.
    """
    raw_path = Path(raw_dir)
    events_path = Path(events_file)
    events_path.parent.mkdir(parents=True, exist_ok=True)

    # Load existing content hashes for in-memory dedup
    existing_hashes: set[str] = set()
    if events_path.exists():
        with open(events_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        ev = json.loads(line)
                        existing_hashes.add(ev.get("content_hash", ""))
                    except json.JSONDecodeError:
                        pass

    stats = {"sources_processed": 0, "records_read": 0, "events_new": 0, "events_skipped": 0, "errors": 0}
    new_events = []

    # Discover raw JSON files
    raw_files = list(raw_path.glob("*.json"))
    if source_filter:
        raw_files = [f for f in raw_files if source_filter in f.stem]

    for raw_file in raw_files:
        source = raw_file.stem  # e.g. "git", "notes", "calendar", "google_timeline"
        if source not in VALID_SOURCES:
            logger.debug(f"Skipping unknown source file: {raw_file.name}")
            continue

        try:
            with open(raw_file, encoding="utf-8") as f:
                raw_records = json.load(f)
        except Exception as e:
            logger.warning(f"Could not read {raw_file}: {e}")
            stats["errors"] += 1
            continue

        stats["sources_processed"] += 1
        stats["records_read"] += len(raw_records)

        for raw in raw_records:
            event = normalize_record(raw, source, config)
            if event is None:
                stats["errors"] += 1
                continue

            if event["content_hash"] in existing_hashes:
                stats["events_skipped"] += 1
                continue

            existing_hashes.add(event["content_hash"])
            new_events.append(event)
            stats["events_new"] += 1

    if dry_run:
        for ev in new_events[:20]:
            print(json.dumps(ev, ensure_ascii=False, indent=2))
        if len(new_events) > 20:
            print(f"\n... and {len(new_events) - 20} more events")
        return stats

    # Write to JSONL
    if new_events:
        with open(events_path, "a", encoding="utf-8") as f:
            for ev in new_events:
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")

        # Insert into SQLite
        inserted, skipped = storage.insert_events_batch(conn, new_events)
        stats["events_new"] = inserted
        stats["events_skipped"] += skipped

    return stats


# ─── CLI ─────────────────────────────────────────────────────────────────────

@click.command()
@click.option("--config", "config_path", default="config.yaml", help="Path to config.yaml")
@click.option("--source", default=None, help="Only normalize this source (git/notes/calendar/google_timeline)")
@click.option("--dry-run", is_flag=True, help="Print events without saving")
def main(config_path: str, source: Optional[str], dry_run: bool):
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    conn = storage.init_db(config["paths"]["sqlite_db"])

    logger.info(f"Normalizing raw data (source={source or 'all'}, dry_run={dry_run})")
    stats = normalize_all(
        raw_dir=config["paths"]["raw_dir"],
        events_file=config["paths"]["events_file"],
        conn=conn,
        config=config,
        source_filter=source,
        dry_run=dry_run,
    )

    total_events = storage.get_events_count(conn)
    logger.info(
        f"Done: {stats['events_new']} new, {stats['events_skipped']} skipped, "
        f"{stats['errors']} errors | Total in DB: {total_events}"
    )


if __name__ == "__main__":
    main()
