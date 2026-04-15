"""
collect/calendar_collector.py - Collects events from a CSV calendar export.

Handles Google Calendar, Outlook, and Apple Calendar CSV formats by
trying multiple known column name aliases. Falls back gracefully.
"""

from pathlib import Path
from typing import Optional

from loguru import logger

from collect.base import BaseCollector, CollectorError

# Known column aliases for auto-detection
_DATE_ALIASES = ["Start Date", "DTSTART", "Start", "Date", "StartDate", "start_date", "datetime"]
_TITLE_ALIASES = ["Subject", "Summary", "Title", "Event Name", "Name", "title", "summary", "subject"]
_END_ALIASES = ["End Date", "DTEND", "End", "EndDate", "end_date"]
_DESC_ALIASES = ["Description", "Body", "Notes", "description", "notes", "body"]
_LOCATION_ALIASES = ["Location", "location", "Place", "place"]


def _find_column(df_columns: list[str], aliases: list[str]) -> Optional[str]:
    for alias in aliases:
        if alias in df_columns:
            return alias
    # Case-insensitive fallback
    lower_cols = {c.lower(): c for c in df_columns}
    for alias in aliases:
        if alias.lower() in lower_cols:
            return lower_cols[alias.lower()]
    return None


class CalendarCollector(BaseCollector):
    source_name = "calendar"

    def collect(self, last_run_timestamp: Optional[str] = None) -> list[dict]:
        try:
            import pandas as pd
            from dateutil import parser as dp
        except ImportError as e:
            raise CollectorError(f"Missing dependency: {e}")

        csv_path = Path(self.config["paths"]["input_dir"]) / "calendar.csv"
        if not csv_path.exists():
            logger.info(f"[calendar] No CSV found at {csv_path} - skipping")
            return []

        try:
            df = pd.read_csv(csv_path, encoding="utf-8", on_bad_lines="skip")
        except UnicodeDecodeError:
            df = pd.read_csv(csv_path, encoding="latin-1", on_bad_lines="skip")
        except Exception as e:
            raise CollectorError(f"Failed to read calendar CSV: {e}")

        columns = list(df.columns)

        # Auto-detect or use config overrides
        cfg = self.collect_config
        date_col = cfg.get("date_column") or _find_column(columns, _DATE_ALIASES)
        title_col = cfg.get("title_column") or _find_column(columns, _TITLE_ALIASES)
        end_col = _find_column(columns, _END_ALIASES)
        desc_col = _find_column(columns, _DESC_ALIASES)
        loc_col = _find_column(columns, _LOCATION_ALIASES)

        if not date_col or not title_col:
            raise CollectorError(
                f"Calendar CSV columns not recognized. Found: {columns}\n"
                f"Set 'date_column' and 'title_column' in config.yaml under collect.calendar"
            )

        logger.info(f"[calendar] Using columns: date={date_col}, title={title_col}")

        # Cutoff for incremental import
        cutoff_ts: Optional[float] = None
        if last_run_timestamp:
            try:
                cutoff_ts = dp.parse(last_run_timestamp).timestamp()
            except Exception:
                pass

        records = []
        for _, row in df.iterrows():
            try:
                raw_date = row[date_col]
                if pd.isna(raw_date) or str(raw_date).strip() == "":
                    continue

                dt = pd.to_datetime(raw_date, infer_datetime_format=True, errors="coerce")
                if pd.isna(dt):
                    continue

                ts = dt.timestamp()
                if cutoff_ts and ts <= cutoff_ts:
                    continue

                title = str(row[title_col]).strip() if not pd.isna(row[title_col]) else "Untitled"

                # Compute duration if end column available
                duration_min = None
                is_all_day = False
                if end_col and end_col in row and not pd.isna(row[end_col]):
                    end_dt = pd.to_datetime(row[end_col], errors="coerce")
                    if not pd.isna(end_dt):
                        delta = (end_dt - dt).total_seconds() / 60
                        duration_min = max(0, int(delta))
                        if duration_min >= 1440:
                            is_all_day = True

                description = ""
                if desc_col and desc_col in row and not pd.isna(row[desc_col]):
                    description = str(row[desc_col])[:500]

                location = ""
                if loc_col and loc_col in row and not pd.isna(row[loc_col]):
                    location = str(row[loc_col]).strip()

                # Detect recurring events: same title appears >1 time
                # (will be set post-processing below)

                records.append(
                    {
                        "raw_timestamp": dt.isoformat(),
                        "raw_summary": title,
                        "source": "calendar",
                        "title": title,
                        "duration_minutes": duration_min,
                        "is_all_day": is_all_day,
                        "description": description,
                        "location": location,
                        "is_recurring": False,  # updated below
                    }
                )
            except Exception as e:
                logger.debug(f"[calendar] Skipping row: {e}")

        # Mark recurring events
        from collections import Counter
        title_counts = Counter(r["title"] for r in records)
        for r in records:
            if title_counts[r["title"]] > 2:
                r["is_recurring"] = True

        logger.info(f"[calendar] Collected {len(records)} events")
        return records
