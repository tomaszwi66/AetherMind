"""
collect/base.py - Abstract base class for all data collectors.
Collectors must NEVER crash the pipeline. They catch exceptions internally
and return an empty list with a warning on failure.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional
import json
import tempfile
import os

from loguru import logger


class CollectorError(Exception):
    """Raised when a collector encounters an unrecoverable configuration error."""


class BaseCollector(ABC):
    """
    All collectors implement this interface.

    Each collector:
    - Reads from a specific source (notes dir, git repo, CSV file, JSON dir)
    - Returns a list of raw dicts (source-specific schema)
    - Must include at minimum: 'raw_timestamp', 'raw_summary', 'source'
    - Must not raise exceptions to the caller (handle internally)
    """

    source_name: str = "unknown"

    def __init__(self, config: dict):
        self.config = config
        self.collect_config = config.get("collect", {}).get(self.source_name, {})

    @abstractmethod
    def collect(self, last_run_timestamp: Optional[str] = None) -> list[dict]:
        """
        Collect raw records from the source.

        Args:
            last_run_timestamp: ISO 8601 string of last successful import.
                                Use this for incremental imports (skip older data).
                                If None, collect everything available.

        Returns:
            List of raw dicts. Each must have:
                - raw_timestamp: str (any parseable date format)
                - raw_summary: str (human-readable description)
                - source: str (matches self.source_name)

        Never raises. Returns [] on failure after logging the error.
        """

    def save(self, records: list[dict], output_path: str) -> None:
        """Write records atomically to output_path as JSON array."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Atomic write via temp file in same directory
        fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2, default=str)
            os.replace(tmp_path, path)
            logger.debug(f"Saved {len(records)} records to {path}")
        except Exception as e:
            os.unlink(tmp_path)
            raise CollectorError(f"Failed to save records to {path}: {e}") from e

    def _safe_collect(self, last_run_timestamp: Optional[str] = None) -> list[dict]:
        """Wrapper that catches all exceptions and returns [] on failure."""
        try:
            return self.collect(last_run_timestamp)
        except CollectorError:
            raise
        except Exception as e:
            logger.warning(f"[{self.source_name}] Collection failed: {e}")
            return []
