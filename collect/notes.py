"""
collect/notes.py - Collects plain text, Markdown, and Jupyter notebook notes.

Each file (or paragraph within a file) becomes one raw record.
Files are read incrementally based on mtime vs last_run_timestamp.
Supported formats: .txt, .md, .ipynb
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dateutil import parser as dateutil_parser
from loguru import logger

from collect.base import BaseCollector

# Regex to detect a YAML frontmatter date field
_DATE_RE = re.compile(r"^date:\s*(.+)$", re.MULTILINE | re.IGNORECASE)
# Regex: first line looks like a date (YYYY-MM-DD or similar)
_FIRST_LINE_DATE_RE = re.compile(r"^\d{4}[-/]\d{2}[-/]\d{2}")
# YAML frontmatter block
_FRONTMATTER_RE = re.compile(r"^---\s*\n.*?\n---\s*\n", re.DOTALL)


def _parse_date_from_text(text: str) -> Optional[datetime]:
    """Try to extract a date from the beginning of note text."""
    m = _DATE_RE.search(text[:500])
    if m:
        try:
            return dateutil_parser.parse(m.group(1).strip())
        except (ValueError, OverflowError):
            pass
    first_line = text.strip().split("\n")[0].strip()
    if _FIRST_LINE_DATE_RE.match(first_line):
        try:
            return dateutil_parser.parse(first_line[:20])
        except (ValueError, OverflowError):
            pass
    return None


def _strip_frontmatter(content: str) -> str:
    return _FRONTMATTER_RE.sub("", content).strip()


def _extract_tags_from_markdown(content: str) -> list[str]:
    """Extract #hashtags from Markdown content."""
    return list({m.lower() for m in re.findall(r"#([a-zA-Z][a-zA-Z0-9_-]*)", content)})


def _extract_ipynb_text(raw_content: str) -> str:
    """
    Extract readable text from a Jupyter notebook (.ipynb) JSON.
    Combines markdown cells (full text) and code cells (source code).
    Ignores output blobs, widget state, and binary data.
    """
    try:
        nb = json.loads(raw_content)
    except (json.JSONDecodeError, ValueError):
        return ""

    parts = []
    for cell in nb.get("cells", []):
        cell_type = cell.get("cell_type", "")
        source = cell.get("source", [])
        # source can be a list of lines or a single string
        if isinstance(source, list):
            source = "".join(source)
        source = source.strip()
        if not source:
            continue

        if cell_type == "markdown":
            parts.append(source)
        elif cell_type == "code":
            # Skip cells that are just imports or magic commands
            if source.startswith(("import ", "from ", "%", "!")):
                # Still include but compact
                parts.append(f"[code] {source[:200]}")
            else:
                parts.append(f"[code] {source[:500]}")
        # Ignore raw cells

    return "\n\n".join(parts)


def _split_into_chunks(content: str, max_chars: int = 800) -> list[str]:
    """Split large content into paragraph-sized chunks."""
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", content) if p.strip()]
    chunks = []
    current = ""
    for para in paragraphs:
        if len(current) + len(para) > max_chars and current:
            chunks.append(current.strip())
            current = para
        else:
            current = (current + "\n\n" + para) if current else para
    if current:
        chunks.append(current.strip())
    return chunks if chunks else [content[:max_chars]]


class NotesCollector(BaseCollector):
    source_name = "notes"

    def collect(self, last_run_timestamp: Optional[str] = None) -> list[dict]:
        notes_dir = Path(self.config["paths"]["input_dir"]) / self.collect_config.get("directory", "input/notes").replace("input/", "")
        extensions = self.collect_config.get("extensions", [".txt", ".md"])

        if not notes_dir.exists():
            logger.warning(f"Notes directory not found: {notes_dir}")
            return []

        # Parse cutoff for incremental import
        cutoff_ts: Optional[float] = None
        if last_run_timestamp:
            try:
                cutoff_ts = dateutil_parser.parse(last_run_timestamp).timestamp()
            except (ValueError, OverflowError):
                pass

        records = []
        files = [f for f in notes_dir.rglob("*") if f.suffix.lower() in extensions and f.is_file()]
        logger.info(f"[notes] Found {len(files)} note files")

        for filepath in files:
            try:
                mtime = filepath.stat().st_mtime
                if cutoff_ts and mtime <= cutoff_ts:
                    continue

                # Try UTF-8 first, fallback to latin-1
                try:
                    raw_content = filepath.read_text(encoding="utf-8")
                except UnicodeDecodeError:
                    raw_content = filepath.read_text(encoding="latin-1")

                # ── Jupyter notebook: extract cell text first ──────────────────
                if filepath.suffix.lower() == ".ipynb":
                    extracted = _extract_ipynb_text(raw_content)
                    if not extracted:
                        logger.debug(f"[notes] Empty notebook: {filepath.name}")
                        continue
                    # Treat extracted text like a markdown file from here on
                    content = extracted
                    # Notebook title: use filename stem as date fallback label
                    timestamp = datetime.fromtimestamp(mtime).isoformat()
                    hashtags = _extract_tags_from_markdown(content)
                    clean_content = content
                else:
                    content = raw_content
                    # Determine timestamp: frontmatter date > first-line date > file mtime
                    date_from_text = _parse_date_from_text(content)
                    timestamp = date_from_text.isoformat() if date_from_text else datetime.fromtimestamp(mtime).isoformat()
                    # Strip frontmatter for content analysis
                    clean_content = _strip_frontmatter(content)
                    hashtags = _extract_tags_from_markdown(content)

                if not clean_content:
                    continue

                # Split large files into paragraph chunks
                chunks = _split_into_chunks(clean_content)
                for i, chunk in enumerate(chunks):
                    # Summary: first non-empty, non-code line of the chunk
                    first_line = chunk.split("\n")[0].strip()
                    # Skip code-prefix lines as summary
                    if first_line.startswith("[code]"):
                        first_line = chunk.split("\n")[1].strip() if "\n" in chunk else chunk[:200]
                    summary = first_line[:200] or chunk[:200]
                    records.append(
                        {
                            "raw_timestamp": timestamp,
                            "raw_summary": summary,
                            "source": "notes",
                            "filename": filepath.name,
                            "full_content": chunk,
                            "hashtags": hashtags,
                            "chunk_index": i,
                            "chunk_total": len(chunks),
                            "file_type": filepath.suffix.lower().lstrip("."),
                        }
                    )
            except PermissionError as e:
                logger.warning(f"[notes] Permission denied: {filepath} - {e}")
            except Exception as e:
                logger.warning(f"[notes] Failed to read {filepath}: {e}")

        logger.info(f"[notes] Collected {len(records)} records")
        return records
