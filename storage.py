"""
storage.py - SQLite + Qdrant abstraction layer.
Single source of truth for all persistence operations.
Never access SQLite or Qdrant directly from other modules.
"""

import json
import sqlite3
import uuid
from pathlib import Path
from typing import Optional

from loguru import logger
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    FieldCondition,
    Filter,
    HnswConfigDiff,
    MatchValue,
    OptimizersConfigDiff,
    PointStruct,
    Range,
    ScalarQuantization,
    ScalarType,
    VectorParams,
)

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS events (
    id              TEXT PRIMARY KEY,
    timestamp       TEXT NOT NULL,
    timestamp_unix  INTEGER NOT NULL,
    type            TEXT NOT NULL CHECK(type IN ('work','location','health','social','note','unknown')),
    source          TEXT NOT NULL CHECK(source IN ('git','notes','calendar','google_calendar','google_timeline','manual')),
    summary         TEXT NOT NULL,
    metadata        TEXT NOT NULL DEFAULT '{}',
    tags            TEXT NOT NULL DEFAULT '[]',
    importance      REAL NOT NULL DEFAULT 0.5 CHECK(importance >= 0.0 AND importance <= 1.0),
    embedding_id    TEXT,
    indexed_at      TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now')),
    content_hash    TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_events_timestamp     ON events(timestamp_unix);
CREATE INDEX IF NOT EXISTS idx_events_type          ON events(type);
CREATE INDEX IF NOT EXISTS idx_events_source        ON events(source);
CREATE INDEX IF NOT EXISTS idx_events_importance    ON events(importance DESC);
CREATE INDEX IF NOT EXISTS idx_events_date          ON events(substr(timestamp, 1, 10));
CREATE INDEX IF NOT EXISTS idx_events_unindexed     ON events(indexed_at) WHERE indexed_at IS NULL;

CREATE TABLE IF NOT EXISTS event_tags (
    event_id        TEXT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    tag             TEXT NOT NULL,
    PRIMARY KEY (event_id, tag)
);

CREATE INDEX IF NOT EXISTS idx_event_tags_tag ON event_tags(tag);

CREATE TABLE IF NOT EXISTS reflections (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT NOT NULL UNIQUE,
    summary         TEXT NOT NULL,
    wins            TEXT NOT NULL DEFAULT '[]',
    risks           TEXT NOT NULL DEFAULT '[]',
    patterns        TEXT NOT NULL DEFAULT '[]',
    theme           TEXT,
    mood            TEXT,
    event_count     INTEGER NOT NULL DEFAULT 0,
    model_used      TEXT,
    raw_response    TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_reflections_date ON reflections(date);

CREATE TABLE IF NOT EXISTS import_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    events_found    INTEGER DEFAULT 0,
    events_inserted INTEGER DEFAULT 0,
    events_skipped  INTEGER DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'running'
                    CHECK(status IN ('running','success','failed')),
    error_message   TEXT
);
"""


# ─── SQLite ───────────────────────────────────────────────────────────────────

def init_db(db_path: str) -> sqlite3.Connection:
    """Initialize SQLite database with full schema. Safe to call repeatedly (idempotent)."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    logger.info(f"DB initialized: {db_path}")
    return conn


def insert_events_batch(conn: sqlite3.Connection, events: list[dict]) -> tuple[int, int]:
    """Insert a batch of canonical events. Returns (inserted, skipped) counts."""
    inserted = 0
    skipped = 0
    with conn:
        for ev in events:
            try:
                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO events
                        (id, timestamp, timestamp_unix, type, source, summary,
                         metadata, tags, importance, content_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ev["id"],
                        ev["timestamp"],
                        ev["timestamp_unix"],
                        ev["type"],
                        ev["source"],
                        ev["summary"],
                        json.dumps(ev.get("metadata", {})),
                        json.dumps(ev.get("tags", [])),
                        ev.get("importance", 0.5),
                        ev["content_hash"],
                    ),
                )
                if cursor.rowcount == 1:
                    # Insert tags into the lookup table
                    for tag in ev.get("tags", []):
                        conn.execute(
                            "INSERT OR IGNORE INTO event_tags (event_id, tag) VALUES (?, ?)",
                            (ev["id"], tag),
                        )
                    inserted += 1
                else:
                    skipped += 1
            except sqlite3.Error as e:
                logger.warning(f"Failed to insert event {ev.get('id')}: {e}")
                skipped += 1
    return inserted, skipped


def get_events_for_date(conn: sqlite3.Connection, date: str) -> list[dict]:
    """Return all events for a specific date (YYYY-MM-DD), ordered by timestamp."""
    rows = conn.execute(
        "SELECT * FROM events WHERE substr(timestamp, 1, 10) = ? ORDER BY timestamp_unix",
        (date,),
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_events_for_range(conn: sqlite3.Connection, start_unix: int, end_unix: int) -> list[dict]:
    """Return events within a unix timestamp range."""
    rows = conn.execute(
        "SELECT * FROM events WHERE timestamp_unix BETWEEN ? AND ? ORDER BY timestamp_unix",
        (start_unix, end_unix),
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_unindexed_events(conn: sqlite3.Connection, limit: int = 2000) -> list[dict]:
    """Return events not yet embedded into Qdrant."""
    rows = conn.execute(
        "SELECT * FROM events WHERE indexed_at IS NULL ORDER BY timestamp_unix LIMIT ?",
        (limit,),
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_events_count(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]


def get_indexed_count(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM events WHERE indexed_at IS NOT NULL").fetchone()[0]


def mark_events_indexed(
    conn: sqlite3.Connection, event_ids: list[str], embedding_ids: list[str]
) -> None:
    """Update indexed_at and embedding_id for a batch of events."""
    with conn:
        conn.executemany(
            "UPDATE events SET indexed_at = strftime('%Y-%m-%dT%H:%M:%S', 'now'), embedding_id = ? WHERE id = ?",
            [(eid, evid) for eid, evid in zip(embedding_ids, event_ids)],
        )


def get_event_by_id(conn: sqlite3.Connection, event_id: str) -> Optional[dict]:
    row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
    return _row_to_dict(row) if row else None


def get_reflection_for_date(conn: sqlite3.Connection, date: str) -> Optional[dict]:
    row = conn.execute("SELECT * FROM reflections WHERE date = ?", (date,)).fetchone()
    return dict(row) if row else None


def get_reflections_for_range(conn: sqlite3.Connection, start: str, end: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM reflections WHERE date BETWEEN ? AND ? ORDER BY date",
        (start, end),
    ).fetchall()
    return [dict(r) for r in rows]


def save_reflection(conn: sqlite3.Connection, reflection: dict) -> None:
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO reflections
                (date, summary, wins, risks, patterns, theme, mood, event_count, model_used, raw_response)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                reflection["date"],
                reflection.get("summary", ""),
                json.dumps(reflection.get("wins", [])),
                json.dumps(reflection.get("risks", [])),
                json.dumps(reflection.get("patterns", [])),
                reflection.get("theme"),
                reflection.get("mood"),
                reflection.get("event_count", 0),
                reflection.get("model_used"),
                reflection.get("raw_response"),
            ),
        )


def start_import_run(conn: sqlite3.Connection, source: str) -> int:
    with conn:
        cursor = conn.execute(
            "INSERT INTO import_runs (source, started_at) VALUES (?, strftime('%Y-%m-%dT%H:%M:%S', 'now'))",
            (source,),
        )
    return cursor.lastrowid


def finish_import_run(
    conn: sqlite3.Connection,
    run_id: int,
    status: str,
    events_found: int = 0,
    events_inserted: int = 0,
    events_skipped: int = 0,
    error_message: Optional[str] = None,
) -> None:
    with conn:
        conn.execute(
            """
            UPDATE import_runs SET
                finished_at = strftime('%Y-%m-%dT%H:%M:%S', 'now'),
                status = ?,
                events_found = ?,
                events_inserted = ?,
                events_skipped = ?,
                error_message = ?
            WHERE id = ?
            """,
            (status, events_found, events_inserted, events_skipped, error_message, run_id),
        )


def get_last_successful_run(conn: sqlite3.Connection, source: str) -> Optional[str]:
    """Return the finished_at timestamp of the last successful import for this source."""
    row = conn.execute(
        "SELECT finished_at FROM import_runs WHERE source = ? AND status = 'success' ORDER BY id DESC LIMIT 1",
        (source,),
    ).fetchone()
    return row[0] if row else None


def _row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    # Deserialize JSON fields
    for field in ("metadata", "tags"):
        if isinstance(d.get(field), str):
            try:
                d[field] = json.loads(d[field])
            except (json.JSONDecodeError, TypeError):
                d[field] = {} if field == "metadata" else []
    return d


# ─── Qdrant ───────────────────────────────────────────────────────────────────

def get_qdrant_client(storage_path: str) -> QdrantClient:
    """Return a Qdrant client using local file-based storage (no Docker required)."""
    Path(storage_path).mkdir(parents=True, exist_ok=True)
    return QdrantClient(path=storage_path)


def ensure_collection(client: QdrantClient, config: dict) -> None:
    """Create the Qdrant collection if it doesn't exist."""
    name = config["qdrant"]["collection_name"]
    vector_size = config["qdrant"]["vector_size"]

    if client.collection_exists(name):
        info = client.get_collection(name)
        existing_size = info.config.params.vectors.size
        if existing_size != vector_size:
            raise RuntimeError(
                f"Qdrant collection '{name}' exists with vector size {existing_size}, "
                f"but config expects {vector_size}. Delete db/qdrant_storage/ and re-index."
            )
        return

    client.create_collection(
        collection_name=name,
        vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
        hnsw_config=HnswConfigDiff(m=16, ef_construct=100, full_scan_threshold=10_000),
        optimizers_config=OptimizersConfigDiff(default_segment_number=2),
        quantization_config=ScalarQuantization(
            scalar={"type": ScalarType.INT8, "quantile": 0.99, "always_ram": True}
        ),
    )

    # Create payload indexes for filtered search (server mode only; local mode ignores silently)
    import warnings
    for field, schema_type in [
        ("type", "keyword"),
        ("source", "keyword"),
        ("tags", "keyword"),
        ("importance", "float"),
    ]:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            client.create_payload_index(
                collection_name=name,
                field_name=field,
                field_schema=schema_type,
            )

    logger.info(f"Qdrant collection '{name}' created (dim={vector_size})")


def upsert_vectors(client: QdrantClient, collection: str, points: list[PointStruct]) -> None:
    """Upsert a batch of vectors into Qdrant."""
    client.upsert(collection_name=collection, points=points)


def search_similar(
    client: QdrantClient,
    collection: str,
    query_vector: list[float],
    top_k: int = 8,
    min_score: float = 0.0,
    filter_type: Optional[str] = None,
    filter_source: Optional[str] = None,
    filter_tags: Optional[list[str]] = None,
    filter_since_unix: Optional[int] = None,
) -> list[dict]:
    """Semantic search with optional payload filters. Returns list of {event_id, score, payload}."""
    conditions = []
    if filter_type:
        conditions.append(FieldCondition(key="type", match=MatchValue(value=filter_type)))
    if filter_source:
        conditions.append(FieldCondition(key="source", match=MatchValue(value=filter_source)))
    if filter_tags:
        for tag in filter_tags:
            conditions.append(FieldCondition(key="tags", match=MatchValue(value=tag)))

    query_filter = Filter(must=conditions) if conditions else None

    # qdrant-client >= 1.10 uses query_points(); older versions used search()
    results = client.query_points(
        collection_name=collection,
        query=query_vector,
        limit=top_k,
        query_filter=query_filter,
        score_threshold=min_score if min_score > 0 else None,
        with_payload=True,
    ).points

    hits = []
    for r in results:
        hits.append(
            {
                "event_id": r.payload.get("event_id"),
                "score": r.score,
                "payload": r.payload,
            }
        )
    return hits


def get_collection_count(client: QdrantClient, collection: str) -> int:
    try:
        # vectors_count can be None in local embedded mode - use count() instead
        result = client.count(collection_name=collection, exact=True)
        return result.count
    except Exception:
        try:
            info = client.get_collection(collection)
            return info.vectors_count or 0
        except Exception:
            return 0


# ─── Self-test ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import tempfile

    print("Testing storage.py...")
    tmp_dir = tempfile.mkdtemp()
    conn = None
    client = None
    try:
        # SQLite test
        conn = init_db(f"{tmp_dir}/memory.db")
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        print(f"Tables: {[t[0] for t in tables]}")

        test_event = {
            "id": "evt_test_001",
            "timestamp": "2026-04-14T09:00:00",
            "timestamp_unix": 1744621200,
            "type": "work",
            "source": "git",
            "summary": "Test commit",
            "metadata": {"repo": "test"},
            "tags": ["git", "test"],
            "importance": 0.7,
            "content_hash": "abc123def456",
        }
        ins, skip = insert_events_batch(conn, [test_event, test_event])
        print(f"Inserted: {ins}, Skipped (duplicate): {skip}")
        print(f"Events for 2026-04-14: {len(get_events_for_date(conn, '2026-04-14'))}")
        print(f"Unindexed events: {len(get_unindexed_events(conn))}")

        # Qdrant test
        client = get_qdrant_client(f"{tmp_dir}/qdrant")
        mock_config = {"qdrant": {"collection_name": "test_col", "vector_size": 4}}
        ensure_collection(client, mock_config)
        upsert_vectors(
            client,
            "test_col",
            [PointStruct(id=str(uuid.uuid4()), vector=[0.1, 0.2, 0.3, 0.4], payload={"event_id": "evt_test_001"})],
        )
        print(f"Qdrant upsert OK")
    finally:
        if client:
            client.close()
        if conn:
            conn.close()
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)

    print("storage.py OK")
