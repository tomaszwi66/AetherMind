"""
index.py - Embed events and load into Qdrant.

Reads unindexed events from SQLite, generates embeddings via sentence-transformers
(CUDA-accelerated), and upserts into local Qdrant collection.

Run:
    python index.py                 # Index all unindexed events
    python index.py --stats         # Show collection statistics only
    python index.py --rebuild       # Drop + rebuild entire Qdrant index
    python index.py --max-events N  # Limit to N events (useful for testing)
"""

import uuid
from pathlib import Path
from typing import Optional

import click
import yaml
from loguru import logger
from qdrant_client.models import PointStruct

import storage

# BGE query prefix - improves recall when used with queries (not documents)
BGE_QUERY_PREFIX = "Represent this sentence for searching relevant passages: "


def load_model(config: dict):
    """Load the sentence-transformers embedding model (downloads once, then cached)."""
    from sentence_transformers import SentenceTransformer

    model_name = config["embedding"]["model_name"]
    device = config["embedding"].get("device", "cpu")
    cache_dir = config["embedding"].get("cache_dir", "db/model_cache")

    Path(cache_dir).mkdir(parents=True, exist_ok=True)

    logger.info(f"Loading embedding model '{model_name}' on {device}...")
    model = SentenceTransformer(model_name, device=device, cache_folder=cache_dir)
    try:
        dim = model.get_embedding_dimension()
    except AttributeError:
        dim = model.get_sentence_embedding_dimension()
    logger.info(f"Model loaded (dim={dim})")
    return model


def build_embedding_text(event: dict) -> str:
    """
    Build the text to embed for a single event.
    For notes/notebooks: embeds the full chunk content for semantic richness.
    For calendar: includes description and location.
    For git: uses summary + commit body.
    BGE performs best with rich textual context rather than just summary titles.
    """
    summary = event.get("summary", "")
    tags = ", ".join(event.get("tags", []))
    event_type = event.get("type", "")
    source = event.get("source", "")
    metadata = event.get("metadata", {}) if isinstance(event.get("metadata"), dict) else {}

    parts = [summary]

    # ── Source-specific content enrichment ────────────────────────────────────
    if source == "notes":
        # full_content is the actual paragraph chunk - this is the semantic core
        full_content = metadata.get("full_content", "")
        if full_content and full_content.strip() != summary.strip():
            # Embed up to 700 chars of actual note content (fits in BGE 512 token window)
            parts.append(full_content[:700])

    elif source in ("calendar", "google_calendar"):
        # Include description and location for richer calendar matching
        description = metadata.get("description", "")
        location = metadata.get("location", "")
        if description:
            parts.append(description[:300])
        if location:
            parts.append(f"location: {location}")

    elif source == "git":
        # Commit body often contains the real intent (issue refs, reasons)
        commit_body = metadata.get("commit_body", "")
        if commit_body:
            parts.append(commit_body[:300])

    # ── Universal context fields ───────────────────────────────────────────────
    if tags:
        parts.append(f"tags: {tags}")
    if event_type:
        parts.append(f"type: {event_type}")
    if source:
        parts.append(f"source: {source}")

    return " | ".join(filter(None, parts))


def embed_texts(model, texts: list[str], batch_size: int, normalize: bool = True) -> list[list[float]]:
    """Batch-embed a list of texts. Returns list of float vectors."""
    vectors = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=len(texts) > 50,
        normalize_embeddings=normalize,
        convert_to_numpy=True,
    )
    return vectors.tolist()


def embed_query(query: str, model) -> list[float]:
    """Embed a search query with the BGE query prefix."""
    prefixed = f"{BGE_QUERY_PREFIX}{query}"
    vec = model.encode([prefixed], normalize_embeddings=True, convert_to_numpy=True)
    return vec[0].tolist()


def index_events(
    conn,
    qdrant_client,
    model,
    config: dict,
    max_events: Optional[int] = None,
) -> dict:
    """
    Embed all unindexed events and upsert into Qdrant.
    Returns stats dict.
    """
    collection = config["qdrant"]["collection_name"]
    batch_size = config["embedding"]["batch_size"]
    fetch_limit = max_events or 10_000

    unindexed = storage.get_unindexed_events(conn, limit=fetch_limit)
    if not unindexed:
        logger.info("No unindexed events found")
        return {"total": 0, "indexed": 0, "errors": 0}

    logger.info(f"Embedding {len(unindexed)} events...")
    stats = {"total": len(unindexed), "indexed": 0, "errors": 0}

    # Process in batches
    BATCH = 512  # Qdrant upsert batch size
    for start in range(0, len(unindexed), BATCH):
        batch = unindexed[start : start + BATCH]
        texts = [build_embedding_text(ev) for ev in batch]

        try:
            vectors = embed_texts(model, texts, batch_size=batch_size)
        except Exception as e:
            logger.error(f"Embedding batch [{start}:{start+BATCH}] failed: {e}")
            stats["errors"] += len(batch)
            continue

        points = []
        embedding_ids = []
        event_ids = []

        for ev, vec in zip(batch, vectors):
            point_id = str(uuid.uuid4())
            points.append(
                PointStruct(
                    id=point_id,
                    vector=vec,
                    payload={
                        "event_id": ev["id"],
                        "timestamp": ev["timestamp"],
                        "type": ev["type"],
                        "source": ev["source"],
                        "summary": ev["summary"],
                        "tags": ev.get("tags", []),
                        "importance": ev.get("importance", 0.5),
                    },
                )
            )
            embedding_ids.append(point_id)
            event_ids.append(ev["id"])

        try:
            storage.upsert_vectors(qdrant_client, collection, points)
            storage.mark_events_indexed(conn, event_ids, embedding_ids)
            stats["indexed"] += len(batch)
            logger.debug(f"Indexed batch [{start}:{start+len(batch)}]")
        except Exception as e:
            logger.error(f"Qdrant upsert failed for batch [{start}:{start+BATCH}]: {e}")
            stats["errors"] += len(batch)

    return stats


def rebuild_index(conn, qdrant_client, model, config: dict) -> dict:
    """
    Reset Qdrant collection and re-embed all events from scratch.
    Safe because SQLite is the source of truth.
    """
    collection = config["qdrant"]["collection_name"]
    logger.warning(f"Rebuilding Qdrant collection '{collection}'...")

    # Delete collection if exists
    if qdrant_client.collection_exists(collection):
        qdrant_client.delete_collection(collection)
        logger.info(f"Deleted collection '{collection}'")

    # Reset indexed_at in SQLite
    with conn:
        conn.execute("UPDATE events SET indexed_at = NULL, embedding_id = NULL")

    # Recreate collection
    storage.ensure_collection(qdrant_client, config)

    # Re-index everything
    return index_events(conn, qdrant_client, model, config)


# ─── CLI ─────────────────────────────────────────────────────────────────────

@click.command()
@click.option("--config", "config_path", default="config.yaml")
@click.option("--stats", "show_stats", is_flag=True, help="Show stats only, don't index")
@click.option("--rebuild", is_flag=True, help="Rebuild entire Qdrant index from scratch")
@click.option("--max-events", type=int, default=None, help="Limit events to index")
def main(config_path: str, show_stats: bool, rebuild: bool, max_events: Optional[int]):
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    conn = storage.init_db(config["paths"]["sqlite_db"])
    qdrant_client = storage.get_qdrant_client(config["qdrant"]["storage_path"])
    storage.ensure_collection(qdrant_client, config)

    total_events = storage.get_events_count(conn)
    indexed_count = storage.get_indexed_count(conn)
    vector_count = storage.get_collection_count(qdrant_client, config["qdrant"]["collection_name"])

    if show_stats:
        print(f"\n=== AetherMind Index Stats ===")
        print(f"SQLite events:     {total_events}")
        print(f"Indexed events:    {indexed_count}")
        print(f"Unindexed events:  {total_events - indexed_count}")
        print(f"Qdrant vectors:    {vector_count}")
        return

    model = load_model(config)

    if rebuild:
        result = rebuild_index(conn, qdrant_client, model, config)
    else:
        result = index_events(conn, qdrant_client, model, config, max_events=max_events)

    logger.info(
        f"Indexing complete: {result['indexed']} indexed, "
        f"{result['errors']} errors, {result['total']} total"
    )


if __name__ == "__main__":
    main()
