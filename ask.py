"""
ask.py - CLI RAG Q&A over your personal memory.

Embeds the question, retrieves similar events from Qdrant,
reranks by importance, sends context to Ollama, prints the answer.

Run:
    python ask.py "When was I most productive?"
    python ask.py "What projects did I work on in March?"
    python ask.py "Show gym visits this year" --type health
    python ask.py "What did I do last week?" --since 2026-04-07
    python ask.py --interactive
"""

from datetime import date, datetime
from pathlib import Path
from typing import Optional

import click
import yaml
from loguru import logger

import storage
from index import embed_query, load_model
from reflect import call_ollama, check_ollama

# System prompt is built dynamically (datetime injected at call time)
RAG_SYSTEM_PROMPT_TEMPLATE = """You are a personal AI assistant with access to the user's life memory database.

CURRENT DATE AND TIME: {current_datetime}
TODAY IS: {weekday}, {today_date}

CRITICAL INSTRUCTIONS:
- Answer ONLY based on the memory context provided below.
- NEVER output raw JSON, dicts, or data structures in your response.
- Write natural, fluent sentences as a thoughtful assistant would.
- Use the current date above to resolve relative time references like "today", "tomorrow", "yesterday", "next week", "recently".
- When citing events, mention the date naturally (e.g. "On April 14th you...").
- If the context contains notes/notebooks, synthesize the key insights from them.
- If the answer is not in the context, say clearly: "I don't have that information in your memories."
- Be concise but thorough. No bullet-point dumps."""

RAG_PROMPT_TEMPLATE = """QUESTION: {question}

MEMORY CONTEXT ({count} relevant records retrieved):
{context}

Synthesize a natural, helpful answer based strictly on the memory context above:"""


def _format_context(
    events: list[dict],
    reflections: list[dict],
    max_chars: int,
    note_content_chars: int = 600,
) -> str:
    """
    Format retrieved events and reflections into a rich context string for the LLM.

    Notes:    include full chunk content so the LLM can synthesize from actual text.
    Calendar: include description, location, duration.
    Git:      include commit body.
    Timeline: include duration and place tags.
    Reflections: prepended as high-density daily summaries.
    """
    lines = []
    char_count = 0

    # ── Reflections first (high-density summaries) ────────────────────────────
    for ref in reflections[:3]:
        wins_str = "; ".join((ref.get("wins") or [])[:2])
        line = (
            f"[DAILY SUMMARY {ref['date']}] "
            f"{ref.get('summary', '')} "
            f"Theme: {ref.get('theme', '')}. "
            f"Mood: {ref.get('mood', '')}."
        )
        if wins_str:
            line += f" Wins: {wins_str}."
        lines.append(line)
        char_count += len(line)
        if char_count >= max_chars // 3:
            break

    # ── Events grouped by date, newest first ─────────────────────────────────
    by_date: dict[str, list[dict]] = {}
    for ev in events:
        d = ev.get("timestamp", "")[:10]
        by_date.setdefault(d, []).append(ev)

    for d in sorted(by_date.keys(), reverse=True):
        if char_count >= max_chars:
            break
        lines.append(f"\n--- {d} ---")
        char_count += 12

        for ev in by_date[d]:
            if char_count >= max_chars:
                break

            time_str = ev.get("timestamp", "")[:16].replace("T", " ")
            source = ev.get("source", "")
            ev_type = ev.get("type", "")
            summary = ev.get("summary", "")
            meta = ev.get("metadata", {}) if isinstance(ev.get("metadata"), dict) else {}

            # ── Base line ────────────────────────────────────────────────────
            header = f"[{time_str}] [{ev_type}|{source}] {summary}"

            # ── Source-specific enrichment ───────────────────────────────────
            extras = []

            if source == "notes":
                full_content = meta.get("full_content", "")
                if full_content and full_content.strip() != summary.strip():
                    # Include note body - this is what the LLM actually needs to read
                    content_snippet = full_content[:note_content_chars].strip()
                    extras.append(f"  Content: {content_snippet}")
                filename = meta.get("filename", "")
                if filename:
                    extras.append(f"  File: {filename}")

            elif source in ("calendar", "google_calendar"):
                desc = meta.get("description", "")
                loc = meta.get("location", "")
                dur = meta.get("duration_minutes")
                cal = meta.get("calendar_name", "")
                if dur:
                    header += f" ({dur}min)"
                if loc:
                    extras.append(f"  Location: {loc}")
                if desc:
                    extras.append(f"  Details: {desc[:300]}")
                if cal:
                    extras.append(f"  Calendar: {cal}")

            elif source == "git":
                lines_changed = meta.get("lines_changed", 0)
                repo = meta.get("repo_name", "")
                body = meta.get("commit_body", "")
                if lines_changed:
                    header += f" (+{lines_changed} lines)"
                if repo:
                    header += f" [repo: {repo}]"
                if body:
                    extras.append(f"  Commit notes: {body[:200]}")

            elif source == "google_timeline":
                dur = meta.get("duration_minutes", 0)
                place_tags = meta.get("place_tags", [])
                if dur:
                    header += f" ({dur}min)"
                if place_tags:
                    header += f" [{', '.join(place_tags)}]"

            lines.append(header)
            char_count += len(header)
            for extra in extras:
                lines.append(extra)
                char_count += len(extra)

    return "\n".join(lines)


def retrieve_and_answer(
    question: str,
    conn,
    qdrant_client,
    model,
    config: dict,
    top_k: Optional[int] = None,
    filter_type: Optional[str] = None,
    filter_tags: Optional[list[str]] = None,
    since_date: Optional[str] = None,
) -> tuple[str, list[dict]]:
    """
    Full RAG pipeline:
    1. Embed question with BGE query prefix
    2. Search Qdrant (top_k candidates, min_score threshold)
    3. Fetch full events from SQLite (includes metadata.full_content)
    4. Rerank by 0.7 * semantic_score + 0.3 * importance
    5. Fetch related daily reflections
    6. Build rich context (note bodies, calendar details, git info)
    7. Inject current datetime into system prompt
    8. Call Ollama -> synthesized natural language answer
    """
    now = datetime.now()
    ask_cfg = config.get("ask", {})

    # ── Resolve top_k ─────────────────────────────────────────────────────────
    k = top_k or ask_cfg.get("top_k", 20)
    min_score = ask_cfg.get("min_score", 0.15)
    do_rerank = ask_cfg.get("rerank", True)
    max_chars = ask_cfg.get("context_max_chars", 12000)
    note_content_chars = ask_cfg.get("note_content_chars", 600)
    collection = config["qdrant"]["collection_name"]

    # ── Step 1: Embed query ───────────────────────────────────────────────────
    query_vec = embed_query(question, model)

    # ── Step 2: Qdrant semantic search ───────────────────────────────────────
    hits = storage.search_similar(
        qdrant_client,
        collection,
        query_vec,
        top_k=k,
        min_score=min_score,
        filter_type=filter_type,
        filter_tags=filter_tags,
    )

    if not hits:
        # Fallback: try with lower threshold if nothing found
        hits = storage.search_similar(
            qdrant_client,
            collection,
            query_vec,
            top_k=min(k, 10),
            min_score=0.0,
        )
        if not hits:
            return (
                "I couldn't find any relevant memories for that question. "
                "Make sure you've run `python run_pipeline.py` to import and index your data.",
                [],
            )

    # ── Step 3: Fetch full events from SQLite ─────────────────────────────────
    events = []
    for hit in hits:
        event_id = hit.get("event_id")
        if not event_id:
            continue
        ev = storage.get_event_by_id(conn, event_id)
        if ev:
            ev["_search_score"] = hit["score"]
            if since_date and ev.get("timestamp", "")[:10] < since_date:
                continue
            events.append(ev)

    if not events:
        return "I found vector matches but couldn't load the event details from the database.", []

    # ── Step 4: Rerank ────────────────────────────────────────────────────────
    if do_rerank:
        events.sort(
            key=lambda e: 0.7 * e.get("_search_score", 0) + 0.3 * e.get("importance", 0.5),
            reverse=True,
        )

    # ── Step 5: Fetch related daily reflections ───────────────────────────────
    event_dates = sorted({ev["timestamp"][:10] for ev in events})
    reflections = []
    if event_dates:
        reflections = storage.get_reflections_for_range(conn, event_dates[0], event_dates[-1])
        # Parse JSON fields in reflections
        for ref in reflections:
            for field in ("wins", "risks", "patterns"):
                if isinstance(ref.get(field), str):
                    try:
                        import json as _json
                        ref[field] = _json.loads(ref[field])
                    except Exception:
                        ref[field] = []

    # ── Step 6: Build rich context string ─────────────────────────────────────
    context = _format_context(events, reflections, max_chars, note_content_chars)

    # ── Step 7: Build datetime-aware system prompt ────────────────────────────
    system_prompt = RAG_SYSTEM_PROMPT_TEMPLATE.format(
        current_datetime=now.strftime("%Y-%m-%d %H:%M"),
        weekday=now.strftime("%A"),
        today_date=now.strftime("%B %d, %Y"),
    )

    # ── Step 8: Call Ollama ───────────────────────────────────────────────────
    if not check_ollama(config["ollama"]["base_url"]):
        logger.warning("Ollama not running - returning formatted context without synthesis")
        answer = (
            "**Ollama is not running** - cannot synthesize an answer.\n\n"
            "Start Ollama with: `ollama serve`\n\n"
            "**Raw memories found:**\n```\n" + context + "\n```"
        )
        return answer, events

    prompt = RAG_PROMPT_TEMPLATE.format(
        question=question,
        count=len(events),
        context=context,
    )
    full_prompt = f"{system_prompt}\n\n{prompt}"

    try:
        answer = call_ollama(full_prompt, config)
        if not answer or not answer.strip():
            answer = f"Ollama returned an empty response. Context retrieved:\n\n{context}"
    except Exception as e:
        logger.error(f"Ollama call failed: {e}")
        answer = (
            f"**LLM error:** {e}\n\n"
            f"**Raw memories found** ({len(events)} events):\n```\n{context[:3000]}\n```"
        )

    return answer, events


def _print_answer(question: str, answer: str, sources: list[dict]) -> None:
    print(f"\n{'='*60}")
    print(f"Q: {question}")
    print(f"{'='*60}")
    print(f"\n{answer}\n")
    if sources:
        print(f"\n--- Sources ({len(sources)} events) ---")
        for i, ev in enumerate(sources[:8], 1):
            ts = ev.get("timestamp", "")[:10]
            print(f"  [{i}] {ts} | {ev['type']}/{ev['source']} | {ev['summary'][:80]}")


def interactive_session(conn, qdrant_client, model, config: dict) -> None:
    """REPL mode for interactive Q&A."""
    print("\n=== AetherMind Interactive Mode ===")
    print("Type your questions. Enter 'quit' or Ctrl+C to exit.\n")

    while True:
        try:
            question = input("You: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nExiting.")
            break

        if question.lower() in ("quit", "exit", "q"):
            break
        if not question:
            continue

        answer, sources = retrieve_and_answer(question, conn, qdrant_client, model, config)
        _print_answer(question, answer, sources)


# ─── CLI ─────────────────────────────────────────────────────────────────────

@click.command()
@click.argument("question", required=False)
@click.option("--config", "config_path", default="config.yaml")
@click.option("--top-k", default=None, type=int, help="Number of memories to retrieve")
@click.option("--type", "filter_type", default=None, help="Filter by event type (work/health/social/etc.)")
@click.option("--tags", default=None, help="Filter by tags (comma-separated)")
@click.option("--since", "since_date", default=None, help="Only consider events since date (YYYY-MM-DD)")
@click.option("--interactive", "-i", is_flag=True, help="Interactive REPL mode")
def main(
    question: Optional[str],
    config_path: str,
    top_k: Optional[int],
    filter_type: Optional[str],
    tags: Optional[str],
    since_date: Optional[str],
    interactive: bool,
):
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    conn = storage.init_db(config["paths"]["sqlite_db"])
    qdrant_client = storage.get_qdrant_client(config["qdrant"]["storage_path"])
    storage.ensure_collection(qdrant_client, config)

    total = storage.get_events_count(conn)
    indexed = storage.get_indexed_count(conn)
    if indexed == 0:
        print(f"[Warning] No indexed events found. Run: python index.py")
    else:
        logger.info(f"Memory loaded: {indexed}/{total} events indexed")

    model = load_model(config)

    if interactive or not question:
        interactive_session(conn, qdrant_client, model, config)
        return

    ask_cfg = config.get("ask", {})
    k = top_k or ask_cfg.get("top_k", 8)
    tag_list = [t.strip() for t in tags.split(",")] if tags else None

    answer, sources = retrieve_and_answer(
        question,
        conn,
        qdrant_client,
        model,
        config,
        top_k=k,
        filter_type=filter_type,
        filter_tags=tag_list,
        since_date=since_date,
    )
    _print_answer(question, answer, sources)


if __name__ == "__main__":
    main()
