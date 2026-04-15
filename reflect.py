"""
reflect.py - Daily AI reflection engine powered by Ollama.

For a given date, fetches all events, sends them to a local LLM,
and generates a structured JSON reflection (summary, wins, risks, patterns, mood).

Run:
    python reflect.py                         # Reflect on today
    python reflect.py --date 2026-04-14       # Reflect on a specific date
    python reflect.py --date 2026-04-14 --force  # Overwrite existing reflection
"""

import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import click
import httpx
import yaml
from loguru import logger

import storage

REFLECTION_SCHEMA = {
    "summary": "2-3 sentence overview of the day",
    "wins": ["achievement 1", "achievement 2"],
    "risks": ["concern or warning"],
    "patterns": ["behavioral pattern observed"],
    "theme": "single word describing the day",
    "mood": "focused|productive|social|tired|stressed|creative|mixed|unknown",
}

SYSTEM_PROMPT = """You are a personal AI life analyst. Your job is to review a person's daily activity log and produce a structured reflection.
Be honest, concise, and insightful. Look for patterns, achievements, and potential concerns.
You MUST respond ONLY with valid JSON - no markdown, no explanation, just the JSON object."""

REFLECTION_PROMPT_TEMPLATE = """Today is {date}. Review these {count} life events from the day:

EVENTS:
{events_text}

Respond ONLY with valid JSON matching this exact structure:
{{
  "summary": "2-3 sentence overview of what happened and how the day went",
  "wins": ["specific achievement or positive thing that happened"],
  "risks": ["concern, warning, or negative pattern to watch"],
  "patterns": ["behavioral pattern observed (e.g. 'worked late again', 'skipped exercise')"],
  "theme": "one word that best describes this day",
  "mood": "one of: focused, productive, social, tired, stressed, creative, mixed, unknown"
}}

If there were no events or data is insufficient, still return valid JSON with empty arrays and "unknown" values."""


def _format_events_for_prompt(events: list[dict], max_events: int) -> str:
    """Format events as a numbered list for the LLM prompt."""
    # Sort by importance DESC to put highest-signal events first; truncate to max
    sorted_events = sorted(events, key=lambda e: e.get("importance", 0.5), reverse=True)[:max_events]
    # Then sort chronologically for readability
    sorted_events.sort(key=lambda e: e.get("timestamp", ""))

    lines = []
    for i, ev in enumerate(sorted_events, 1):
        time_str = ev.get("timestamp", "")[:16].replace("T", " ")
        ev_type = ev.get("type", "")
        source = ev.get("source", "")
        summary = ev.get("summary", "")
        importance = ev.get("importance", 0.5)
        tags = ", ".join(ev.get("tags", [])[:4])

        line = f"{i}. [{time_str}] [{ev_type}/{source}] {summary}"
        if tags:
            line += f" #{tags}"
        lines.append(line)

    return "\n".join(lines)


def check_ollama(base_url: str, timeout: int = 5) -> bool:
    """Check if Ollama is running. Returns False instead of raising."""
    try:
        resp = httpx.get(f"{base_url}/api/tags", timeout=timeout)
        return resp.status_code == 200
    except Exception:
        return False


def call_ollama(prompt: str, config: dict) -> str:
    """Call Ollama /api/generate and return the response text."""
    base_url = config["ollama"]["base_url"]
    model = config["ollama"]["model"]
    timeout = config["ollama"].get("timeout_seconds", 120)
    temperature = config["ollama"].get("temperature", 0.3)

    payload = {
        "model": model,
        "prompt": f"{SYSTEM_PROMPT}\n\n{prompt}",
        "stream": False,
        "options": {
            "temperature": temperature,
            "num_predict": 800,
        },
    }

    for attempt in range(2):
        try:
            resp = httpx.post(
                f"{base_url}/api/generate",
                json=payload,
                timeout=timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("response", "")
        except httpx.TimeoutException:
            if attempt == 0:
                logger.warning("Ollama timeout, retrying once...")
                continue
            raise
        except Exception:
            raise

    return ""


def parse_reflection_response(raw_response: str) -> dict:
    """
    Three-tier JSON parsing fallback:
    1. Direct json.loads
    2. Extract {...} block with regex
    3. Return minimal fallback dict
    """
    if not raw_response:
        return _fallback_reflection()

    # Tier 1: direct parse
    try:
        data = json.loads(raw_response.strip())
        return _validate_reflection(data)
    except json.JSONDecodeError:
        pass

    # Tier 2: extract JSON block
    match = re.search(r"\{[\s\S]*\}", raw_response)
    if match:
        try:
            data = json.loads(match.group())
            return _validate_reflection(data)
        except json.JSONDecodeError:
            pass

    # Tier 3: fallback
    logger.warning("LLM returned non-JSON response; using fallback")
    fallback = _fallback_reflection()
    fallback["summary"] = raw_response[:300]
    return fallback


def _validate_reflection(data: dict) -> dict:
    """Ensure all required fields exist with correct types."""
    return {
        "summary": str(data.get("summary", "")),
        "wins": list(data.get("wins", [])),
        "risks": list(data.get("risks", [])),
        "patterns": list(data.get("patterns", [])),
        "theme": str(data.get("theme", "unknown")),
        "mood": str(data.get("mood", "unknown")),
    }


def _fallback_reflection() -> dict:
    return {
        "summary": "No reflection generated.",
        "wins": [],
        "risks": [],
        "patterns": [],
        "theme": "unknown",
        "mood": "unknown",
    }


def reflect_on_date(date_str: str, conn, config: dict, force: bool = False) -> Optional[dict]:
    """
    Generate a reflection for a specific date.
    Returns the reflection dict, or None if skipped.
    """
    # Check if reflection already exists
    if not force:
        existing = storage.get_reflection_for_date(conn, date_str)
        if existing:
            logger.info(f"Reflection for {date_str} already exists (use --force to overwrite)")
            return existing

    # Fetch events for this date
    events = storage.get_events_for_date(conn, date_str)
    if not events:
        logger.info(f"No events found for {date_str} - skipping reflection")
        return None

    max_events = config["reflect"].get("max_events_in_prompt", 30)
    events_text = _format_events_for_prompt(events, max_events)
    prompt = REFLECTION_PROMPT_TEMPLATE.format(
        date=date_str,
        count=len(events),
        events_text=events_text,
    )

    # Check Ollama health
    base_url = config["ollama"]["base_url"]
    if not check_ollama(base_url):
        logger.warning(
            f"Ollama is not running at {base_url}. "
            f"Start it with: ollama serve\n"
            f"Then pull the model: ollama pull {config['ollama']['model']}"
        )
        return None

    logger.info(f"Generating reflection for {date_str} ({len(events)} events)...")
    try:
        raw_response = call_ollama(prompt, config)
    except Exception as e:
        logger.error(f"Ollama call failed: {e}")
        return None

    reflection = parse_reflection_response(raw_response)
    reflection["date"] = date_str
    reflection["event_count"] = len(events)
    reflection["model_used"] = config["ollama"]["model"]
    reflection["raw_response"] = raw_response

    # Save to file
    reflections_dir = Path(config["paths"]["reflections_dir"])
    reflections_dir.mkdir(parents=True, exist_ok=True)
    out_file = reflections_dir / f"{date_str}.json"
    out_file.write_text(
        json.dumps(reflection, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Save to SQLite
    storage.save_reflection(conn, reflection)

    logger.info(f"Reflection saved: theme={reflection['theme']}, mood={reflection['mood']}")
    return reflection


# ─── CLI ─────────────────────────────────────────────────────────────────────

@click.command()
@click.option("--config", "config_path", default="config.yaml")
@click.option("--date", "date_str", default=None, help="Date to reflect on (YYYY-MM-DD, default: today)")
@click.option("--force", is_flag=True, help="Overwrite existing reflection")
def main(config_path: str, date_str: Optional[str], force: bool):
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    conn = storage.init_db(config["paths"]["sqlite_db"])

    if date_str is None:
        date_str = date.today().isoformat()

    result = reflect_on_date(date_str, conn, config, force=force)
    if result:
        print(f"\n=== Reflection: {date_str} ===")
        print(f"Theme:   {result['theme']}")
        print(f"Mood:    {result['mood']}")
        print(f"Summary: {result['summary']}")
        if result["wins"]:
            print(f"Wins:    {', '.join(result['wins'])}")
        if result["risks"]:
            print(f"Risks:   {', '.join(result['risks'])}")
        if result["patterns"]:
            print(f"Patterns: {', '.join(result['patterns'])}")


if __name__ == "__main__":
    main()
