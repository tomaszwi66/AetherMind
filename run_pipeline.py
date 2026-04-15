"""
run_pipeline.py - Daily orchestrator for AetherMind.

Runs: collect → normalize → index → (optionally) reflect

Run:
    python run_pipeline.py                          # Full pipeline
    python run_pipeline.py --stages collect,index   # Specific stages
    python run_pipeline.py --reflect                # Reflection only
    python run_pipeline.py --source git             # One collector only

Windows Task Scheduler setup:
    schtasks /create /tn "AetherMind-Pipeline" /tr "python C:\\...\\run_pipeline.py" /sc daily /st 08:00
    schtasks /create /tn "AetherMind-Reflect"  /tr "python C:\\...\\run_pipeline.py --reflect" /sc daily /st 22:00
"""

import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import click
import yaml
from loguru import logger
from rich.console import Console
from rich.table import Table

import storage
from normalize import normalize_all
from reflect import reflect_on_date

# ─── Always run from the project directory ────────────────────────────────────
# Ensures config.yaml, db/, raw/, data/ are resolved from the project root
# regardless of the working directory when this script was launched.
os.chdir(Path(__file__).parent.resolve())

console = Console()

ALL_STAGES = ["collect", "normalize", "index", "reflect"]
DEFAULT_STAGES = ["collect", "normalize", "index"]


def _setup_logging(config: dict) -> None:
    logs_dir = Path(config["paths"]["logs_dir"])
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "pipeline.log"

    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}")
    logger.add(
        str(log_file),
        level="DEBUG",
        rotation="10 MB",
        retention=3,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
    )


def _acquire_lock(db_dir: str) -> Optional[Path]:
    """Create a lockfile to prevent concurrent pipeline runs. Returns lock path or None."""
    lock_path = Path(db_dir) / ".pipeline.lock"
    if lock_path.exists():
        # Check if the lock is stale (> 30 minutes old)
        age = time.time() - lock_path.stat().st_mtime
        if age < 1800:
            return None
    lock_path.write_text(str(datetime.now()))
    return lock_path


def _release_lock(lock_path: Optional[Path]) -> None:
    if lock_path and lock_path.exists():
        lock_path.unlink()


# ─── Stage implementations ────────────────────────────────────────────────────

def stage_collect(config: dict, source_filter: Optional[str] = None) -> dict:
    """Run all collectors and save raw JSON files."""
    from collect import get_all_collectors, get_collector

    raw_dir = Path(config["paths"]["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    conn = storage.init_db(config["paths"]["sqlite_db"])

    if source_filter:
        collectors = {source_filter: get_collector(source_filter, config)}
    else:
        collectors = get_all_collectors(config)

    stats = {"sources": {}, "total_records": 0, "errors": []}

    for name, collector in collectors.items():
        t0 = time.time()
        run_id = storage.start_import_run(conn, name)
        last_run = storage.get_last_successful_run(conn, name)

        try:
            records = collector._safe_collect(last_run_timestamp=last_run)
            output_path = raw_dir / f"{name}.json"
            collector.save(records, str(output_path))

            elapsed = time.time() - t0
            storage.finish_import_run(conn, run_id, "success", events_found=len(records))
            stats["sources"][name] = {"records": len(records), "elapsed": round(elapsed, 1)}
            stats["total_records"] += len(records)
            logger.info(f"[collect] {name}: {len(records)} records in {elapsed:.1f}s")
        except Exception as e:
            elapsed = time.time() - t0
            storage.finish_import_run(conn, run_id, "failed", error_message=str(e))
            stats["sources"][name] = {"records": 0, "elapsed": round(elapsed, 1), "error": str(e)}
            stats["errors"].append(f"{name}: {e}")
            logger.error(f"[collect] {name} failed: {e}")

    return stats


def stage_normalize(config: dict, source_filter: Optional[str] = None) -> dict:
    """Normalize raw records into canonical events."""
    conn = storage.init_db(config["paths"]["sqlite_db"])
    t0 = time.time()
    result = normalize_all(
        raw_dir=config["paths"]["raw_dir"],
        events_file=config["paths"]["events_file"],
        conn=conn,
        config=config,
        source_filter=source_filter,
    )
    result["elapsed"] = round(time.time() - t0, 1)
    logger.info(f"[normalize] {result['events_new']} new, {result['events_skipped']} skipped in {result['elapsed']}s")
    return result


def stage_index(config: dict, max_events: Optional[int] = None) -> dict:
    """Embed unindexed events into Qdrant."""
    from index import index_events, load_model

    conn = storage.init_db(config["paths"]["sqlite_db"])
    qdrant_client = storage.get_qdrant_client(config["qdrant"]["storage_path"])
    storage.ensure_collection(qdrant_client, config)

    unindexed_count = storage.get_events_count(conn) - storage.get_indexed_count(conn)
    if unindexed_count == 0:
        logger.info("[index] No unindexed events - skipping model load")
        return {"total": 0, "indexed": 0, "errors": 0, "elapsed": 0}

    t0 = time.time()
    model = load_model(config)
    result = index_events(conn, qdrant_client, model, config, max_events=max_events)
    result["elapsed"] = round(time.time() - t0, 1)
    logger.info(f"[index] {result['indexed']} events indexed in {result['elapsed']}s")
    return result


def stage_reflect(config: dict, reflect_date: Optional[str] = None, force: bool = False) -> dict:
    """Generate daily AI reflection."""
    conn = storage.init_db(config["paths"]["sqlite_db"])
    d = reflect_date or date.today().isoformat()
    t0 = time.time()
    result = reflect_on_date(d, conn, config, force=force)
    elapsed = round(time.time() - t0, 1)
    if result:
        return {"date": d, "theme": result.get("theme"), "mood": result.get("mood"), "elapsed": elapsed}
    return {"date": d, "skipped": True, "elapsed": elapsed}


# ─── Rich summary table ───────────────────────────────────────────────────────

def print_summary(stage_results: dict, total_elapsed: float) -> None:
    table = Table(title="AetherMind Pipeline", show_header=True, header_style="bold cyan", safe_box=True)
    table.add_column("Stage", style="bold")
    table.add_column("Result", style="green")
    table.add_column("Time(s)", justify="right")

    for stage, result in stage_results.items():
        if stage == "collect":
            details = f"{result.get('total_records', 0)} records from {len(result.get('sources', {}))} sources"
            if result.get("errors"):
                details += f" | {len(result['errors'])} error(s)"
        elif stage == "normalize":
            details = f"{result.get('events_new', 0)} new events, {result.get('events_skipped', 0)} duplicates"
        elif stage == "index":
            if result.get("total") == 0:
                details = "Nothing to index"
            else:
                details = f"{result.get('indexed', 0)}/{result.get('total', 0)} events embedded"
        elif stage == "reflect":
            if result.get("skipped"):
                details = f"Skipped ({result.get('date')})"
            else:
                details = f"{result.get('date')} | theme={result.get('theme')} mood={result.get('mood')}"
        else:
            details = str(result)

        elapsed = result.get("elapsed", 0) if isinstance(result, dict) else 0
        table.add_row(stage.capitalize(), details, f"{elapsed:.1f}")

    table.add_row("[bold]Total[/bold]", "", f"[bold]{total_elapsed:.1f}[/bold]")
    console.print(table)


# ─── CLI ─────────────────────────────────────────────────────────────────────

@click.command()
@click.option("--config", "config_path", default="config.yaml")
@click.option("--stages", default=None, help=f"Comma-separated stages to run: {ALL_STAGES}")
@click.option("--reflect", "run_reflect_only", is_flag=True, help="Run reflection stage only")
@click.option("--source", default=None, help="Only collect/normalize this source")
@click.option("--force-reflect", is_flag=True, help="Overwrite existing reflection")
@click.option("--max-events", type=int, default=None, help="Limit events to index")
@click.option("--reflect-date", default=None, help="Date for reflection (YYYY-MM-DD, default: today)")
def main(
    config_path: str,
    stages: Optional[str],
    run_reflect_only: bool,
    source: Optional[str],
    force_reflect: bool,
    max_events: Optional[int],
    reflect_date: Optional[str],
):
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    _setup_logging(config)
    logger.info("=== AetherMind Pipeline starting ===")
    pipeline_start = time.time()

    # Determine which stages to run
    if run_reflect_only:
        stages_to_run = ["reflect"]
    elif stages:
        stages_to_run = [s.strip() for s in stages.split(",")]
        invalid = [s for s in stages_to_run if s not in ALL_STAGES]
        if invalid:
            logger.error(f"Invalid stages: {invalid}. Valid: {ALL_STAGES}")
            sys.exit(1)
    else:
        stages_to_run = DEFAULT_STAGES

    # Acquire lock
    lock = _acquire_lock(config["paths"]["db_dir"])
    if lock is None:
        logger.error("Pipeline is already running (lockfile exists). Exiting.")
        sys.exit(1)

    stage_results = {}
    exit_code = 0

    try:
        if "collect" in stages_to_run:
            logger.info("--- Stage: collect ---")
            stage_results["collect"] = stage_collect(config, source_filter=source)
            if stage_results["collect"]["errors"] and config["pipeline"].get("fail_fast"):
                logger.error("Collection errors in fail_fast mode - stopping")
                sys.exit(1)

        if "normalize" in stages_to_run:
            logger.info("--- Stage: normalize ---")
            stage_results["normalize"] = stage_normalize(config, source_filter=source)

        if "index" in stages_to_run:
            logger.info("--- Stage: index ---")
            stage_results["index"] = stage_index(config, max_events=max_events)

        if "reflect" in stages_to_run:
            logger.info("--- Stage: reflect ---")
            stage_results["reflect"] = stage_reflect(
                config, reflect_date=reflect_date, force=force_reflect
            )

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        exit_code = 1
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        exit_code = 1
    finally:
        _release_lock(lock)

    total_elapsed = round(time.time() - pipeline_start, 1)
    print_summary(stage_results, total_elapsed)
    logger.info(f"=== Pipeline done in {total_elapsed}s ===")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
