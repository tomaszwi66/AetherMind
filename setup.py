"""
setup.py - AetherMind First-Run Setup Wizard

NOT a Python package setup file. This is an interactive wizard that:
  1. Checks system requirements (Python, Git, Ollama, CUDA, disk space)
  2. Installs Python dependencies
  3. Sets up Google OAuth (optional - opens browser)
  4. Configures daily sync schedule
  5. Creates Windows Task Scheduler tasks
  6. Runs the first full import pipeline

Run:
    python setup.py

Re-run any time to update config or re-authenticate Google.
"""

import json
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path

# ── Bootstrap rich (may not be installed yet) ──────────────────────────────────
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.prompt import Confirm, Prompt
    from rich.table import Table
    from rich import print as rprint
    _RICH = True
except ImportError:
    _RICH = False

    class Console:
        def print(self, *args, **kwargs):
            text = " ".join(str(a) for a in args)
            for tag in ["[bold]", "[/bold]", "[green]", "[/green]", "[red]", "[/red]",
                        "[yellow]", "[/yellow]", "[cyan]", "[/cyan]", "[dim]", "[/dim]"]:
                text = text.replace(tag, "")
            print(text)

    class Confirm:
        @staticmethod
        def ask(prompt, default=True):
            suffix = "[Y/n]" if default else "[y/N]"
            ans = input(f"{prompt} {suffix}: ").strip().lower()
            if not ans:
                return default
            return ans in ("y", "yes")

    class Prompt:
        @staticmethod
        def ask(prompt, default=""):
            ans = input(f"{prompt} [{default}]: ").strip()
            return ans if ans else default

console = Console()

PROJECT_DIR = Path(__file__).parent.resolve()
CREDENTIALS_DIR = PROJECT_DIR / "credentials"
CONFIG_FILE = PROJECT_DIR / "config.yaml"
PYTHON = sys.executable

# ── Helpers ───────────────────────────────────────────────────────────────────

def _step(n: int, title: str) -> None:
    console.print(f"\n[bold cyan]Step {n}:[/bold cyan] {title}")
    console.print("─" * 50)


def _ok(msg: str) -> None:
    console.print(f"  [green]✓[/green] {msg}")


def _warn(msg: str) -> None:
    console.print(f"  [yellow]⚠[/yellow] {msg}")


def _fail(msg: str) -> None:
    console.print(f"  [red]✗[/red] {msg}")


def _run(cmd: list[str], capture: bool = True, timeout: int = 60) -> tuple[int, str]:
    """Run a subprocess and return (returncode, output)."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=capture,
            text=True,
            timeout=timeout,
            cwd=str(PROJECT_DIR),  # Always run from project root
        )
        return result.returncode, (result.stdout + result.stderr).strip()
    except subprocess.TimeoutExpired:
        return 1, "Timeout"
    except FileNotFoundError:
        return 1, f"Command not found: {cmd[0]}"
    except Exception as e:
        return 1, str(e)


# ── Step 1: System checks ─────────────────────────────────────────────────────

def check_system() -> dict:
    _step(1, "System Requirements Check")

    results = {}

    # Python version
    major, minor = sys.version_info[:2]
    if major >= 3 and minor >= 11:
        _ok(f"Python {major}.{minor}.{sys.version_info[2]}")
        results["python"] = True
    else:
        _fail(f"Python {major}.{minor} found - need 3.11+")
        results["python"] = False

    # Git
    code, out = _run(["git", "--version"])
    if code == 0:
        _ok(out.strip())
        results["git"] = True
    else:
        _warn("Git not found (optional, needed for git commit collection)")
        results["git"] = False

    # CUDA
    try:
        import torch
        if torch.cuda.is_available():
            gpu = torch.cuda.get_device_name(0)
            _ok(f"CUDA available - {gpu}")
            results["cuda"] = True
        else:
            _warn("CUDA not available - embeddings will use CPU (slower)")
            results["cuda"] = False
    except ImportError:
        _warn("PyTorch not installed yet")
        results["cuda"] = False

    # Ollama
    code, out = _run(["ollama", "--version"])
    if code == 0:
        _ok(f"Ollama: {out.strip()}")
        results["ollama"] = True
    else:
        _warn("Ollama not found")
        results["ollama"] = False

    # Disk space
    usage = shutil.disk_usage(PROJECT_DIR)
    free_gb = usage.free / (1024 ** 3)
    if free_gb >= 5:
        _ok(f"Free disk space: {free_gb:.1f} GB")
        results["disk"] = True
    else:
        _warn(f"Low disk space: {free_gb:.1f} GB (5 GB recommended)")
        results["disk"] = False

    return results


# ── Step 2: Install Python deps ───────────────────────────────────────────────

def install_deps() -> bool:
    _step(2, "Python Dependencies")

    requirements = PROJECT_DIR / "requirements.txt"
    if not requirements.exists():
        _warn("requirements.txt not found - skipping")
        return True

    console.print("  Installing packages...")
    code, out = _run(
        [PYTHON, "-m", "pip", "install", "-r", str(requirements), "--quiet"],
        capture=True,
        timeout=300,
    )
    if code == 0:
        _ok("All Python packages installed")
    else:
        _warn(f"Some packages may have failed: {out[-200:]}")

    # Install Google API packages separately (optional)
    console.print("  Installing Google API packages (for Calendar sync)...")
    google_pkgs = [
        "google-api-python-client",
        "google-auth-oauthlib",
        "google-auth-httplib2",
    ]
    code, out = _run(
        [PYTHON, "-m", "pip", "install"] + google_pkgs + ["--quiet"],
        capture=True,
        timeout=120,
    )
    if code == 0:
        _ok("Google API packages installed")
    else:
        _warn("Google API packages failed - Calendar sync won't work")

    return True


# ── Step 3: Ollama setup ──────────────────────────────────────────────────────

def setup_ollama(system_check: dict) -> bool:
    _step(3, "Ollama & AI Model")

    if not system_check.get("ollama"):
        console.print(
            "\n  Ollama is required for daily reflections and Q&A.\n"
            "  Download from: [bold]https://ollama.ai/download[/bold]\n"
            "  Install it, then re-run this wizard.\n"
        )
        if not Confirm.ask("  Skip Ollama setup for now?", default=True):
            _fail("Cannot continue without Ollama")
            return False
        _warn("Ollama setup skipped - reflections won't work until installed")
        return True

    # Pull the model
    import yaml
    with open(CONFIG_FILE) as f:
        config = yaml.safe_load(f)
    model = config.get("ollama", {}).get("model", "qwen2.5:7b")

    console.print(f"\n  Checking model: [bold]{model}[/bold]")
    code, out = _run(["ollama", "list"], capture=True)
    if code == 0 and model.split(":")[0] in out:
        _ok(f"Model '{model}' already downloaded")
        return True

    if Confirm.ask(f"  Pull model '{model}'? (4-5 GB download)", default=True):
        console.print(f"  Pulling {model}... (this may take several minutes)")
        code, _ = _run(["ollama", "pull", model], capture=False, timeout=600)
        if code == 0:
            _ok(f"Model '{model}' ready")
        else:
            _warn(f"Failed to pull {model} - you can run 'ollama pull {model}' manually")
    else:
        _warn(f"Skipped - run 'ollama pull {model}' when ready")

    return True


# ── Step 4: Google OAuth ──────────────────────────────────────────────────────

def setup_google_auth() -> bool:
    _step(4, "Google Account (Calendar Sync)")

    CREDENTIALS_DIR.mkdir(exist_ok=True)
    client_secret = CREDENTIALS_DIR / "google_client_secret.json"
    token_file = CREDENTIALS_DIR / "google_token.json"

    if token_file.exists():
        _ok("Google account already connected")
        if not Confirm.ask("  Reconnect (re-authenticate)?", default=False):
            return True
        token_file.unlink()

    if not client_secret.exists():
        console.print("""
  [bold]To connect Google Calendar, you need to create OAuth credentials:[/bold]

  1. Go to: [cyan]https://console.cloud.google.com/[/cyan]
  2. Create a new project (or select existing)
  3. Enable the [bold]Google Calendar API[/bold]:
     APIs & Services → Library → Search "Calendar" → Enable
  4. Create OAuth credentials:
     APIs & Services → Credentials → Create Credentials → OAuth client ID
     Application type: [bold]Desktop app[/bold]
     Name: AetherMind
  5. Download the JSON file
  6. Save it as: [bold]credentials/google_client_secret.json[/bold]
  7. Re-run this wizard

  [dim]Note: The app stays in "Testing" mode by default, which is fine for personal use.[/dim]
""")
        if not Confirm.ask("  Skip Google setup for now?", default=True):
            return False
        _warn("Google Calendar sync not configured")
        return True

    # Run OAuth flow
    console.print("\n  Opening browser for Google authentication...")
    console.print("  [dim]Sign in and grant calendar read access.[/dim]\n")

    try:
        from collect.google_calendar import get_credentials
        creds = get_credentials()
        _ok("Google account connected successfully")

        # Show which account
        try:
            from googleapiclient.discovery import build
            service = build("calendar", "v3", credentials=creds, cache_discovery=False)
            cal_list = service.calendarList().list().execute()
            calendars = cal_list.get("items", [])
            primary = next((c for c in calendars if c.get("primary")), None)
            if primary:
                _ok(f"Account: {primary.get('id', 'Unknown')}")
            _ok(f"Calendars found: {len(calendars)}")
        except Exception:
            pass

        return True
    except Exception as e:
        _fail(f"Authentication failed: {e}")
        return False


# ── Step 5: Configure settings ────────────────────────────────────────────────

def configure_settings() -> dict:
    _step(5, "Pipeline Configuration")

    import yaml
    with open(CONFIG_FILE, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    console.print("\n  Configure daily automation:\n")

    # Sync time
    current_hour = config.get("reflect", {}).get("schedule_hour", 20)
    sync_time = Prompt.ask(
        "  Daily sync time (HH:MM, 24h)",
        default=f"{current_hour:02d}:00"
    )
    try:
        h, m = sync_time.split(":")
        h, m = int(h), int(m)
        sync_time = f"{h:02d}:{m:02d}"
    except Exception:
        sync_time = "20:00"
        _warn("Invalid time format - using 20:00")

    # Reflect hour (1 hour after sync)
    reflect_h = (int(sync_time.split(":")[0]) + 1) % 24
    reflect_time = f"{reflect_h:02d}:00"

    # Model selection
    current_model = config.get("ollama", {}).get("model", "qwen2.5:7b")
    model_choice = Prompt.ask(
        "  Ollama model",
        default=current_model
    )

    # CUDA
    enable_cuda = Confirm.ask("  Use CUDA for embeddings (faster)?", default=True)

    # Weekly summaries
    enable_weekly = Confirm.ask("  Enable weekly summaries (every Sunday)?", default=True)

    # Update config
    config["reflect"]["schedule_hour"] = int(sync_time.split(":")[0])
    config["ollama"]["model"] = model_choice
    config["embedding"]["device"] = "cuda" if enable_cuda else "cpu"

    # Add google_calendar to config if not present
    if "google" not in config:
        config["google"] = {}
    config["google"]["weekly_summaries"] = enable_weekly

    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        yaml.dump(config, f, allow_unicode=True, sort_keys=False)

    _ok(f"Config saved - sync at {sync_time}, reflect at {reflect_time}")

    return {
        "sync_time": sync_time,
        "reflect_time": reflect_time,
        "weekly": enable_weekly,
    }


# ── Step 6: Windows Task Scheduler ───────────────────────────────────────────

def setup_scheduler(settings: dict) -> bool:
    _step(6, "Windows Task Scheduler")

    if platform.system() != "Windows":
        _warn("Not on Windows - skipping Task Scheduler setup")
        console.print(f"  Add these to your cron:\n"
                      f"  {settings['sync_time'].replace(':', '')} * * * python {PROJECT_DIR}/run_pipeline.py\n"
                      f"  {settings['reflect_time'].replace(':', '')} * * * python {PROJECT_DIR}/run_pipeline.py --reflect")
        return True

    pipeline_script = str(PROJECT_DIR / "run_pipeline.py")
    sync_time = settings["sync_time"]
    reflect_time = settings["reflect_time"]

    tasks = [
        {
            "name": "AetherMind-Pipeline",
            "cmd": f'"{PYTHON}" "{pipeline_script}"',
            "time": sync_time,
            "desc": f"AetherMind: daily data collection at {sync_time}",
        },
        {
            "name": "AetherMind-Reflect",
            "cmd": f'"{PYTHON}" "{pipeline_script}" --reflect',
            "time": reflect_time,
            "desc": f"AetherMind: daily AI reflection at {reflect_time}",
        },
    ]

    if settings.get("weekly"):
        weekly_script = str(PROJECT_DIR / "run_pipeline.py")
        tasks.append(
            {
                "name": "AetherMind-Weekly",
                "cmd": f'"{PYTHON}" "{weekly_script}" --stages reflect --reflect-date weekly',
                "time": "09:00",
                "desc": "AetherMind: weekly summary (Sundays)",
                "weekly": True,
            }
        )

    for task in tasks:
        cmd = [
            "schtasks", "/create",
            "/tn", task["name"],
            "/tr", task["cmd"],
            "/sc", "weekly" if task.get("weekly") else "daily",
            "/st", task["time"],
            "/f",  # force overwrite
        ]
        if task.get("weekly"):
            cmd.extend(["/d", "SUN"])

        code, out = _run(cmd)
        if code == 0:
            _ok(f"Task '{task['name']}' created ({task['time']})")
        else:
            _warn(f"Task '{task['name']}' failed: {out[:100]}")
            console.print(f"  Manual command:\n  schtasks /create /tn \"{task['name']}\" /tr {task['cmd']!r} /sc daily /st {task['time']}")

    return True


# ── Step 7: First import ──────────────────────────────────────────────────────

def run_first_import() -> bool:
    _step(7, "First Data Import")

    console.print("\n  Running collect → normalize → index pipeline...")
    console.print("  [dim](First run downloads the embedding model ~90MB)[/dim]\n")

    cmd = [PYTHON, str(PROJECT_DIR / "run_pipeline.py"), "--stages", "collect,normalize,index"]
    code, _ = _run(cmd, capture=False, timeout=600)

    if code == 0:
        _ok("First import completed!")
    else:
        _warn("Import finished with some errors - check logs/pipeline.log")

    return True


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary(settings: dict) -> None:
    console.print("\n")
    if _RICH:
        from rich.panel import Panel
        console.print(Panel.fit(
            "[bold green]AetherMind Setup Complete![/bold green]\n\n"
            "[bold]Daily automation:[/bold]\n"
            f"  Pipeline runs at {settings.get('sync_time', '20:00')} (collect + index)\n"
            f"  Reflection runs at {settings.get('reflect_time', '21:00')} (AI summary)\n\n"
            "[bold]Quick commands:[/bold]\n"
            "  streamlit run app.py          - Open web UI\n"
            "  python ask.py \"...\"           - Ask your memory\n"
            "  python run_pipeline.py        - Manual sync\n"
            "  python reflect.py             - Manual reflection\n\n"
            "[bold]Add your data:[/bold]\n"
            "  input/notes/                  - Drop .txt/.md notes here\n"
            "  input/calendar.csv            - Calendar export (CSV)\n"
            "  input/Semantic_Location_History/ - Google Takeout JSON",
            title="[bold cyan]AetherMind[/bold cyan]",
        ))
    else:
        print("\n=== AetherMind Setup Complete! ===")
        print(f"Pipeline: daily at {settings.get('sync_time', '20:00')}")
        print(f"Reflect:  daily at {settings.get('reflect_time', '21:00')}")
        print("\nCommands:")
        print("  streamlit run app.py")
        print("  python ask.py \"When was I most productive?\"")


# ── Main wizard ───────────────────────────────────────────────────────────────

def main():
    if _RICH:
        console.print(Panel.fit(
            "[bold cyan]AetherMind[/bold cyan] - Private AI That Remembers Your Life\n"
            "[dim]Local-first · Privacy-first · Zero cloud[/dim]",
            border_style="cyan",
        ))
    else:
        print("=" * 50)
        print("AetherMind - Setup Wizard")
        print("=" * 50)

    console.print(f"\nProject directory: [dim]{PROJECT_DIR}[/dim]")
    console.print()

    if not Confirm.ask("Start setup?", default=True):
        print("Cancelled.")
        return

    # Run steps
    system = check_system()

    if not system.get("python"):
        _fail("Python 3.11+ is required. Please upgrade Python and re-run.")
        sys.exit(1)

    install_deps()
    setup_ollama(system)

    google_ok = Confirm.ask("\nConnect Google Calendar for automatic sync?", default=True)
    if google_ok:
        setup_google_auth()

    settings = configure_settings()

    scheduler_ok = Confirm.ask("\nSet up Windows Task Scheduler?", default=True)
    if scheduler_ok:
        setup_scheduler(settings)

    import_ok = Confirm.ask("\nRun first data import now?", default=True)
    if import_ok:
        run_first_import()

    print_summary(settings)


if __name__ == "__main__":
    main()
