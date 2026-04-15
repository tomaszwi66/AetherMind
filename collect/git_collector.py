"""
collect/git_collector.py - Collects git commit history.

Auto-discovers repos by walking parent directories. Supports
incremental import via last_run_timestamp. Each commit = one record.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from loguru import logger

from collect.base import BaseCollector, CollectorError

try:
    import git
    from git import InvalidGitRepositoryError, GitCommandError
    GIT_AVAILABLE = True
except ImportError:
    GIT_AVAILABLE = False


def _find_git_repos(search_roots: list[str], max_depth: int = 3) -> list[Path]:
    """Find .git directories up to max_depth levels deep in search_roots."""
    repos = set()
    for root in search_roots:
        root_path = Path(root)
        if not root_path.exists():
            continue
        # Check the root itself
        if (root_path / ".git").exists():
            repos.add(root_path)
        # Walk subdirectories
        try:
            for depth, (dirpath, dirnames, _) in enumerate(
                (d for d in [(root_path, list(root_path.iterdir()), None)] if True)
            ):
                break
            # Simple BFS limited by depth
            queue = [(root_path, 0)]
            while queue:
                current, d = queue.pop(0)
                if d > max_depth:
                    continue
                try:
                    for child in current.iterdir():
                        if not child.is_dir():
                            continue
                        if child.name.startswith(".") and child.name != ".git":
                            continue
                        if child.name == ".git":
                            repos.add(current)
                        elif child.is_dir():
                            queue.append((child, d + 1))
                except PermissionError:
                    pass
        except Exception:
            pass
    return list(repos)


class GitCollector(BaseCollector):
    source_name = "git"

    def collect(self, last_run_timestamp: Optional[str] = None) -> list[dict]:
        if not GIT_AVAILABLE:
            raise CollectorError("GitPython not installed. Run: pip install gitpython")

        configured_repos = self.collect_config.get("repos", [])
        max_days_back = self.collect_config.get("max_days_back", 365)
        exclude_merges = self.collect_config.get("exclude_merge_commits", True)

        # Determine search roots
        if configured_repos:
            repo_paths = [Path(p) for p in configured_repos if Path(p).exists()]
        else:
            # Auto-discover: search CWD and its parents/siblings
            cwd = Path.cwd()
            search_roots = [str(cwd), str(cwd.parent), str(Path.home() / "Desktop")]
            repo_paths = _find_git_repos(search_roots)
            logger.info(f"[git] Auto-discovered repos: {[str(p) for p in repo_paths]}")

        if not repo_paths:
            logger.warning("[git] No git repos found")
            return []

        # Cutoff datetime
        if last_run_timestamp:
            try:
                from dateutil import parser as dp
                cutoff_dt = dp.parse(last_run_timestamp).replace(tzinfo=timezone.utc)
            except Exception:
                cutoff_dt = datetime.now(timezone.utc) - timedelta(days=max_days_back)
        else:
            cutoff_dt = datetime.now(timezone.utc) - timedelta(days=max_days_back)

        records = []
        for repo_path in repo_paths:
            try:
                repo = git.Repo(repo_path)
                repo_name = repo_path.name

                try:
                    branch = repo.active_branch.name
                except TypeError:
                    branch = "detached"

                for commit in repo.iter_commits():
                    try:
                        committed_dt = commit.committed_datetime
                        # Normalize to UTC
                        if committed_dt.tzinfo is None:
                            committed_dt = committed_dt.replace(tzinfo=timezone.utc)
                        else:
                            from datetime import timezone as tz_module
                            committed_dt = committed_dt.astimezone(timezone.utc)

                        if committed_dt < cutoff_dt:
                            break  # iter_commits is chronological desc, safe to break

                        message = commit.message.strip()
                        first_line = message.split("\n")[0][:200]
                        body = "\n".join(message.split("\n")[1:]).strip()

                        # Skip merge commits if configured
                        if exclude_merges and len(commit.parents) > 1:
                            continue

                        # Line change stats (can be slow for large repos)
                        try:
                            stats = commit.stats.total
                            lines_changed = stats.get("insertions", 0) + stats.get("deletions", 0)
                        except Exception:
                            lines_changed = 0

                        records.append(
                            {
                                "raw_timestamp": committed_dt.isoformat(),
                                "raw_summary": f"[{repo_name}] {first_line}",
                                "source": "git",
                                "commit_hash": commit.hexsha[:8],
                                "repo_name": repo_name,
                                "repo_path": str(repo_path),
                                "author": commit.author.name,
                                "lines_changed": lines_changed,
                                "branch": branch,
                                "commit_body": body[:500] if body else "",
                            }
                        )
                    except Exception as e:
                        logger.debug(f"[git] Skipping commit in {repo_name}: {e}")

            except (InvalidGitRepositoryError, GitCommandError) as e:
                logger.warning(f"[git] Could not read repo {repo_path}: {e}")
            except Exception as e:
                logger.warning(f"[git] Unexpected error for {repo_path}: {e}")

        logger.info(f"[git] Collected {len(records)} commits from {len(repo_paths)} repos")
        return records
