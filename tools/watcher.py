#!/usr/bin/env python3
"""DAG Watcher and Hot-Reload Tool.

Continuously monitors active DAGs and their manifest dependencies for file changes,
and automatically re-syncs modified files into 'enabled_dags/' for instant Airflow hot-reloading.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Set, Tuple

# Ensure repo root and current working directory are on sys.path regardless of symlink execution
_file_dir = os.path.dirname(os.path.realpath(__file__))
_repo_root = os.path.abspath(os.path.join(_file_dir, ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
if os.getcwd() not in sys.path:
    sys.path.insert(0, os.getcwd())

from tools.sync import (
    DEFAULT_TARGET_DIR,
    get_enabled_dags,
    read_manifest,
    sync_dag,
    clean_target,
)


def find_all_manifest_dags(root_dir: str = "dags") -> List[str]:
    """Scan root_dir recursively for any DAG directory containing a manifest."""
    manifest_dags: List[str] = []
    for dirpath, _, filenames in os.walk(root_dir):
        if "manifest" in filenames or "dependencies.manifest" in filenames:
            manifest_dags.append(os.path.relpath(dirpath, "."))
    return sorted(manifest_dags)


def get_files_snapshot(paths: Set[str]) -> Dict[str, float]:
    """Take a snapshot of file paths and their modification timestamps."""
    snapshot: Dict[str, float] = {}
    for p in paths:
        if not os.path.exists(p):
            continue
        if os.path.isfile(p):
            try:
                snapshot[p] = os.path.getmtime(p)
            except OSError:
                pass
        elif os.path.isdir(p):
            for dirpath, _, filenames in os.walk(p):
                # Skip __pycache__ and git metadata
                if "__pycache__" in dirpath or ".git" in dirpath:
                    continue
                for fn in filenames:
                    if fn.endswith(".pyc") or fn.startswith("."):
                        continue
                    full_p = os.path.join(dirpath, fn)
                    try:
                        snapshot[full_p] = os.path.getmtime(full_p)
                    except OSError:
                        pass
    return snapshot


class DagWatcher:
    """Monitors DAGs and dependencies, triggering auto-sync on file modifications."""

    def __init__(
        self,
        dag_dirs: List[str],
        target_dir: str = DEFAULT_TARGET_DIR,
        poll_interval: float = 1.0,
    ):
        self.dag_dirs = [d.rstrip("/") for d in dag_dirs]
        self.target_dir = target_dir
        self.poll_interval = poll_interval
        # Mapping: dag_dir -> set of monitored relative paths (manifest deps + dag_dir)
        self.dag_dependencies: Dict[str, Set[str]] = {}
        # Flat set of all paths being monitored
        self.all_monitored_paths: Set[str] = set()
        self.last_snapshot: Dict[str, float] = {}

    def refresh_dependencies(self) -> None:
        """Read manifests for all target DAGs and compile watched paths."""
        self.dag_dependencies.clear()
        self.all_monitored_paths.clear()

        for dag_dir in self.dag_dirs:
            manifest_path = os.path.join(dag_dir, "manifest")
            if not os.path.exists(manifest_path):
                manifest_path = os.path.join(dag_dir, "dependencies.manifest")

            deps = {dag_dir}
            if os.path.exists(manifest_path):
                deps.add(manifest_path)
                try:
                    for d in read_manifest(manifest_path):
                        deps.add(d)
                except Exception as exc:
                    print(f"[!] Warning reading manifest for {dag_dir}: {exc}")

            self.dag_dependencies[dag_dir] = deps
            self.all_monitored_paths.update(deps)

    def initial_sync(self) -> None:
        """Perform initial sync for all monitored DAGs."""
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] [*] Performing initial sync into '{self.target_dir}/'...")
        for dag_dir in self.dag_dirs:
            sync_dag(dag_dir, self.target_dir, quiet=False)
        self.refresh_dependencies()
        self.last_snapshot = get_files_snapshot(self.all_monitored_paths)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [✓] Initial sync complete. Monitoring {len(self.last_snapshot)} files.\n")

    def detect_changes(self) -> Tuple[List[str], List[str], List[str]]:
        """Compare current file snapshot with previous snapshot.
        
        Returns (modified_files, added_files, removed_files).
        """
        current_snapshot = get_files_snapshot(self.all_monitored_paths)
        modified = []
        added = []
        removed = []

        for f, mtime in current_snapshot.items():
            if f not in self.last_snapshot:
                added.append(f)
            elif mtime > self.last_snapshot[f]:
                modified.append(f)

        for f in self.last_snapshot:
            if f not in current_snapshot:
                removed.append(f)

        self.last_snapshot = current_snapshot
        return modified, added, removed

    def find_affected_dags(self, changed_files: List[str]) -> Set[str]:
        """Find all DAGs whose dependencies contain any of the changed files."""
        affected: Set[str] = set()
        for f in changed_files:
            for dag_dir, deps in self.dag_dependencies.items():
                for dep in deps:
                    # Check if changed file is within or matches dep path
                    if f == dep or f.startswith(dep.rstrip("/") + os.sep):
                        affected.add(dag_dir)
                        break
        return affected

    def sync_affected_dags(self, affected_dags: Set[str], changed_files: List[str]) -> None:
        """Re-sync only DAGs affected by the changed files."""
        now_str = datetime.now().strftime("%H:%M:%S")
        print(f"[{now_str}] [!] Detected changes in {len(changed_files)} file(s):")
        for f in changed_files[:5]:
            print(f"    - {f}")
        if len(changed_files) > 5:
            print(f"    ... and {len(changed_files) - 5} more files.")

        print(f"[{now_str}] [->] Re-syncing {len(affected_dags)} affected DAG(s): {list(affected_dags)}")
        for dag_dir in affected_dags:
            sync_dag(dag_dir, self.target_dir, quiet=True)
            print(f"[{now_str}] [✓] Hot-reloaded: {dag_dir}")

        # Check if any manifest file itself changed
        if any(os.path.basename(f) in ("manifest", "dependencies.manifest") for f in changed_files):
            print(f"[{now_str}] [*] Manifest changed, updating dependency graph...")
            self.refresh_dependencies()

    def watch(self) -> None:
        """Run continuous watch loop."""
        self.initial_sync()
        print(f"[*] Watching for file changes (interval: {self.poll_interval}s). Press Ctrl+C to stop.\n")

        try:
            while True:
                time.sleep(self.poll_interval)
                modified, added, removed = self.detect_changes()
                all_changed = modified + added + removed

                if all_changed:
                    affected = self.find_affected_dags(all_changed)
                    if affected:
                        self.sync_affected_dags(affected, all_changed)
        except KeyboardInterrupt:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] [x] Watcher stopped.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Watch DAG packages and manifest dependencies, auto-syncing to enabled_dags on changes."
    )
    parser.add_argument(
        "dag_dirs",
        nargs="*",
        help="DAG directories to watch (defaults to currently enabled DAGs, or all manifest DAGs)",
    )
    parser.add_argument(
        "--target",
        default=DEFAULT_TARGET_DIR,
        help=f"Target directory (default: {DEFAULT_TARGET_DIR})",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Polling interval in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean target directory before starting watcher",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run initial sync once and exit without watching",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Watch all DAGs in repository that have a manifest",
    )

    args = parser.parse_args()

    if args.clean:
        clean_target(args.target)

    # Determine which DAGs to watch
    target_dags: List[str] = []
    if args.dag_dirs:
        target_dags = [d.rstrip("/") for d in args.dag_dirs]
    elif args.all:
        target_dags = find_all_manifest_dags("dags")
    else:
        # Check enabled_dags/.enabled_list
        enabled = get_enabled_dags(args.target)
        if enabled:
            target_dags = sorted(enabled)
            print(f"[*] Watching currently enabled DAGs from {args.target}/ ({len(target_dags)} DAGs)")
        else:
            # Fallback: scan all DAGs with manifest
            target_dags = find_all_manifest_dags("dags")
            print(f"[*] No specific DAG specified. Found {len(target_dags)} DAGs with manifests to watch.")

    if not target_dags:
        print("[x] Error: No DAG directories with manifest found to watch.")
        sys.exit(1)

    print(f"[*] Active target directory : {args.target}/")
    print(f"[*] Monitored DAGs ({len(target_dags)}):")
    for d in target_dags:
        print(f"    - {d}")

    watcher = DagWatcher(
        dag_dirs=target_dags,
        target_dir=args.target,
        poll_interval=args.interval,
    )

    if args.once:
        watcher.initial_sync()
        return

    watcher.watch()


if __name__ == "__main__":
    main()
