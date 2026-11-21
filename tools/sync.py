#!/usr/bin/env python3
"""DAG Manifest Sync Tool.

Reads a DAG's manifest file and syncs the active DAG and its explicit
dependencies into the 'enabled_dags' directory for lightweight local development.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from typing import List, Set

# Ensure repo root is on sys.path
_file_dir = os.path.dirname(os.path.realpath(__file__))
_repo_root = os.path.abspath(os.path.join(_file_dir, ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
if os.getcwd() not in sys.path:
    sys.path.insert(0, os.getcwd())

DEFAULT_TARGET_DIR = "enabled_dags"
STATE_FILE_NAME = ".enabled_list"


def read_manifest(manifest_path: str) -> List[str]:
    """Parse manifest file and return list of relative directory/file paths."""
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(f"Manifest file not found: {manifest_path}")

    dependencies: List[str] = []
    with open(manifest_path, "r", encoding="utf-8") as f:
        for line in f:
            clean = line.strip()
            if clean and not clean.startswith("#"):
                dependencies.append(clean)
    return dependencies


def get_enabled_dags(target_dir: str = DEFAULT_TARGET_DIR) -> Set[str]:
    """Retrieve the set of currently enabled DAG paths from state file."""
    state_file = os.path.join(target_dir, STATE_FILE_NAME)
    if not os.path.exists(state_file):
        return set()
    with open(state_file, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip() and not line.startswith("#")}


def save_enabled_dags(dags: Set[str], target_dir: str = DEFAULT_TARGET_DIR) -> None:
    """Save the set of enabled DAG paths to state file."""
    os.makedirs(target_dir, exist_ok=True)
    state_file = os.path.join(target_dir, STATE_FILE_NAME)
    with open(state_file, "w", encoding="utf-8") as f:
        for d in sorted(dags):
            f.write(f"{d}\n")


def ensure_package_inits(target_dir: str = DEFAULT_TARGET_DIR) -> None:
    """Ensure top-level __init__.py files exist in target for package imports."""
    for sub in ["dags", "plugins"]:
        init_file = os.path.join(target_dir, sub, "__init__.py")
        if os.path.exists(os.path.join(target_dir, sub)) and not os.path.exists(init_file):
            with open(init_file, "w", encoding="utf-8") as f:
                f.write(f'"""Root {sub} package inside {target_dir}."""\n')


def sync_path(src: str, dst: str, dry_run: bool = False) -> bool:
    """Sync a file or directory from src to dst. Returns True if changes copied."""
    if not os.path.exists(src):
        print(f"  [x] Missing source: {src}")
        return False

    if dry_run:
        print(f"  [->] (dry-run) {src} -> {dst}")
        return True

    os.makedirs(os.path.dirname(dst), exist_ok=True)

    if os.path.isdir(src):
        # Sync directory contents
        if os.path.exists(dst):
            shutil.rmtree(dst)
        shutil.copytree(src, dst, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".git*"))
    else:
        shutil.copy2(src, dst)

    return True


def sync_dag(
    dag_dir: str,
    target_dir: str = DEFAULT_TARGET_DIR,
    dry_run: bool = False,
    quiet: bool = False,
) -> List[str]:
    """Sync specified DAG and its dependencies from manifest into target_dir."""
    dag_clean = dag_dir.rstrip("/")
    manifest_path = os.path.join(dag_clean, "manifest")
    if not os.path.exists(manifest_path):
        alt_manifest = os.path.join(dag_clean, "dependencies.manifest")
        if os.path.exists(alt_manifest):
            manifest_path = alt_manifest
        else:
            if not quiet:
                print(f"[x] Error: Neither manifest nor dependencies.manifest exists in {dag_clean}")
            return []

    deps = read_manifest(manifest_path)
    if not quiet:
        print(f"[*] Syncing DAG: {dag_clean} -> {target_dir}/ ({len(deps)} manifest entries)")

    for dep in deps:
        src = os.path.abspath(dep)
        dst = os.path.abspath(os.path.join(target_dir, dep))
        sync_path(src, dst, dry_run=dry_run)
        if not quiet:
            print(f"  [->] Synced: {dep}")

    if not dry_run:
        ensure_package_inits(target_dir)
        current_enabled = get_enabled_dags(target_dir)
        current_enabled.add(dag_clean)
        save_enabled_dags(current_enabled, target_dir)

    if not quiet:
        print(f"[✓] Successfully enabled: {dag_clean}")

    return deps


def disable_dag(dag_dir: str, target_dir: str = DEFAULT_TARGET_DIR) -> None:
    """Disable a DAG by removing it from enabled state and removing target files."""
    dag_clean = dag_dir.rstrip("/")
    current_enabled = get_enabled_dags(target_dir)
    if dag_clean in current_enabled:
        current_enabled.remove(dag_clean)
        save_enabled_dags(current_enabled, target_dir)

    target_dag_path = os.path.join(target_dir, dag_clean)
    if os.path.exists(target_dag_path):
        if os.path.isdir(target_dag_path):
            shutil.rmtree(target_dag_path)
        else:
            os.remove(target_dag_path)
        print(f"[✓] Disabled and removed: {dag_clean} from {target_dir}")
    else:
        print(f"[!] {dag_clean} not found in {target_dir}")


def clean_target(target_dir: str = DEFAULT_TARGET_DIR) -> None:
    """Clean all synced files from target_dir."""
    if os.path.exists(target_dir):
        shutil.rmtree(target_dir)
    os.makedirs(target_dir, exist_ok=True)
    print(f"[✓] Cleaned target directory: {target_dir}/")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync DAG package and its manifest dependencies to enabled_dags."
    )
    parser.add_argument("dag_dir", nargs="?", help="Path to DAG directory containing manifest")
    parser.add_argument("--target", default=DEFAULT_TARGET_DIR, help=f"Target directory (default: {DEFAULT_TARGET_DIR})")
    parser.add_argument("--clean", action="store_true", help="Clean target directory before syncing")
    parser.add_argument("--disable", help="Disable and remove a specified DAG from enabled_dags")
    parser.add_argument("--list", action="store_true", help="List all currently enabled DAGs")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without modifying files")

    args = parser.parse_args()

    if args.list:
        enabled = get_enabled_dags(args.target)
        print(f"Currently enabled DAGs in {args.target}/ ({len(enabled)}):")
        for d in sorted(enabled):
            print(f"  - {d}")
        return

    if args.disable:
        disable_dag(args.disable, args.target)
        return

    if args.clean:
        clean_target(args.target)
        if not args.dag_dir:
            return

    if not args.dag_dir:
        parser.print_help()
        sys.exit(1)

    sync_dag(args.dag_dir, args.target, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
