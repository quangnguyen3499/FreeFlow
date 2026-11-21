"""File handling utilities for CSV, JSON, staging directories, and archives."""

from __future__ import annotations

import csv
import json
import os
from typing import Any, Dict, List, Optional, Sequence
from dags.common_utils.general import load_json, load_yaml, load_csv


def ensure_dir_exists(dir_path: str) -> str:
    """Ensure directory exists on disk, creating intermediate parents if needed."""
    os.makedirs(dir_path, exist_ok=True)
    return dir_path


def csv_to_dict(filepath: str, delimiter: str = ",") -> List[Dict[str, str]]:
    """Parse CSV file into a list of row dictionaries."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")

    with open(filepath, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return list(reader)


def dict_to_csv(
    data: Sequence[Dict[str, Any]],
    filepath: str,
    fieldnames: Optional[Sequence[str]] = None,
    delimiter: str = ",",
) -> str:
    """Write a list of row dictionaries to a CSV file."""
    if not data and not fieldnames:
        raise ValueError("Cannot write empty CSV without explicit fieldnames.")

    parent_dir = os.path.dirname(filepath)
    if parent_dir:
        ensure_dir_exists(parent_dir)

    cols = fieldnames if fieldnames is not None else list(data[0].keys())

    with open(filepath, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols, delimiter=delimiter)
        writer.writeheader()
        if data:
            writer.writerows(data)

    return filepath
