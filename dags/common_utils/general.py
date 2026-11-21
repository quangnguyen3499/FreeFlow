"""General utility functions for data handling, transformation, and formatting."""

from __future__ import annotations

import csv
import hashlib
import json
import logging
from typing import Any, Dict, Generator, Iterable, List, Optional, Sequence

logger = logging.getLogger(__name__)


def pretty(data: Any, indent: int = 2) -> str:
    """Format object as indented JSON string for logging and debugging."""
    try:
        return json.dumps(data, indent=indent, default=str, ensure_ascii=False)
    except Exception as exc:
        logger.warning("Failed to serialize object with json.dumps: %s. Using repr.", exc)
        return repr(data)


def load_json(filepath: str) -> Any:
    """Load JSON from file path."""
    with open(filepath, mode="r", encoding="utf-8") as f:
        return json.load(f)


def _parse_yaml_lines(lines: List[str]) -> Any:
    """Lightweight pure-python parser for nested YAML mappings."""
    root: Dict[str, Any] = {}
    stack: List[tuple[int, Dict[str, Any], Optional[str]]] = [(0, root, None)]

    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(raw_line) - len(raw_line.lstrip(" "))
        while stack and indent < stack[-1][0]:
            stack.pop()

        current_dict = stack[-1][1]

        if ":" in stripped:
            key, val = stripped.split(":", 1)
            key = key.strip()
            val = val.strip()

            if val.startswith("#"):
                val = ""
            elif " #" in val:
                val = val.split(" #")[0].strip()

            if not val:
                new_dict: Dict[str, Any] = {}
                current_dict[key] = new_dict
                stack.append((indent + 2, new_dict, key))
            else:
                val = val.strip("\"'")
                if val.isdigit():
                    current_dict[key] = int(val)
                elif val.lower() == "true":
                    current_dict[key] = True
                elif val.lower() == "false":
                    current_dict[key] = False
                elif val.startswith("{") and val.endswith("}"):
                    try:
                        current_dict[key] = json.loads(val)
                    except Exception:
                        current_dict[key] = {}
                else:
                    current_dict[key] = val

    return root


def load_yaml(filepath: str) -> Any:
    """Load YAML from file path. Uses PyYAML if installed, otherwise robust nested fallback."""
    try:
        import yaml  # type: ignore
        with open(filepath, mode="r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except ImportError:
        with open(filepath, mode="r", encoding="utf-8") as f:
            content = f.read()
            if content.strip().startswith("{") or content.strip().startswith("["):
                return json.loads(content)
            return _parse_yaml_lines(content.splitlines())


def load_csv(filepath: str, delimiter: str = ",") -> List[Dict[str, str]]:
    """Load CSV into a list of row dictionaries."""
    with open(filepath, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return list(reader)


def batch(iterable: Sequence[Any], n: int) -> Generator[List[Any], None, None]:
    """Yield successive n-sized batches from iterable."""
    if n <= 0:
        raise ValueError("Batch size must be greater than zero.")
    for i in range(0, len(iterable), n):
        yield list(iterable[i : i + n])


def dig(data: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Safely traverse deeply nested dictionary keys."""
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def flatten(items: Iterable[Any]) -> List[Any]:
    """Flatten an arbitrarily nested sequence of items."""
    output: List[Any] = []
    for item in items:
        if isinstance(item, (list, tuple, set)):
            output.extend(flatten(item))
        else:
            output.append(item)
    return output


def compact(items: Iterable[Any]) -> List[Any]:
    """Filter out None, empty strings, and empty sequences."""
    return [item for item in items if item not in (None, "", [], {}, ())]


def camel_case(s: str) -> str:
    """Convert snake_case or kebab-case string to camelCase."""
    parts = s.replace("-", "_").split("_")
    if not parts:
        return ""
    return parts[0].lower() + "".join(word.capitalize() for word in parts[1:] if word)


def md5(value: str | bytes) -> str:
    """Generate MD5 hex digest from string or bytes."""
    if isinstance(value, str):
        value = value.encode("utf-8")
    return hashlib.md5(value).hexdigest()
