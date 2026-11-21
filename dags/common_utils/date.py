"""Business day, date manipulation, and date template expression helpers."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Set

# Standard holiday set (can be extended via variable or configuration)
DEFAULT_HOLIDAYS: Set[date] = {
    date(2026, 1, 1),    # New Year's Day
    date(2026, 7, 4),    # Independence Day
    date(2026, 12, 25),  # Christmas Day
}


def today_iso() -> str:
    """Return today's date in ISO 8601 YYYY-MM-DD format."""
    return date.today().isoformat()


def yesterday() -> str:
    """Return yesterday's date in ISO 8601 YYYY-MM-DD format."""
    return (date.today() - timedelta(days=1)).isoformat()


def unique_timestamp() -> str:
    """Generate high-precision unique timestamp string suitable for run directories or file names."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")


def is_business_day(target_date: Optional[date] = None, holidays: Optional[Set[date]] = None) -> bool:
    """Check if the given date is a business day (Monday through Friday, not in holidays)."""
    if target_date is None:
        target_date = date.today()
    holidays_set = holidays if holidays is not None else DEFAULT_HOLIDAYS
    # Monday is 0, Sunday is 6
    if target_date.weekday() >= 5:
        return False
    return target_date not in holidays_set


def next_business_day(target_date: Optional[date] = None, holidays: Optional[Set[date]] = None) -> date:
    """Return the next immediate business day after target_date."""
    if target_date is None:
        target_date = date.today()
    curr = target_date + timedelta(days=1)
    while not is_business_day(curr, holidays):
        curr += timedelta(days=1)
    return curr


def next_n_business_days(n: int, target_date: Optional[date] = None, holidays: Optional[Set[date]] = None) -> date:
    """Return the date that is n business days in the future."""
    if n <= 0:
        raise ValueError("n must be a positive integer.")
    curr = target_date or date.today()
    count = 0
    while count < n:
        curr = next_business_day(curr, holidays)
        count += 1
    return curr


def previous_business_day(target_date: Optional[date] = None, holidays: Optional[Set[date]] = None) -> date:
    """Return the prior immediate business day before target_date."""
    if target_date is None:
        target_date = date.today()
    curr = target_date - timedelta(days=1)
    while not is_business_day(curr, holidays):
        curr -= timedelta(days=1)
    return curr


def first_business_day_of_month(target_date: Optional[date] = None, holidays: Optional[Set[date]] = None) -> date:
    """Return the first business day of the month for target_date."""
    if target_date is None:
        target_date = date.today()
    curr = date(target_date.year, target_date.month, 1)
    while not is_business_day(curr, holidays):
        curr += timedelta(days=1)
    return curr


def last_business_day_of_month(target_date: Optional[date] = None, holidays: Optional[Set[date]] = None) -> date:
    """Return the last business day of the month for target_date."""
    if target_date is None:
        target_date = date.today()
    # Find next month 1st day then subtract 1 day
    if target_date.month == 12:
        next_month = date(target_date.year + 1, 1, 1)
    else:
        next_month = date(target_date.year, target_date.month + 1, 1)
    curr = next_month - timedelta(days=1)
    while not is_business_day(curr, holidays):
        curr -= timedelta(days=1)
    return curr


def resolve_date_expression(expr: str, base_date: Optional[date] = None) -> str:
    """Resolve dynamic date template expressions such as:
    
    - '{{ today }}' -> '2026-11-16'
    - '{{ yesterday }}'
    - '{{ next_business_day }}'
    - '{{ previous_business_day }}'
    - '{{ next_2_business_day }}' or '{{ next_3_business_days }}'
    - '{{ first_business_day_of_month }}'
    - '{{ last_business_day_of_month }}'
    """
    if not isinstance(expr, str) or "{{" not in expr:
        return expr

    base = base_date or date.today()

    def replace_match(m: re.Match[str]) -> str:
        key = m.group(1).strip()
        if key == "today":
            return base.isoformat()
        if key == "yesterday":
            return (base - timedelta(days=1)).isoformat()
        if key == "next_business_day":
            return next_business_day(base).isoformat()
        if key == "previous_business_day":
            return previous_business_day(base).isoformat()
        if key == "first_business_day_of_month":
            return first_business_day_of_month(base).isoformat()
        if key == "last_business_day_of_month":
            return last_business_day_of_month(base).isoformat()

        # Match next_n_business_day(s)
        match_n = re.match(r"^next_(\d+)_business_days?$", key)
        if match_n:
            n = int(match_n.group(1))
            return next_n_business_days(n, base).isoformat()

        # Unknown expression, leave as is
        return m.group(0)

    pattern = r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}"
    return re.sub(pattern, replace_match, expr)
