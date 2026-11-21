"""Unit tests for date and business day utilities."""

from datetime import date
import unittest
from dags.common_utils.date import (
    today_iso,
    yesterday,
    unique_timestamp,
    is_business_day,
    next_business_day,
    next_n_business_days,
    previous_business_day,
    first_business_day_of_month,
    last_business_day_of_month,
    resolve_date_expression,
)


class TestDateUtils(unittest.TestCase):
    def test_today_iso(self):
        res = today_iso()
        self.assertEqual(len(res), 10)
        self.assertEqual(res.count("-"), 2)

    def test_yesterday(self):
        res = yesterday()
        self.assertEqual(len(res), 10)

    def test_unique_timestamp(self):
        ts1 = unique_timestamp()
        ts2 = unique_timestamp()
        self.assertTrue(len(ts1) >= 15)
        # Verify formatting
        self.assertIn("_", ts1)

    def test_is_business_day(self):
        # 2026-11-16 is Monday
        monday = date(2026, 11, 16)
        self.assertTrue(is_business_day(monday))

        # 2026-11-21 is Saturday
        saturday = date(2026, 11, 21)
        self.assertFalse(is_business_day(saturday))

        # Holiday check
        holiday = date(2026, 1, 1)
        self.assertFalse(is_business_day(holiday))

    def test_next_business_day(self):
        friday = date(2026, 11, 20)
        # Next business day after Friday should be Monday 2026-11-23
        nxt = next_business_day(friday)
        self.assertEqual(nxt, date(2026, 11, 23))

    def test_next_n_business_days(self):
        friday = date(2026, 11, 20)
        # 2 business days after Friday: Monday -> Tuesday (Nov 24)
        nxt_2 = next_n_business_days(2, friday)
        self.assertEqual(nxt_2, date(2026, 11, 24))

    def test_previous_business_day(self):
        monday = date(2026, 11, 23)
        prev = previous_business_day(monday)
        self.assertEqual(prev, date(2026, 11, 20))

    def test_first_and_last_business_day_of_month(self):
        # Nov 2026
        dt = date(2026, 11, 15)
        first_bday = first_business_day_of_month(dt)
        self.assertTrue(is_business_day(first_bday))
        self.assertEqual(first_bday.month, 11)

        last_bday = last_business_day_of_month(dt)
        self.assertTrue(is_business_day(last_bday))
        self.assertEqual(last_bday.month, 11)

    def test_resolve_date_expression(self):
        base = date(2026, 11, 20)  # Friday
        res_today = resolve_date_expression("{{ today }}", base_date=base)
        self.assertEqual(res_today, "2026-11-20")

        res_next_bday = resolve_date_expression("{{ next_business_day }}", base_date=base)
        self.assertEqual(res_next_bday, "2026-11-23")

        res_next_2_bday = resolve_date_expression("{{ next_2_business_days }}", base_date=base)
        self.assertEqual(res_next_2_bday, "2026-11-24")

        # Static string stays unchanged
        self.assertEqual(resolve_date_expression("STATIC_STRING"), "STATIC_STRING")


if __name__ == "__main__":
    unittest.main()
