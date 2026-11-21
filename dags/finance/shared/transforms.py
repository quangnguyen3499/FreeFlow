"""Financial data transformations and exact Decimal arithmetic using ROUND_HALF_UP."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Union

# Standard currency quantizers
DECIMAL_PLACES_TWO = Decimal("0.01")
DECIMAL_PLACES_ZERO = Decimal("1")


def to_decimal(val: Union[str, int, float, Decimal], precision: Decimal = DECIMAL_PLACES_TWO) -> Decimal:
    """Convert input to Decimal and quantize with ROUND_HALF_UP."""
    if isinstance(val, Decimal):
        dec = val
    elif isinstance(val, (int, str)):
        dec = Decimal(str(val).strip())
    elif isinstance(val, float):
        # Convert float through string representation to prevent binary float inaccuracies
        dec = Decimal(str(val))
    else:
        raise TypeError(f"Cannot convert type {type(val)} to Decimal.")

    return dec.quantize(precision, rounding=ROUND_HALF_UP)


def calculate_line_item(
    amount: Union[str, int, float, Decimal],
    fee_rate: Union[str, int, float, Decimal] = Decimal("0.02"),
    tax_rate: Union[str, int, float, Decimal] = Decimal("0.10"),
) -> Dict[str, Decimal]:
    """Calculate subtotal, service fee, tax, and net amount with exact Decimal arithmetic."""
    base_amount = to_decimal(amount)
    fee_rate_dec = to_decimal(fee_rate, Decimal("0.0001"))
    tax_rate_dec = to_decimal(tax_rate, Decimal("0.0001"))

    fee_amount = (base_amount * fee_rate_dec).quantize(DECIMAL_PLACES_TWO, rounding=ROUND_HALF_UP)
    tax_amount = (base_amount * tax_rate_dec).quantize(DECIMAL_PLACES_TWO, rounding=ROUND_HALF_UP)
    total_billed = (base_amount + fee_amount + tax_amount).quantize(DECIMAL_PLACES_TWO, rounding=ROUND_HALF_UP)
    net_payout = (base_amount - fee_amount).quantize(DECIMAL_PLACES_TWO, rounding=ROUND_HALF_UP)

    return {
        "base_amount": base_amount,
        "fee_amount": fee_amount,
        "tax_amount": tax_amount,
        "total_billed": total_billed,
        "net_payout": net_payout,
    }
