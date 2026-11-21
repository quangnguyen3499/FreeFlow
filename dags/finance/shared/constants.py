"""Shared constants, record statuses, and partner codes for the Finance domain."""

from decimal import Decimal

# Record Settlement & Invoicing Statuses
STATUS_PENDING = "PENDING"
STATUS_VALIDATED = "VALIDATED"
STATUS_PROCESSING = "PROCESSING"
STATUS_BILLED = "BILLED"
STATUS_FAILED = "FAILED"
STATUS_RECONCILED = "RECONCILED"

# Supported Partner Identifiers
PARTNER_ALPHA = "ALPHA_LOGISTICS"
PARTNER_BETA = "BETA_PAYMENTS"
PARTNER_GAMMA = "GAMMA_RETAIL"

SUPPORTED_PARTNERS = {PARTNER_ALPHA, PARTNER_BETA, PARTNER_GAMMA}

# Financial Invariant Tolerances & Rates
DEFAULT_CURRENCY = "USD"
DEFAULT_TAX_RATE = Decimal("0.10")  # 10% VAT
MAX_ALLOWED_BATCH_AMOUNT = Decimal("5000000.00")  # 5 Million USD cap per batch
MIN_TRANSACTION_AMOUNT = Decimal("1.00")
