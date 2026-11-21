# DAG: finance_process_partner_billing

## Overview
Extracts unbilled transactions (`PENDING`), executes a 3-stage validation and transformation pipeline using exact `Decimal` arithmetic, verifies integrity invariants with an automated circuit breaker, submits billing batches to the partner REST API, and updates database settlement state.

Runs with `schedule=None`. Triggered via operations trigger DAGs or the Airflow REST API with `conf`.

---

## Trigger Example (Airflow REST API)

```bash
curl -X POST "http://localhost:8080/api/v1/dags/finance_process_partner_billing/dagRuns" \
  -H "Content-Type: application/json" \
  -u "admin:admin" \
  -d '{
    "conf": {
      "partner_code": "ALPHA_LOGISTICS",
      "target_date": "2026-11-16",
      "max_batch_size": 200,
      "dry_run": false
    }
  }'
```

---

## Configuration Parameters (`conf`)

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `partner_code` | `string` | No | `ALPHA_LOGISTICS` | Target partner identifier (`ALPHA_LOGISTICS`, `BETA_PAYMENTS`, `GAMMA_RETAIL`) |
| `target_date` | `string` | No | `today_iso()` (YYYY-MM-DD) | Cutoff date for extracting unbilled transactions |
| `max_batch_size` | `integer` | No | `500` | Maximum number of transactions per batch |
| `dry_run` | `boolean` | No | `false` | When `true`, extracts and verifies without external API dispatch or DB updates |

---

## Multi-Gate Architecture
1. **Gate 1 - Record Validation:** Validates schema, partner codes, and minimum amount thresholds. Errors are isolated per record without halting the batch.
2. **Gate 2 - Transform & Precision:** Computes service fees, taxes, and net totals using `Decimal(ROUND_HALF_UP)`. Outputs staging JSON and CSV.
3. **Gate 3 - Invariant Verification:** Re-reads files from disk and verifies aggregate consistency (item sums match batch header, record counts match 100%, within ceiling limit). Breaches trip the **Circuit Breaker** to auto-disable the DAG and trigger critical alerts.
