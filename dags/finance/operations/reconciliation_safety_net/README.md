# DAG: finance_reconcile_unsettled_billing

## Overview
Nightly safety-net and reconciliation job (runs at 23:30 UTC):
- Scans for orphaned transactions stuck in intermediate states (`PROCESSING`, `VALIDATED`) beyond time thresholds due to network timeouts, service restarts, or unhandled exceptions.
- Dispatches alert notifications with record IDs to operations channels, enabling proactive investigation and safeguarding financial data consistency.
