# Trigger DAG: finance_trigger_daily_partner_invoicing

## Overview
Config-driven trigger DAG that manages execution schedules for partner invoicing pipelines:
- Loads declarative trigger definitions from `triggers.yml`.
- Evaluates `business_day_check` to skip weekends and holidays.
- Resolves dynamic date template expressions (`{{ today }}`, `{{ next_2_business_day }}`).
- Fans out triggers to target downstream processing DAGs (`finance_process_partner_billing`).

Decoupling scheduling from processing guarantees that processing DAGs remain idempotent (`schedule=None`) and can be re-run manually with custom parameters at any time.
