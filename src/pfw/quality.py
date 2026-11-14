"""Data quality checks that gate every pipeline run.

Each check is a SQL query returning the number of violating rows. An ERROR
check with violations fails the run, and the run's transaction is rolled back,
so a load that breaks an invariant never reaches the warehouse. A WARN check is
recorded and reported but does not stop the load.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import duckdb

from pfw import db

ERROR = "ERROR"
WARN = "WARN"


@dataclass(frozen=True)
class Check:
    name: str
    severity: str
    description: str
    sql: str


@dataclass(frozen=True)
class CheckResult:
    check: Check
    violations: int

    @property
    def status(self) -> str:
        if self.violations == 0:
            return "PASS"
        return "FAIL" if self.check.severity == ERROR else "WARN"


CHECKS: list[Check] = [
    # ---- keys (the warehouse does not enforce them, so the pipeline asserts them)
    Check("fact_transaction_unique_txn_id", ERROR, "A payment appears once in fact_transaction",
          "select count(*) from (select txn_id from dw.fact_transaction group by txn_id having count(*) > 1)"),
    Check("fact_refund_unique_refund_id", ERROR, "A refund appears once in fact_refund",
          "select count(*) from (select refund_id from dw.fact_refund group by refund_id having count(*) > 1)"),
    Check("fact_settlement_unique_batch_id", ERROR, "A settlement batch appears once in fact_settlement",
          "select count(*) from (select batch_id from dw.fact_settlement group by batch_id having count(*) > 1)"),
    Check("dim_merchant_unique_surrogate_key", ERROR, "merchant_sk is unique",
          "select count(*) from (select merchant_sk from dw.dim_merchant group by merchant_sk having count(*) > 1)"),

    # ---- slowly changing dimension integrity
    Check("dim_merchant_one_current_version", ERROR, "Every merchant has exactly one current version",
          """select count(*) from (
                 select merchant_id from dw.dim_merchant
                 group by merchant_id having count(*) filter (where is_current) <> 1)"""),
    Check("dim_merchant_versions_contiguous", ERROR,
          "Merchant versions neither overlap nor leave gaps, and run from 1900-01-01 to 9999-12-31",
          """select count(*) from (
                 select
                     valid_from, valid_to,
                     lag(valid_to) over w  as previous_valid_to,
                     lead(valid_from) over w as next_valid_from
                 from dw.dim_merchant
                 window w as (partition by merchant_id order by valid_from))
             where valid_from >= valid_to
                or valid_from <> coalesce(previous_valid_to, date '1900-01-01')
                or valid_to <> coalesce(next_valid_from, date '9999-12-31')"""),

    # ---- referential integrity
    Check("fact_transaction_merchant_key_resolves", ERROR,
          "Every payment points at the merchant version valid on its authorization date",
          """select count(*) from dw.fact_transaction f
             left join dw.dim_merchant d
               on d.merchant_sk = f.merchant_sk and d.merchant_id = f.merchant_id
              and cast(f.authorized_at as date) >= d.valid_from and cast(f.authorized_at as date) < d.valid_to
             where d.merchant_sk is null"""),
    Check("fact_transaction_date_keys_resolve", ERROR, "Every date key on a payment exists in dim_date",
          """select count(*) from dw.fact_transaction f
             where not exists (select 1 from dw.dim_date d where d.date_key = f.authorized_date_key)
                or (f.captured_date_key is not null
                    and not exists (select 1 from dw.dim_date d where d.date_key = f.captured_date_key))
                or (f.settled_date_key is not null
                    and not exists (select 1 from dw.dim_date d where d.date_key = f.settled_date_key))"""),
    Check("fact_refund_payment_exists", ERROR, "Every refund belongs to a payment in the warehouse",
          """select count(*) from dw.fact_refund r
             where not exists (select 1 from dw.fact_transaction f where f.txn_id = r.txn_id)"""),
    Check("fact_settlement_merchant_key_resolves", ERROR, "Every settlement batch points at a merchant version",
          """select count(*) from dw.fact_settlement s
             where not exists (select 1 from dw.dim_merchant d where d.merchant_sk = s.merchant_sk)"""),

    # ---- business rules
    Check("payment_amounts_valid", ERROR,
          "Amounts are positive, and fee and refunds never exceed the captured amount",
          """select count(*) from dw.fact_transaction
             where amount_minor <= 0 or captured_minor < 0 or fee_minor < 0 or refunded_minor < 0
                or captured_minor not in (0, amount_minor)
                or fee_minor > captured_minor or refunded_minor > captured_minor"""),
    Check("payment_state_consistent", ERROR,
          "Status agrees with the capture and settlement columns",
          """select count(*) from dw.fact_transaction
             where status not in ('AUTHORIZED', 'DECLINED', 'CAPTURED', 'REVERSED', 'PARTIALLY_REFUNDED', 'REFUNDED')
                or (status in ('CAPTURED', 'PARTIALLY_REFUNDED', 'REFUNDED') and captured_date_key is null)
                or (status in ('AUTHORIZED', 'DECLINED', 'REVERSED') and captured_minor <> 0)
                or (status = 'DECLINED' and decline_reason is null)
                or (settled_date_key is not null and (captured_date_key is null or settlement_batch_id is null))
                or captured_date_key < authorized_date_key
                or settled_date_key < captured_date_key"""),
    Check("refund_totals_roll_up", ERROR, "refunded_minor on a payment equals the sum of its refunds",
          """select count(*) from dw.fact_transaction f
             left join (select txn_id, sum(amount_minor) as total from dw.fact_refund group by txn_id) r
               on r.txn_id = f.txn_id
             where f.refunded_minor <> coalesce(r.total, 0)"""),
    Check("settlement_arithmetic", ERROR, "For every batch, net = gross - fees - refunds, and net is positive",
          """select count(*) from dw.fact_settlement
             where net_minor <> gross_minor - fee_minor - refund_minor or net_minor <= 0"""),
    Check("settlement_matches_payments", ERROR,
          "A batch's count, gross and fees equal the payments the warehouse holds for it",
          "select count(*) from mart.settlement_reconciliation where not is_reconciled"),

    # ---- control totals between layers
    Check("file_row_accounting", ERROR,
          "For today's files: rows delivered = rows loaded + rows quarantined + duplicates dropped",
          """select count(*) from ops.file_load
             where business_date = $business_date
               and (declared_rows <> raw_rows or raw_rows <> staged_rows + rejected_rows + duplicate_rows)"""),
    Check("aggregate_matches_fact", ERROR, "agg_daily_merchant reconciles to fact_transaction for every date",
          """select count(*) from (
                 select coalesce(a.date_key, f.date_key) as date_key
                 from (select date_key, sum(attempts) as n, sum(captured_minor) as c, sum(refunded_minor) as r
                       from dw.agg_daily_merchant group by date_key) a
                 full join (select authorized_date_key as date_key, count(*) as n,
                                   sum(captured_minor) as c, sum(refunded_minor) as r
                            from dw.fact_transaction group by authorized_date_key) f
                   on f.date_key = a.date_key
                 where a.n is distinct from f.n or a.c is distinct from f.c or a.r is distinct from f.r)"""),

    # ---- warnings: worth a look, not a reason to stop the load
    Check("reject_rate_under_5_percent", WARN, "Fewer than 5% of today's delivered rows were quarantined",
          """select case when sum(rejected_rows) * 20 > sum(raw_rows) then sum(rejected_rows) else 0 end
             from ops.file_load where business_date = $business_date"""),
    Check("payments_delivered_today", WARN, "Today's transaction file delivered at least one payment",
          """select case when coalesce(sum(staged_rows), 0) = 0 then 1 else 0 end
             from ops.file_load where business_date = $business_date and file_name = 'transactions.csv'"""),
    Check("refund_status_agrees_with_refunds", WARN,
          "A payment's refund status agrees with the refunds loaded for it (they arrive in separate files)",
          """select count(*) from dw.fact_transaction
             where (status = 'REFUNDED' and refunded_minor <> captured_minor)
                or (status = 'PARTIALLY_REFUNDED' and not (refunded_minor > 0 and refunded_minor < captured_minor))
                or (status = 'CAPTURED' and refunded_minor <> 0)"""),
    Check("no_inferred_merchants_outstanding", WARN,
          "No merchant is still a placeholder waiting for the merchant extract",
          "select count(*) from dw.dim_merchant where is_inferred"),
    Check("no_stale_unsettled_payments", WARN, "No captured payment has waited more than 5 days for settlement",
          """select count(*) from dw.fact_transaction
             where captured_minor > 0 and settled_date_key is null
               and date_diff('day', cast(captured_at as date), $business_date) > 5"""),
]


class QualityGateError(RuntimeError):
    def __init__(self, failures: list[CheckResult]) -> None:
        self.failures = failures
        summary = ", ".join(f"{f.check.name} ({f.violations})" for f in failures)
        super().__init__(f"Data quality gate failed: {summary}")


def run_checks(con: duckdb.DuckDBPyConnection, business_date: date) -> list[CheckResult]:
    params = {"business_date": business_date}
    return [CheckResult(check, int(db.scalar(con, check.sql, params) or 0)) for check in CHECKS]
