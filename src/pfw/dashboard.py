"""Build a self-contained static HTML dashboard from the mart views.

The page embeds its data as JSON and draws its own SVG charts, so it needs no
server, no build step and no network access. It can be opened from disk or
hosted on any static file host.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from importlib import resources
from pathlib import Path
from typing import Any

import duckdb

from pfw import db

PLACEHOLDER = "/*__DASHBOARD_DATA__*/null"


def _plain(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


def _query(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    return [{k: _plain(v) for k, v in row.items()} for row in db.rows(con, sql)]


def _by_currency(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(record.pop("currency_code"), []).append(record)
    return grouped


def collect(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Everything the dashboard shows, read from the marts and the run log."""
    latest_run = """(select run_id from ops.pipeline_run where status = 'SUCCESS'
                     order by business_date desc, started_at desc limit 1)"""
    return {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "currencies": [r["currency_code"] for r in _query(con, """
            select currency_code from mart.monthly_summary
            group by currency_code order by sum(attempts) desc""")],
        "totals": _query(con, """
            select
                (select count(*) from dw.fact_transaction)                      as payments,
                (select count(*) from dw.fact_refund)                           as refunds,
                (select count(*) from dw.fact_settlement)                       as settlement_batches,
                (select count(distinct merchant_id) from dw.dim_merchant)       as merchants,
                (select count(*) from dw.dim_merchant)                          as merchant_versions,
                (select min(activity_date) from mart.daily_kpis)                as first_date,
                (select max(business_date) from ops.file_load)                  as last_date,
                (select count(*) from ops.pipeline_run where status = 'SUCCESS') as runs_succeeded,
                (select count(*) from ops.pipeline_run where status = 'FAILED')  as runs_failed,
                (select coalesce(sum(raw_rows), 0) from ops.file_load)          as rows_delivered,
                (select coalesce(sum(staged_rows), 0) from ops.file_load)       as rows_loaded,
                (select coalesce(sum(rejected_rows), 0) from ops.file_load)     as rows_rejected,
                (select coalesce(sum(duplicate_rows), 0) from ops.file_load)    as rows_duplicate,
                (select coalesce(sum(payments), 0) from mart.late_arrivals)     as late_payments""")[0],
        "daily": _by_currency(_query(con, """
            select currency_code, activity_date as d, attempts, approved, captured_count,
                   captured_amount as captured, captured_amount_7d_avg as captured_7d,
                   approval_rate_pct as approval, approval_rate_7d_pct as approval_7d,
                   fee_amount as fees, refunded_amount as refunded
            from mart.daily_kpis order by currency_code, activity_date""")),
        "monthly": _by_currency(_query(con, """
            select currency_code, month_start, attempts, approval_rate_pct, captured_amount, fee_amount,
                   effective_fee_bps, refund_rate_pct, captured_growth_pct
            from mart.monthly_summary order by currency_code, month_start""")),
        "merchants": _by_currency(_query(con, """
            select currency_code, volume_rank, merchant_name, category, city, fee_plan_code, merchant_status,
                   captured_amount, share_of_currency_pct, approval_rate_pct, avg_ticket,
                   effective_fee_bps, refund_rate_pct
            from mart.merchant_scorecard order by currency_code, volume_rank""")),
        "declines": _by_currency(_query(con, """
            select currency_code, decline_reason, declines, share_of_declines_pct
            from mart.decline_reasons order by currency_code, declines desc""")),
        "brands": _by_currency(_query(con, """
            select currency_code, card_brand, attempts, approval_rate_pct, share_of_currency_pct
            from mart.card_brand_mix order by currency_code, attempts desc""")),
        "lag": _by_currency(_query(con, """
            select currency_code, lag_days, payments from mart.settlement_lag
            order by currency_code, lag_days""")),
        "unsettled": _by_currency(_query(con, """
            select currency_code, sum(payments) as payments, sum(amount_awaiting_settlement) as amount,
                   max(oldest_days) as oldest_days
            from mart.unsettled_aging group by currency_code""")),
        "checks": _query(con, f"""
            select check_name, severity, status, violations, description
            from ops.dq_result where run_id = {latest_run}
            order by case status when 'FAIL' then 0 when 'WARN' then 1 else 2 end, check_name"""),
        "warning_history": _query(con, """
            select check_name, count(*) as runs, min(business_date) as first_date, max(business_date) as last_date
            from ops.dq_result r
            where status = 'WARN'
              and run_id in (select run_id from ops.pipeline_run where status = 'SUCCESS')
            group by check_name order by check_name"""),
        "rejects": _query(con, """
            select file_name, reason, count(*) as row_count from ops.rejected_row
            group by file_name, reason order by row_count desc, reason"""),
        "merchant_changes": _query(con, """
            select merchant_name, version, valid_from, changed_attributes, category, city, fee_plan_code, status
            from mart.merchant_history where version > 1 order by valid_from, merchant_name"""),
        "runs": _query(con, """
            select a.business_date as d, a.raw_rows, a.rejected_rows, a.duplicate_rows
            from mart.load_accounting a order by a.business_date"""),
    }


def build(con: duckdb.DuckDBPyConnection, out_path: Path) -> Path:
    template = resources.files("pfw").joinpath("templates", "dashboard.html").read_text(encoding="utf-8")
    if PLACEHOLDER not in template:
        raise RuntimeError("Dashboard template is missing its data placeholder")
    payload = json.dumps(collect(con), separators=(",", ":")).replace("</", "<\\/")
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(template.replace(PLACEHOLDER, payload), encoding="utf-8", newline="\n")
    return out_path
