"""The full simulated feed, loaded day by day, compared with the simulator's own books."""

from __future__ import annotations

import json
import re

from pfw import dashboard, exports


def test_every_daily_run_succeeds(loaded):
    assert len(loaded.results) == 45
    assert all(r.ok for r in loaded.results)
    assert loaded.con.execute("select count(*) from ops.dq_result where status = 'FAIL'").fetchone()[0] == 0


def test_warehouse_matches_the_source_system_payment_by_payment(loaded):
    """The simulator knows the true final state of every payment it delivered."""
    expected = {
        t.txn_id: (t.merchant_id, t.status, t.amount, t.amount if t.captured_at else 0, t.fee or 0, t.refunded,
                   t.batch_id or None, int(t.authorized_at.strftime("%Y%m%d")))
        for t in loaded.simulator.txns.values()
        if t.visible_from <= loaded.last_day
    }
    actual = {
        row[0]: row[1:]
        for row in loaded.con.execute("""
            select txn_id, merchant_id, status, amount_minor, captured_minor, fee_minor, refunded_minor,
                   settlement_batch_id, authorized_date_key
            from dw.fact_transaction""").fetchall()
    }
    assert len(actual) == len(expected) > 10000
    assert actual == expected


def test_refunds_and_settlements_match_the_source_system(loaded):
    refunds = loaded.con.execute("select count(*), sum(amount_minor) from dw.fact_refund").fetchone()
    assert refunds == (len(loaded.simulator.refunds), sum(r.amount for r in loaded.simulator.refunds))
    batches, reconciled = loaded.con.execute(
        "select count(*), count(*) filter (where is_reconciled) from mart.settlement_reconciliation").fetchone()
    assert batches == loaded.simulator.batch_seq == reconciled


def test_every_delivered_row_is_accounted_for(loaded):
    delivered, loaded_rows, rejected, duplicates = loaded.con.execute(
        "select sum(raw_rows), sum(staged_rows), sum(rejected_rows), sum(duplicate_rows) from ops.file_load").fetchone()
    assert delivered == loaded_rows + rejected + duplicates
    assert rejected > 0 and duplicates > 0
    assert loaded.con.execute("select count(*) from ops.rejected_row").fetchone()[0] == rejected
    # nothing malformed leaked into the warehouse
    assert loaded.con.execute("select count(*) from dw.fact_transaction where txn_id like 'TXN-BAD%'").fetchone()[0] == 0


def test_dimension_history_reflects_the_scripted_changes(loaded):
    versions = dict(loaded.con.execute(
        "select merchant_id, count(*) from dw.dim_merchant group by 1").fetchall())
    assert versions["M-1002"] == 2      # fee plan change
    assert versions["M-2001"] == 2      # relocation
    assert versions["M-3002"] == 2      # reclassification
    assert versions["M-1004"] == 1      # a name correction is type 1: no new version
    assert loaded.con.execute(
        "select merchant_name from dw.dim_merchant where merchant_id = 'M-1004'").fetchone()[0] == "Bluefin Streaming Co."
    # the merchant that transacted before its extract arrived was a placeholder for exactly two runs
    assert loaded.con.execute("select count(*) from dw.dim_merchant where is_inferred").fetchone()[0] == 0
    warned = loaded.con.execute("""select count(*) from ops.dq_result
                                   where check_name = 'no_inferred_merchants_outstanding' and status = 'WARN'""").fetchone()[0]
    assert warned == 2


def test_daily_kpis_cover_every_calendar_day_and_agree_with_the_fact(loaded):
    days, currencies = loaded.con.execute(
        "select count(distinct activity_date), count(distinct currency_code) from mart.daily_kpis").fetchone()
    assert (days, currencies) == (45, 3)
    mart = loaded.con.execute(
        "select currency_code, cast(sum(captured_amount) * 100 as bigint) from mart.daily_kpis group by 1 order by 1").fetchall()
    fact = loaded.con.execute(
        "select currency_code, sum(captured_minor) from dw.fact_transaction group by 1 order by 1").fetchall()
    assert mart == fact
    shares = loaded.con.execute(
        "select currency_code, sum(share_of_currency_pct) from mart.merchant_scorecard group by 1").fetchall()
    assert all(abs(float(total) - 100) < 0.1 for _, total in shares)


def test_dashboard_and_exports_are_built_from_the_marts(loaded, tmp_path):
    page = dashboard.build(loaded.con, tmp_path / "site" / "index.html").read_text(encoding="utf-8")
    assert dashboard.PLACEHOLDER not in page
    data = json.loads(re.search(r"const DATA = (\{.*?\});\n", page, re.S).group(1).replace("<\\/", "</"))
    assert data["currencies"][0] == "USD" and set(data["currencies"]) == {"USD", "EUR", "PKR"}
    assert len(data["daily"]["USD"]) == 45
    assert data["totals"]["runs_succeeded"] == 45 and data["totals"]["runs_failed"] == 0
    assert all(c["status"] == "PASS" for c in data["checks"])

    views = exports.export_marts(loaded.con, tmp_path / "exports")
    assert "daily_kpis" in views and len(views) >= 10
    for view in views:
        assert (tmp_path / "exports" / f"{view}.parquet").stat().st_size > 0
        assert (tmp_path / "exports" / f"{view}.csv").stat().st_size > 0
    rows = loaded.con.execute(
        f"select count(*) from read_parquet('{(tmp_path / 'exports' / 'daily_kpis.parquet').as_posix()}')").fetchone()[0]
    assert rows == 135
