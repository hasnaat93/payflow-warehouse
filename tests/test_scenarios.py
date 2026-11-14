"""Hand-built deliveries with known answers: validation, de-duplication, the
slowly changing dimension, late arrivals, idempotency and failure handling."""

from __future__ import annotations

import json
from datetime import timedelta

from conftest import merchant, ts, txn
from pfw import pipeline
from pfw.pipeline import Outcome, Task

# ------------------------------------------------------------ validation


def test_money_is_parsed_exactly_to_minor_units(lab, day):
    lab.load(day, merchants=[merchant()], transactions=[
        txn("T-1", day, amount="0.07"),
        txn("T-2", day, amount="1234567.89"),
        txn("T-3", day, amount="5"),
        txn("T-4", day, amount="19.9"),
    ])
    assert lab.all("select txn_id, amount_minor from dw.fact_transaction order by 1") == [
        ("T-1", 7), ("T-2", 123456789), ("T-3", 500), ("T-4", 1990),
    ]


def test_bad_rows_are_quarantined_with_a_reason_and_good_rows_still_load(lab, day):
    good = txn("T-OK", day)
    bad = {
        "INVALID_AMOUNT": [
            txn("T-A1", day, amount="12.3x"), txn("T-A2", day, amount="-5.00"), txn("T-A3", day, amount="0.00"),
            txn("T-A4", day, amount="7.255"), txn("T-A5", day, amount=""), txn("T-A6", day, amount="1e3"),
        ],
        "MISSING_TXN_ID": [txn("", day), txn("   ", day)],
        "MISSING_MERCHANT_ID": [txn("T-M1", day, merchant="")],
        "UNKNOWN_CURRENCY": [txn("T-C1", day, currency="XXX"), txn("T-C2", day, currency="")],
        "INVALID_STATUS": [txn("T-S1", day, status="PENDING_REVIEW")],
        "INVALID_TIMESTAMP": [
            txn("T-T1", day, authorized="yesterday"),
            txn("T-T2", day, authorized="2026-08-01 10:00:00"),
            txn("T-T3", day, status="CAPTURED", captured="2026-13-45T00:00:00Z", fee="0.59"),
        ],
        "INVALID_FEE": [txn("T-F1", day, fee="free")],
        "INCONSISTENT_STATE": [
            txn("T-I1", day, status="CAPTURED"),                                          # captured, no capture time
            txn("T-I2", day, status="CAPTURED", captured=ts(day, "11:00:00")),             # captured, no fee
            txn("T-I3", day, settled=ts(day, "23:00:00"), batch="B-1"),                    # settled, never captured
        ],
    }
    rows = [good] + [row for group in bad.values() for row in group]
    result = lab.load(day, merchants=[merchant()], transactions=rows)

    assert lab.all("select txn_id from dw.fact_transaction") == [("T-OK",)]
    counted = dict(lab.all("select reason, count(*) from ops.rejected_row group by 1"))
    assert counted == {reason: len(group) for reason, group in bad.items()}

    # the quarantined record is the source row, verbatim
    reason, record = lab.one("select reason, record from ops.rejected_row where record like '%12.3x%'")
    assert reason == "INVALID_AMOUNT"
    assert json.loads(record)["txn_id"] == "T-A1"

    # every delivered row is accounted for, and a high reject rate is a warning, not a failure
    assert lab.one("""select raw_rows, staged_rows, rejected_rows, duplicate_rows from ops.file_load
                      where file_name = 'transactions.csv'""") == (len(rows), 1, len(rows) - 1, 0)
    assert "reject_rate_under_5_percent" in [c.check.name for c in result.warnings]


def test_duplicates_collapse_to_the_newest_version(lab, day):
    captured = txn("T-1", day, status="CAPTURED", captured=ts(day, "10:05:00"), fee="0.59")
    stale = txn("T-1", day)                      # the earlier AUTHORIZED version, listed last
    lab.load(day, merchants=[merchant()], transactions=[captured, list(captured), stale, txn("T-2", day)])

    assert lab.all("select txn_id, status, captured_minor, fee_minor from dw.fact_transaction order by 1") == [
        ("T-1", "CAPTURED", 1000, 59), ("T-2", "AUTHORIZED", 0, 0),
    ]
    assert lab.one("select raw_rows, staged_rows, rejected_rows, duplicate_rows from ops.file_load "
                   "where file_name = 'transactions.csv'") == (4, 2, 0, 2)


# ----------------------------------------------- accumulating snapshot fact


def test_a_payment_row_accumulates_capture_refund_and_settlement(lab, day):
    d1, d2, d3 = day, day + timedelta(days=1), day + timedelta(days=2)
    lab.load(d1, merchants=[merchant()], transactions=[txn("T-1", d1, amount="100.00")])
    lab.load(d2, merchants=[merchant()], transactions=[
        txn("T-1", d1, amount="100.00", status="CAPTURED", captured=ts(d2, "09:00:00"), fee="3.20"),
    ])
    lab.load(
        d3, merchants=[merchant()],
        transactions=[txn("T-1", d1, amount="100.00", status="PARTIALLY_REFUNDED", captured=ts(d2, "09:00:00"),
                          fee="3.20", settled=ts(d3, "23:00:00"), batch="B-1", updated=ts(d3, "23:00:00"))],
        refunds=[["R-1", "T-1", "30.00", "USD", ts(d3, "12:00:00"), "CUSTOMER_REQUEST"],
                 ["R-2", "T-1", "5.50", "USD", ts(d3, "13:00:00"), "CUSTOMER_REQUEST"]],
        settlements=[["B-1", "M-1", d3.isoformat(), "USD", "100.00", "3.20", "0.00", "96.80", "1"]],
    )
    assert lab.value("select count(*) from dw.fact_transaction") == 1
    assert lab.one("""select status, authorized_date_key, captured_date_key, settled_date_key, first_seen_date_key,
                             amount_minor, captured_minor, fee_minor, refunded_minor, settlement_batch_id
                      from dw.fact_transaction""") == (
        "PARTIALLY_REFUNDED", 20260801, 20260802, 20260803, 20260801, 10000, 10000, 320, 3550, "B-1",
    )
    assert lab.one("select is_reconciled, batch_net from mart.settlement_reconciliation")[0] is True
    # the aggregate attributes everything to the authorization date
    assert lab.all("select date_key, attempts, captured_minor, refunded_minor from dw.agg_daily_merchant") == [
        (20260801, 1, 10000, 3550),
    ]


def test_rerunning_a_date_changes_nothing(lab, day):
    lab.load(
        day, merchants=[merchant()],
        transactions=[txn("T-1", day, status="PARTIALLY_REFUNDED", captured=ts(day, "10:05:00"), fee="0.59",
                          updated=ts(day, "12:00:00")),
                      txn("T-2", day), txn("T-BAD", day, amount="oops")],
        refunds=[["R-1", "T-1", "4.00", "USD", ts(day, "12:00:00"), "CUSTOMER_REQUEST"]],
    )
    snapshot = """select
        (select count(*) from dw.fact_transaction), (select sum(refunded_minor) from dw.fact_transaction),
        (select count(*) from dw.fact_refund), (select count(*) from dw.dim_merchant),
        (select count(*) from raw.transactions), (select count(*) from ops.rejected_row),
        (select count(*) from ops.file_load), (select sum(attempts) from dw.agg_daily_merchant)"""
    before = lab.one(snapshot)
    for _ in range(2):
        assert lab.run(day).ok
    assert lab.one(snapshot) == before == (2, 400, 1, 1, 3, 1, 4, 2)
    assert lab.value("select count(*) from ops.pipeline_run where status = 'SUCCESS'") == 3


def test_reprocessing_an_old_date_never_moves_a_payment_backwards(lab, day):
    d1, d2 = day, day + timedelta(days=1)
    lab.load(d1, merchants=[merchant()], transactions=[txn("T-1", d1)])
    lab.load(d2, merchants=[merchant()], transactions=[
        txn("T-1", d1, status="CAPTURED", captured=ts(d2, "09:00:00"), fee="0.59"),
    ])
    assert lab.run(d1).ok       # replay the older file, which still says AUTHORIZED
    assert lab.one("select status, captured_minor from dw.fact_transaction") == ("CAPTURED", 1000)


def test_late_arriving_payment_is_dated_by_the_event_not_the_delivery(lab, day):
    d1, d4 = day, day + timedelta(days=3)
    lab.load(d1, merchants=[merchant()], transactions=[txn("T-1", d1)])
    lab.load(d4, merchants=[merchant()], transactions=[txn("T-LATE", d1), txn("T-4", d4)])
    assert lab.one("select authorized_date_key, first_seen_date_key from dw.fact_transaction "
                   "where txn_id = 'T-LATE'") == (20260801, 20260804)
    assert lab.all("select date_key, attempts from dw.agg_daily_merchant order by 1") == [(20260801, 2), (20260804, 1)]
    assert lab.all("select days_late, payments from mart.late_arrivals") == [(3, 1)]


def test_orphan_refund_is_quarantined_then_loads_when_the_date_is_replayed(lab, day):
    d1, d2 = day, day + timedelta(days=1)
    refund = ["R-1", "T-LATE", "4.00", "USD", ts(d1, "12:00:00"), "CUSTOMER_REQUEST"]
    lab.load(d1, merchants=[merchant()], transactions=[txn("T-1", d1)], refunds=[refund])
    assert lab.all("select reason from ops.rejected_row") == [("ORPHAN_REFUND",)]
    assert lab.value("select count(*) from dw.fact_refund") == 0

    captured = txn("T-LATE", d1, status="PARTIALLY_REFUNDED", captured=ts(d1, "10:05:00"), fee="0.59",
                   updated=ts(d1, "12:00:00"))
    arrived = lab.load(d2, merchants=[merchant()], transactions=[captured])
    # the payment says it was refunded, but its refund is still in quarantine: reported, not fatal
    assert [c.check.name for c in arrived.warnings] == ["refund_status_agrees_with_refunds"]

    replay = lab.run(d1)        # replay the day that held the refund
    assert replay.ok and replay.warnings == []
    assert lab.value("select count(*) from ops.rejected_row") == 0
    assert lab.one("select refunded_minor from dw.fact_transaction where txn_id = 'T-LATE'") == (400,)


# ------------------------------------------- slowly changing dimension


def test_attribute_change_opens_a_new_version_and_facts_keep_their_own(lab, day):
    d1, d2, d3 = day, day + timedelta(days=1), day + timedelta(days=2)
    lab.load(d1, merchants=[merchant(city="Boston")], transactions=[txn("T-1", d1)])
    lab.load(d2, merchants=[merchant(city="Boston")], transactions=[txn("T-2", d2)])     # no change
    lab.load(d3, merchants=[merchant(city="Denver", plan="PREMIUM")],
             transactions=[txn("T-3", d3), txn("T-LATE", d1)])

    assert lab.all("""select city, fee_plan_code, valid_from::varchar, valid_to::varchar, is_current
                      from dw.dim_merchant order by valid_from""") == [
        ("Boston", "STANDARD", "1900-01-01", "2026-08-03", False),
        ("Denver", "PREMIUM", "2026-08-03", "9999-12-31", True),
    ]
    # each payment joins to the city in force when it was authorized, including the late one
    assert lab.all("""select f.txn_id, d.city from dw.fact_transaction f
                      join dw.dim_merchant d using (merchant_sk) order by 1""") == [
        ("T-1", "Boston"), ("T-2", "Boston"), ("T-3", "Denver"), ("T-LATE", "Boston"),
    ]
    assert lab.all("select version, changed_attributes from mart.merchant_history order by 1") == [
        (1, ""), (2, "city, fee plan"),
    ]


def test_name_correction_overwrites_every_version(lab, day):
    d1, d2, d3 = day, day + timedelta(days=1), day + timedelta(days=2)
    lab.load(d1, merchants=[merchant(name="Alpha Stroe")])
    lab.load(d2, merchants=[merchant(name="Alpha Stroe", city="Denver")])
    lab.load(d3, merchants=[merchant(name="Alpha Store", city="Denver")])
    assert lab.all("select merchant_name, city from dw.dim_merchant order by valid_from") == [
        ("Alpha Store", "Boston"), ("Alpha Store", "Denver"),
    ]


def test_payment_for_an_unknown_merchant_gets_a_placeholder_that_is_completed_later(lab, day):
    d1, d2 = day, day + timedelta(days=1)
    first = lab.load(d1, merchants=[merchant()], transactions=[txn("T-1", d1, merchant="M-NEW")])
    assert "no_inferred_merchants_outstanding" in [c.check.name for c in first.warnings]
    placeholder = lab.one("select merchant_sk, merchant_name, is_inferred from dw.dim_merchant where merchant_id = 'M-NEW'")
    assert placeholder[1:] == ("Unknown merchant (inferred)", True)

    second = lab.load(d2, merchants=[merchant(), merchant("M-NEW", "Newcomer Ltd", city="Austin")],
                      transactions=[txn("T-2", d2)])
    assert second.warnings == []
    assert lab.all("select merchant_sk, merchant_name, city, is_inferred, valid_from::varchar from dw.dim_merchant "
                   "where merchant_id = 'M-NEW'") == [(placeholder[0], "Newcomer Ltd", "Austin", False, "1900-01-01")]
    assert lab.one("select d.merchant_name from dw.fact_transaction f join dw.dim_merchant d using (merchant_sk) "
                   "where f.txn_id = 'T-1'") == ("Newcomer Ltd",)


def test_an_older_merchant_snapshot_is_not_applied_over_a_newer_one(lab, day):
    d1, d2 = day, day + timedelta(days=1)
    lab.deliver(d1, merchants=[merchant(city="Boston")])
    lab.load(d2, merchants=[merchant(city="Denver")])
    result = lab.run(d1)
    assert result.ok
    assert [t.status for t in result.tasks if t.task == "load_dim_merchant"] == ["SKIPPED"]
    assert lab.all("select city from dw.dim_merchant") == [("Denver",)]


# ------------------------------------------------------ failure handling


def _state(lab):
    return lab.one("""select (select count(*) from dw.fact_transaction), (select count(*) from raw.transactions),
                             (select count(*) from dw.dim_merchant), (select count(*) from ops.file_load)""")


def test_a_tampered_file_fails_the_run_and_loads_nothing(lab, day):
    d1, d2 = day, day + timedelta(days=1)
    lab.load(d1, merchants=[merchant()], transactions=[txn("T-1", d1)])
    before = _state(lab)

    folder = lab.deliver(d2, merchants=[merchant()], transactions=[txn("T-2", d2)])
    with (folder / "transactions.csv").open("a", encoding="utf-8", newline="") as handle:
        handle.write(",".join(txn("T-EVIL", d2, amount="999999.00")) + "\n")
    result = lab.run(d2)

    assert not result.ok
    assert "checksum does not match" in result.error
    assert _state(lab) == before
    assert lab.one("select status, error like '%checksum%' from ops.pipeline_run where business_date = '2026-08-02'") == (
        "FAILED", True)
    assert lab.all("select task, status from ops.task_run where run_id = ?".replace("?", f"'{result.run_id}'")) == [
        ("verify_manifest", "FAILED")]


def test_a_changed_file_layout_fails_the_run(lab, day):
    folder = lab.deliver(day, merchants=[merchant()], transactions=[txn("T-1", day)])
    path = folder / "transactions.csv"
    drifted = path.read_text(encoding="utf-8").replace("txn_id,merchant_id", "transaction_id,merchant_id")
    path.write_bytes(drifted.encode("utf-8"))
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    import hashlib
    manifest["files"]["transactions.csv"]["sha256"] = hashlib.sha256(drifted.encode("utf-8")).hexdigest()
    (folder / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    result = lab.run(day)
    assert not result.ok and "unexpected columns" in result.error


def test_a_missing_delivery_fails_cleanly(lab, day):
    result = lab.run(day)
    assert not result.ok and "No manifest" in result.error


def test_a_row_count_that_disagrees_with_the_manifest_fails_the_run(lab, day):
    folder = lab.deliver(day, merchants=[merchant()], transactions=[txn("T-1", day)])
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    manifest["files"]["transactions.csv"]["rows"] = 5
    (folder / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    result = lab.run(day)
    assert not result.ok and "declares 5 rows but 1 were read" in result.error
    assert lab.value("select count(*) from raw.transactions") == 0


def test_a_failed_quality_check_rolls_the_whole_day_back(lab, day):
    d1, d2 = day, day + timedelta(days=1)
    lab.load(d1, merchants=[merchant()], transactions=[txn("T-1", d1)])
    before = _state(lab)
    lab.deliver(d2, merchants=[merchant(city="Denver")], transactions=[txn("T-2", d2)])

    def corrupt(ctx):   # simulate a defect in a load step: the same payment written twice
        ctx.con.execute("insert into dw.fact_transaction select * from dw.fact_transaction where txn_id = 'T-2'")
        return Outcome(rows=1)

    tasks = pipeline.default_tasks()
    tasks.insert(-1, Task("corrupt", corrupt, ("load_fact_transaction",)))
    tasks[-1] = Task("quality_gate", tasks[-1].run, (*tasks[-1].depends_on, "corrupt"))
    result = lab.run(d2, tasks=tasks)

    assert not result.ok
    failed = {c.check.name for c in result.checks if c.status == "FAIL"}
    assert "fact_transaction_unique_txn_id" in failed
    # nothing from the day survived: not the payment, not the raw rows, not the new merchant version
    assert _state(lab) == before
    assert lab.all("select city from dw.dim_merchant") == [("Boston",)]
    # but the evidence did
    assert lab.value("select count(*) from ops.dq_result where status = 'FAIL'") >= 1

    assert lab.run(d2).ok       # the same delivery loads once the defect is gone
    assert lab.value("select count(*) from dw.fact_transaction") == 2


def test_backfill_stops_at_the_first_failed_date(lab, day):
    for offset in range(3):
        d = day + timedelta(days=offset)
        lab.deliver(d, merchants=[merchant()], transactions=[txn(f"T-{offset}", d)])
    (lab.landing / (day + timedelta(days=1)).isoformat() / "refunds.csv").unlink()
    results = pipeline.backfill(lab.con, lab.landing)
    assert [r.status for r in results] == ["SUCCESS", "FAILED"]
    assert lab.all("select txn_id from dw.fact_transaction") == [("T-0",)]
