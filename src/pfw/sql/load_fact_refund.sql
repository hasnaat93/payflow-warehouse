-- Load the day's refunds. Runs after the payments have been merged.

-- A refund whose payment the warehouse does not hold is quarantined, not loaded.
-- Re-running the date after the payment has arrived will load it.
insert into ops.rejected_row
select $run_id, $business_date, s.file_name, s.row_number, 'ORPHAN_REFUND', s.record
from stg.refunds s
where not exists (select 1 from dw.fact_transaction f where f.txn_id = s.txn_id);

insert into dw.fact_refund
select
    s.refund_id,
    s.txn_id,
    f.merchant_sk,
    f.merchant_id,
    f.currency_code,
    cast(strftime(s.refunded_ts, '%Y%m%d') as integer),
    s.refunded_ts,
    s.amount_minor,
    s.reason,
    $run_id
from stg.refunds s
join dw.fact_transaction f on f.txn_id = s.txn_id
where not exists (select 1 from dw.fact_refund r where r.refund_id = s.refund_id);

-- Roll the refund total up onto the payment row.
update dw.fact_transaction as f
set refunded_minor = r.refunded_minor,
    updated_run_id = $run_id
from (
    select txn_id, sum(amount_minor) as refunded_minor
    from dw.fact_refund
    where txn_id in (select txn_id from stg.refunds)
    group by txn_id
) r
where f.txn_id = r.txn_id
  and f.refunded_minor <> r.refunded_minor;
