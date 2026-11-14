-- Load the day's settlement batches. A batch is immutable once issued, so this is
-- insert-if-absent, keyed on batch_id.

insert into dw.fact_settlement
select
    s.batch_id,
    d.merchant_sk,
    s.merchant_id,
    s.currency_code,
    cast(strftime(s.settlement_date, '%Y%m%d') as integer),
    s.gross_minor,
    s.fee_minor,
    s.refund_minor,
    s.net_minor,
    s.txn_count,
    $run_id
from stg.settlements s
join dw.dim_merchant d
  on d.merchant_id = s.merchant_id
 and s.settlement_date >= d.valid_from
 and s.settlement_date <  d.valid_to
where not exists (select 1 from dw.fact_settlement f where f.batch_id = s.batch_id);
