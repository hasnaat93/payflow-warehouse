-- Incremental aggregate maintenance: rebuild only the authorization dates that
-- this run touched (new payments, status changes, refunds), not the whole table.

create or replace table stg.affected_dates as
select distinct f.authorized_date_key as date_key
from dw.fact_transaction f
where f.txn_id in (select txn_id from stg.transactions)
   or f.txn_id in (select txn_id from stg.refunds);

delete from dw.agg_daily_merchant
where date_key in (select date_key from stg.affected_dates);

insert into dw.agg_daily_merchant
select
    f.authorized_date_key,
    f.merchant_id,
    f.currency_code,
    count(*)                                            as attempts,
    count(*) filter (where f.status <> 'DECLINED')      as approved,
    count(*) filter (where f.status = 'DECLINED')       as declined,
    count(*) filter (where f.captured_minor > 0)        as captured_count,
    coalesce(sum(f.captured_minor), 0)                  as captured_minor,
    coalesce(sum(f.fee_minor), 0)                       as fee_minor,
    coalesce(sum(f.refunded_minor), 0)                  as refunded_minor
from dw.fact_transaction f
where f.authorized_date_key in (select date_key from stg.affected_dates)
group by f.authorized_date_key, f.merchant_id, f.currency_code;
