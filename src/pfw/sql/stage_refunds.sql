-- Type, validate and de-duplicate the day's refund extract.
-- The orphan check (refund for a payment the warehouse does not hold) happens
-- later, in load_fact_refund.sql, once the day's payments have been merged.

create or replace table stg.refunds_checked as
with parsed as (
    select
        r.*,
        nullif(trim(r.refund_id), '')   as refund_id_clean,
        nullif(trim(r.txn_id), '')      as txn_id_clean,
        case when regexp_full_match(trim(r.amount), '[0-9]{1,13}(\.[0-9]{1,2})?')
             then cast(cast(trim(r.amount) as decimal(18, 2)) * 100 as bigint) end as amount_minor,
        try_strptime(r.refunded_at, '%Y-%m-%dT%H:%M:%SZ') as refunded_ts
    from raw.refunds r
    where r._business_date = $business_date
)
select
    p.*,
    case
        when p.refund_id_clean is null                              then 'MISSING_REFUND_ID'
        when p.txn_id_clean is null                                 then 'MISSING_TXN_ID'
        when p.amount_minor is null or p.amount_minor <= 0          then 'INVALID_AMOUNT'
        when p.refunded_ts is null                                  then 'INVALID_TIMESTAMP'
    end as reject_reason
from parsed p;

insert into ops.rejected_row
select
    $run_id, c._business_date, c._file_name, c._row_number, c.reject_reason,
    cast(json_object(
        'refund_id', c.refund_id, 'txn_id', c.txn_id, 'amount', c.amount, 'currency', c.currency,
        'refunded_at', c.refunded_at, 'reason', c.reason
    ) as varchar)
from stg.refunds_checked c
where c.reject_reason is not null;

create or replace table stg.refunds as
select
    refund_id_clean     as refund_id,
    txn_id_clean        as txn_id,
    amount_minor,
    refunded_ts,
    reason,
    _file_name          as file_name,
    _row_number         as row_number,
    cast(json_object(
        'refund_id', refund_id, 'txn_id', txn_id, 'amount', amount, 'currency', currency,
        'refunded_at', refunded_at, 'reason', reason
    ) as varchar)       as record
from (
    select
        c.*,
        row_number() over (partition by c.refund_id_clean order by c._row_number desc) as version_rank
    from stg.refunds_checked c
    where c.reject_reason is null
)
where version_rank = 1;
