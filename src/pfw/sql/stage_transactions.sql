-- Type, validate and de-duplicate the day's transaction extract.
-- Every raw row ends up in exactly one place: staged, quarantined, or counted as a duplicate.

create or replace table stg.transactions_checked as
with parsed as (
    select
        r.*,
        nullif(trim(r.txn_id), '')              as txn_id_clean,
        nullif(trim(r.merchant_id), '')         as merchant_id_clean,
        -- Money is parsed from text straight to integer minor units. Anything that is not
        -- a plain non-negative decimal with at most two places is rejected, not rounded.
        case when regexp_full_match(trim(r.amount), '[0-9]{1,13}(\.[0-9]{1,2})?')
             then cast(cast(trim(r.amount) as decimal(18, 2)) * 100 as bigint) end as amount_minor,
        case when regexp_full_match(trim(r.fee_amount), '[0-9]{1,13}(\.[0-9]{1,2})?')
             then cast(cast(trim(r.fee_amount) as decimal(18, 2)) * 100 as bigint) end as fee_minor,
        try_strptime(r.authorized_at, '%Y-%m-%dT%H:%M:%SZ') as authorized_ts,
        try_strptime(r.captured_at,   '%Y-%m-%dT%H:%M:%SZ') as captured_ts,
        try_strptime(r.settled_at,    '%Y-%m-%dT%H:%M:%SZ') as settled_ts,
        try_strptime(r.updated_at,    '%Y-%m-%dT%H:%M:%SZ') as updated_ts
    from raw.transactions r
    where r._business_date = $business_date
)
select
    p.*,
    case
        when p.txn_id_clean is null                                         then 'MISSING_TXN_ID'
        when p.merchant_id_clean is null                                    then 'MISSING_MERCHANT_ID'
        when p.amount_minor is null or p.amount_minor <= 0                  then 'INVALID_AMOUNT'
        when p.currency is null
          or p.currency not in (select currency_code from dw.dim_currency)  then 'UNKNOWN_CURRENCY'
        when p.status is null
          or p.status not in ('AUTHORIZED', 'DECLINED', 'CAPTURED', 'REVERSED',
                              'PARTIALLY_REFUNDED', 'REFUNDED')              then 'INVALID_STATUS'
        when p.authorized_ts is null or p.updated_ts is null                then 'INVALID_TIMESTAMP'
        when (p.captured_at is not null and p.captured_ts is null)
          or (p.settled_at is not null and p.settled_ts is null)            then 'INVALID_TIMESTAMP'
        when p.fee_amount is not null and p.fee_minor is null               then 'INVALID_FEE'
        when p.status in ('CAPTURED', 'PARTIALLY_REFUNDED', 'REFUNDED')
         and (p.captured_ts is null or p.fee_minor is null)                 then 'INCONSISTENT_STATE'
        when p.settled_ts is not null
         and (p.captured_ts is null or p.settlement_batch_id is null)       then 'INCONSISTENT_STATE'
    end as reject_reason
from parsed p;

insert into ops.rejected_row
select
    $run_id, c._business_date, c._file_name, c._row_number, c.reject_reason,
    cast(json_object(
        'txn_id', c.txn_id, 'merchant_id', c.merchant_id, 'amount', c.amount, 'currency', c.currency,
        'card_brand', c.card_brand, 'status', c.status, 'decline_reason', c.decline_reason,
        'authorized_at', c.authorized_at, 'captured_at', c.captured_at, 'fee_amount', c.fee_amount,
        'settled_at', c.settled_at, 'settlement_batch_id', c.settlement_batch_id, 'updated_at', c.updated_at
    ) as varchar)
from stg.transactions_checked c
where c.reject_reason is not null;

-- Keep the newest version of each payment. Exact duplicates and superseded
-- versions inside the same file fall out here.
create or replace table stg.transactions as
select
    txn_id_clean                        as txn_id,
    merchant_id_clean                   as merchant_id,
    currency                            as currency_code,
    coalesce(card_brand, 'UNKNOWN')     as card_brand,
    status,
    decline_reason,
    authorized_ts,
    captured_ts,
    settled_ts,
    updated_ts,
    amount_minor,
    coalesce(fee_minor, 0)              as fee_minor,
    settlement_batch_id,
    _business_date                      as business_date
from (
    select
        c.*,
        row_number() over (partition by c.txn_id_clean order by c.updated_ts desc, c._row_number desc) as version_rank
    from stg.transactions_checked c
    where c.reject_reason is null
)
where version_rank = 1;
