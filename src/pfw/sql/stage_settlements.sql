-- Type, validate and de-duplicate the day's settlement batch extract.

create or replace table stg.settlements_checked as
with parsed as (
    select
        r.*,
        nullif(trim(r.batch_id), '')        as batch_id_clean,
        nullif(trim(r.merchant_id), '')     as merchant_id_clean,
        try_cast(r.settlement_date as date) as settlement_dt,
        case when regexp_full_match(trim(r.gross_amount), '[0-9]{1,13}(\.[0-9]{1,2})?')
             then cast(cast(trim(r.gross_amount) as decimal(18, 2)) * 100 as bigint) end as gross_minor,
        case when regexp_full_match(trim(r.fee_amount), '[0-9]{1,13}(\.[0-9]{1,2})?')
             then cast(cast(trim(r.fee_amount) as decimal(18, 2)) * 100 as bigint) end as fee_minor,
        case when regexp_full_match(trim(r.refund_amount), '[0-9]{1,13}(\.[0-9]{1,2})?')
             then cast(cast(trim(r.refund_amount) as decimal(18, 2)) * 100 as bigint) end as refund_minor,
        case when regexp_full_match(trim(r.net_amount), '[0-9]{1,13}(\.[0-9]{1,2})?')
             then cast(cast(trim(r.net_amount) as decimal(18, 2)) * 100 as bigint) end as net_minor,
        try_cast(r.txn_count as bigint)     as txn_count_n
    from raw.settlements r
    where r._business_date = $business_date
)
select
    p.*,
    case
        when p.batch_id_clean is null                               then 'MISSING_BATCH_ID'
        when p.merchant_id_clean is null                            then 'MISSING_MERCHANT_ID'
        when p.settlement_dt is null                                then 'INVALID_DATE'
        when p.currency is null
          or p.currency not in (select currency_code from dw.dim_currency) then 'UNKNOWN_CURRENCY'
        when p.gross_minor is null or p.fee_minor is null
          or p.refund_minor is null or p.net_minor is null          then 'INVALID_AMOUNT'
        when p.txn_count_n is null or p.txn_count_n <= 0            then 'INVALID_COUNT'
    end as reject_reason
from parsed p;

insert into ops.rejected_row
select
    $run_id, c._business_date, c._file_name, c._row_number, c.reject_reason,
    cast(json_object(
        'batch_id', c.batch_id, 'merchant_id', c.merchant_id, 'settlement_date', c.settlement_date,
        'currency', c.currency, 'gross_amount', c.gross_amount, 'fee_amount', c.fee_amount,
        'refund_amount', c.refund_amount, 'net_amount', c.net_amount, 'txn_count', c.txn_count
    ) as varchar)
from stg.settlements_checked c
where c.reject_reason is not null;

create or replace table stg.settlements as
select
    batch_id_clean      as batch_id,
    merchant_id_clean   as merchant_id,
    settlement_dt       as settlement_date,
    currency            as currency_code,
    gross_minor,
    fee_minor,
    refund_minor,
    net_minor,
    txn_count_n         as txn_count
from (
    select
        c.*,
        row_number() over (partition by c.batch_id_clean order by c._row_number desc) as version_rank
    from stg.settlements_checked c
    where c.reject_reason is null
)
where version_rank = 1;
