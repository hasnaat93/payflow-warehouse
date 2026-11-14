-- Type, validate and de-duplicate the day's merchant snapshot.

create or replace table stg.merchants_checked as
select
    r.*,
    nullif(trim(r.merchant_id), '')     as merchant_id_clean,
    try_cast(r.onboarded_on as date)    as onboarded_date,
    case
        when nullif(trim(r.merchant_id), '') is null                         then 'MISSING_MERCHANT_ID'
        when nullif(trim(r.merchant_name), '') is null                       then 'MISSING_MERCHANT_NAME'
        when r.settlement_currency is null
          or r.settlement_currency not in (select currency_code from dw.dim_currency) then 'UNKNOWN_CURRENCY'
        when r.status is null or r.status not in ('ACTIVE', 'SUSPENDED', 'CLOSED') then 'INVALID_STATUS'
        when r.onboarded_on is not null and try_cast(r.onboarded_on as date) is null then 'INVALID_DATE'
    end as reject_reason
from raw.merchants r
where r._business_date = $business_date;

insert into ops.rejected_row
select
    $run_id, c._business_date, c._file_name, c._row_number, c.reject_reason,
    cast(json_object(
        'merchant_id', c.merchant_id, 'merchant_name', c.merchant_name, 'category', c.category,
        'country', c.country, 'city', c.city, 'settlement_currency', c.settlement_currency,
        'fee_plan_code', c.fee_plan_code, 'status', c.status, 'onboarded_on', c.onboarded_on
    ) as varchar)
from stg.merchants_checked c
where c.reject_reason is not null;

create or replace table stg.merchants as
select
    merchant_id_clean                       as merchant_id,
    trim(merchant_name)                     as merchant_name,
    coalesce(category, 'Unclassified')      as category,
    coalesce(country, 'ZZ')                 as country,
    coalesce(city, 'Unknown')               as city,
    settlement_currency,
    coalesce(fee_plan_code, 'UNKNOWN')      as fee_plan_code,
    status,
    onboarded_date                          as onboarded_on,
    -- Hash of the attributes tracked as type 2. The name is type 1 and is left out on purpose.
    md5(concat_ws('|', coalesce(category, 'Unclassified'), coalesce(country, 'ZZ'), coalesce(city, 'Unknown'),
                  settlement_currency, coalesce(fee_plan_code, 'UNKNOWN'), status)) as row_hash
from (
    select
        c.*,
        row_number() over (partition by c.merchant_id_clean order by c._row_number desc) as version_rank
    from stg.merchants_checked c
    where c.reject_reason is null
)
where version_rank = 1;
