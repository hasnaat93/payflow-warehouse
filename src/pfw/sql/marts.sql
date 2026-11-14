-- BI-facing views. Point Power BI, Tableau or Metabase at these, or use `pfw export`
-- to write them out as Parquet and CSV.
--
-- Amounts are exposed in major units as exact decimals. Currencies are never
-- added together: every view that reports money is grouped by currency.

create or replace macro to_major(minor) as cast(minor * 0.01 as decimal(18, 2));

-- Daily KPIs per currency on a complete calendar spine (days with no activity
-- appear as zero rows, so rolling windows cover real calendar days).
create or replace view mart.daily_kpis as
with bounds as (
    select min(date_key) as first_key, max(date_key) as last_key from dw.agg_daily_merchant
),
spine as (
    select d.date_key, d.full_date, d.day_name, d.is_weekend, c.currency_code
    from dw.dim_date d
    cross join dw.dim_currency c
    cross join bounds b
    where d.date_key between b.first_key and b.last_key
),
daily as (
    select
        date_key, currency_code,
        sum(attempts) as attempts, sum(approved) as approved, sum(declined) as declined,
        sum(captured_count) as captured_count, sum(captured_minor) as captured_minor,
        sum(fee_minor) as fee_minor, sum(refunded_minor) as refunded_minor
    from dw.agg_daily_merchant
    group by date_key, currency_code
),
filled as (
    select
        s.full_date, s.day_name, s.is_weekend, s.currency_code,
        coalesce(a.attempts, 0)::bigint       as attempts,
        coalesce(a.approved, 0)::bigint       as approved,
        coalesce(a.declined, 0)::bigint       as declined,
        coalesce(a.captured_count, 0)::bigint as captured_count,
        coalesce(a.captured_minor, 0)::bigint as captured_minor,
        coalesce(a.fee_minor, 0)::bigint      as fee_minor,
        coalesce(a.refunded_minor, 0)::bigint as refunded_minor
    from spine s
    left join daily a on a.date_key = s.date_key and a.currency_code = s.currency_code
)
select
    full_date                                               as activity_date,
    currency_code,
    day_name,
    is_weekend,
    attempts,
    approved,
    declined,
    round(100.0 * approved / nullif(attempts, 0), 2)        as approval_rate_pct,
    captured_count,
    to_major(captured_minor)                                as captured_amount,
    to_major(fee_minor)                                     as fee_amount,
    to_major(refunded_minor)                                as refunded_amount,
    to_major(captured_minor - fee_minor - refunded_minor)   as net_amount,
    to_major(cast(round(captured_minor * 1.0 / nullif(captured_count, 0)) as bigint)) as avg_ticket,
    to_major(cast(round(avg(captured_minor) over seven_days) as bigint))              as captured_amount_7d_avg,
    round(100.0 * sum(approved) over seven_days / nullif(sum(attempts) over seven_days, 0), 2)
                                                            as approval_rate_7d_pct,
    to_major(captured_minor - lag(captured_minor, 7) over by_day) as captured_change_vs_prior_week
from filled
window
    by_day     as (partition by currency_code order by full_date),
    seven_days as (partition by currency_code order by full_date rows between 6 preceding and current row);

-- Month-level totals with month-over-month growth.
create or replace view mart.monthly_summary as
with monthly as (
    select
        d.month_start, a.currency_code,
        count(distinct a.date_key)  as active_days,
        sum(a.attempts)             as attempts,
        sum(a.approved)             as approved,
        sum(a.captured_count)       as captured_count,
        sum(a.captured_minor)       as captured_minor,
        sum(a.fee_minor)            as fee_minor,
        sum(a.refunded_minor)       as refunded_minor
    from dw.agg_daily_merchant a
    join dw.dim_date d on d.date_key = a.date_key
    group by d.month_start, a.currency_code
)
select
    month_start,
    currency_code,
    active_days,
    attempts,
    round(100.0 * approved / nullif(attempts, 0), 2)                as approval_rate_pct,
    captured_count,
    to_major(captured_minor)                                        as captured_amount,
    to_major(fee_minor)                                             as fee_amount,
    to_major(refunded_minor)                                        as refunded_amount,
    round(10000.0 * fee_minor / nullif(captured_minor, 0), 1)       as effective_fee_bps,
    round(100.0 * refunded_minor / nullif(captured_minor, 0), 2)    as refund_rate_pct,
    round(100.0 * (captured_minor - lag(captured_minor) over by_month)
          / nullif(lag(captured_minor) over by_month, 0), 2)        as captured_growth_pct
from monthly
window by_month as (partition by currency_code order by month_start);

-- One row per merchant with rank and share inside its currency.
create or replace view mart.merchant_scorecard as
with totals as (
    select
        merchant_id, currency_code,
        count(distinct date_key)    as active_days,
        min(date_key)               as first_date_key,
        max(date_key)               as last_date_key,
        sum(attempts)               as attempts,
        sum(approved)               as approved,
        sum(declined)               as declined,
        sum(captured_count)         as captured_count,
        sum(captured_minor)         as captured_minor,
        sum(fee_minor)              as fee_minor,
        sum(refunded_minor)         as refunded_minor
    from dw.agg_daily_merchant
    group by merchant_id, currency_code
)
select
    rank() over (partition by t.currency_code order by t.captured_minor desc)    as volume_rank,
    t.merchant_id,
    m.merchant_name,
    m.category,
    m.country,
    m.city,
    m.fee_plan_code,
    m.status                                                                    as merchant_status,
    t.currency_code,
    t.active_days,
    t.attempts,
    round(100.0 * t.approved / nullif(t.attempts, 0), 2)                        as approval_rate_pct,
    t.captured_count,
    to_major(t.captured_minor)                                                  as captured_amount,
    round(100.0 * t.captured_minor
          / nullif(sum(t.captured_minor) over (partition by t.currency_code), 0), 2) as share_of_currency_pct,
    to_major(cast(round(t.captured_minor * 1.0 / nullif(t.captured_count, 0)) as bigint)) as avg_ticket,
    to_major(t.fee_minor)                                                       as fee_amount,
    round(10000.0 * t.fee_minor / nullif(t.captured_minor, 0), 1)               as effective_fee_bps,
    to_major(t.refunded_minor)                                                  as refunded_amount,
    round(100.0 * t.refunded_minor / nullif(t.captured_minor, 0), 2)            as refund_rate_pct
from totals t
join dw.dim_merchant m on m.merchant_id = t.merchant_id and m.is_current;

-- Why payments are declined.
create or replace view mart.decline_reasons as
select
    currency_code,
    decline_reason,
    count(*)                                                                    as declines,
    round(100.0 * count(*) / sum(count(*)) over (partition by currency_code), 2) as share_of_declines_pct,
    to_major(sum(amount_minor))                                                 as declined_amount
from dw.fact_transaction
where status = 'DECLINED'
group by currency_code, decline_reason;

-- Approval rate and volume by card brand.
create or replace view mart.card_brand_mix as
select
    currency_code,
    card_brand,
    count(*)                                                                    as attempts,
    round(100.0 * count(*) filter (where status <> 'DECLINED') / count(*), 2)   as approval_rate_pct,
    to_major(sum(captured_minor))                                               as captured_amount,
    round(100.0 * sum(captured_minor)
          / nullif(sum(sum(captured_minor)) over (partition by currency_code), 0), 2) as share_of_currency_pct
from dw.fact_transaction
group by currency_code, card_brand;

-- How many days pass between capture and settlement.
create or replace view mart.settlement_lag as
select
    f.currency_code,
    date_diff('day', cd.full_date, sd.full_date)    as lag_days,
    count(*)                                        as payments,
    to_major(sum(f.captured_minor))                 as captured_amount
from dw.fact_transaction f
join dw.dim_date cd on cd.date_key = f.captured_date_key
join dw.dim_date sd on sd.date_key = f.settled_date_key
group by f.currency_code, lag_days;

-- Captured payments still waiting to be paid out, aged against the newest loaded date.
create or replace view mart.unsettled_aging as
with as_of as (
    select max(d.full_date) as as_of_date
    from dw.fact_transaction f
    join dw.dim_date d on d.date_key = f.first_seen_date_key
)
select
    f.merchant_id,
    m.merchant_name,
    f.currency_code,
    a.as_of_date,
    count(*)                                                                as payments,
    to_major(sum(f.captured_minor - f.fee_minor))                           as amount_awaiting_settlement,
    count(*) filter (where date_diff('day', cd.full_date, a.as_of_date) <= 1)             as age_0_1_days,
    count(*) filter (where date_diff('day', cd.full_date, a.as_of_date) between 2 and 3)  as age_2_3_days,
    count(*) filter (where date_diff('day', cd.full_date, a.as_of_date) > 3)              as age_over_3_days,
    max(date_diff('day', cd.full_date, a.as_of_date))                       as oldest_days
from dw.fact_transaction f
join dw.dim_date cd on cd.date_key = f.captured_date_key
join dw.dim_merchant m on m.merchant_id = f.merchant_id and m.is_current
cross join as_of a
where f.captured_minor > 0
  and f.settled_date_key is null
group by f.merchant_id, m.merchant_name, f.currency_code, a.as_of_date;

-- Each settlement batch against the payments the warehouse holds for it.
create or replace view mart.settlement_reconciliation as
with from_payments as (
    select
        settlement_batch_id         as batch_id,
        count(*)                    as txn_count,
        sum(captured_minor)         as gross_minor,
        sum(fee_minor)              as fee_minor
    from dw.fact_transaction
    where settlement_batch_id is not null
    group by settlement_batch_id
)
select
    s.batch_id,
    s.merchant_id,
    d.full_date                                         as settlement_date,
    s.currency_code,
    s.txn_count                                         as batch_txn_count,
    coalesce(p.txn_count, 0)                            as warehouse_txn_count,
    to_major(s.gross_minor)                             as batch_gross,
    to_major(coalesce(p.gross_minor, 0))                as warehouse_gross,
    to_major(s.fee_minor)                               as batch_fees,
    to_major(coalesce(p.fee_minor, 0))                  as warehouse_fees,
    to_major(s.refund_minor)                            as batch_refunds,
    to_major(s.net_minor)                               as batch_net,
    s.txn_count = coalesce(p.txn_count, 0)
        and s.gross_minor = coalesce(p.gross_minor, 0)
        and s.fee_minor = coalesce(p.fee_minor, 0)      as is_reconciled
from dw.fact_settlement s
join dw.dim_date d on d.date_key = s.settlement_date_key
left join from_payments p on p.batch_id = s.batch_id;

-- The history kept by the type 2 dimension, with what changed between versions.
create or replace view mart.merchant_history as
select
    merchant_id,
    merchant_name,
    row_number() over by_merchant                       as version,
    valid_from,
    valid_to,
    is_current,
    category,
    city,
    country,
    fee_plan_code,
    status,
    concat_ws(', ',
        case when category      <> lag(category)      over by_merchant then 'category' end,
        case when city          <> lag(city)          over by_merchant then 'city' end,
        case when country       <> lag(country)       over by_merchant then 'country' end,
        case when fee_plan_code <> lag(fee_plan_code) over by_merchant then 'fee plan' end,
        case when status        <> lag(status)        over by_merchant then 'status' end
    )                                                   as changed_attributes
from dw.dim_merchant
window by_merchant as (partition by merchant_id order by valid_from);

-- Payments delivered after the day they happened.
create or replace view mart.late_arrivals as
select
    sd.full_date                                        as delivered_on,
    date_diff('day', ad.full_date, sd.full_date)        as days_late,
    count(*)                                            as payments
from dw.fact_transaction f
join dw.dim_date ad on ad.date_key = f.authorized_date_key
join dw.dim_date sd on sd.date_key = f.first_seen_date_key
where f.first_seen_date_key > f.authorized_date_key
group by sd.full_date, days_late;

-- Per business date: what was delivered, loaded, quarantined and dropped.
create or replace view mart.load_accounting as
select
    business_date,
    sum(declared_rows)      as declared_rows,
    sum(raw_rows)           as raw_rows,
    sum(staged_rows)        as loaded_rows,
    sum(rejected_rows)      as rejected_rows,
    sum(duplicate_rows)     as duplicate_rows
from ops.file_load
group by business_date;
