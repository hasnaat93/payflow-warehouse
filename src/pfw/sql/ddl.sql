-- Warehouse schema. Every statement is idempotent, so `pfw init` is safe to repeat.
--
-- Layers
--   raw   exact copy of the delivered files, every column text, plus load metadata
--   stg   typed, validated, de-duplicated rows for the business date being processed
--   dw    the dimensional model (star schema)
--   mart  views shaped for BI tools
--   ops   pipeline run log, file accounting, quarantine and data quality results
--
-- Fact tables carry no enforced keys, as in most analytical databases
-- (Greenplum, Redshift, Snowflake). Uniqueness and referential integrity are
-- asserted by the data quality checks that gate every run.

create schema if not exists raw;
create schema if not exists stg;
create schema if not exists dw;
create schema if not exists mart;
create schema if not exists ops;

-- ---------------------------------------------------------------- ops

create table if not exists ops.pipeline_run (
    run_id          varchar   not null,
    business_date   date      not null,
    status          varchar   not null,      -- SUCCESS | FAILED
    started_at      timestamp not null,
    finished_at     timestamp not null,
    duration_ms     bigint    not null,
    error           varchar
);

create table if not exists ops.task_run (
    run_id          varchar   not null,
    seq             integer   not null,
    task            varchar   not null,
    status          varchar   not null,      -- SUCCESS | FAILED | SKIPPED
    duration_ms     bigint    not null,
    rows_affected   bigint,
    detail          varchar
);

-- One row per delivered file for the latest successful load of a business date.
create table if not exists ops.file_load (
    run_id          varchar   not null,
    business_date   date      not null,
    file_name       varchar   not null,
    sha256          varchar   not null,
    declared_rows   bigint    not null,      -- from the manifest
    raw_rows        bigint    not null,      -- landed in raw
    staged_rows     bigint    not null,      -- passed validation, after de-duplication
    rejected_rows   bigint    not null,      -- quarantined
    duplicate_rows  bigint    not null       -- dropped as duplicates or superseded versions
);

-- Quarantine. Rejected rows are kept verbatim with the reason, never dropped silently.
create table if not exists ops.rejected_row (
    run_id          varchar   not null,
    business_date   date      not null,
    file_name       varchar   not null,
    row_number      bigint    not null,
    reason          varchar   not null,
    record          varchar   not null       -- the source row as JSON
);

create table if not exists ops.dq_result (
    run_id          varchar   not null,
    business_date   date      not null,
    check_name      varchar   not null,
    severity        varchar   not null,      -- ERROR | WARN
    status          varchar   not null,      -- PASS | WARN | FAIL
    violations      bigint    not null,
    description     varchar   not null
);

create table if not exists ops.watermark (
    name            varchar   not null,
    value_date      date      not null
);

-- ---------------------------------------------------------------- raw

create table if not exists raw.transactions (
    txn_id varchar, merchant_id varchar, amount varchar, currency varchar, card_brand varchar,
    status varchar, decline_reason varchar, authorized_at varchar, captured_at varchar,
    fee_amount varchar, settled_at varchar, settlement_batch_id varchar, updated_at varchar,
    _business_date date, _file_name varchar, _row_number bigint, _run_id varchar
);

create table if not exists raw.refunds (
    refund_id varchar, txn_id varchar, amount varchar, currency varchar, refunded_at varchar, reason varchar,
    _business_date date, _file_name varchar, _row_number bigint, _run_id varchar
);

create table if not exists raw.merchants (
    merchant_id varchar, merchant_name varchar, category varchar, country varchar, city varchar,
    settlement_currency varchar, fee_plan_code varchar, status varchar, onboarded_on varchar,
    _business_date date, _file_name varchar, _row_number bigint, _run_id varchar
);

create table if not exists raw.settlements (
    batch_id varchar, merchant_id varchar, settlement_date varchar, currency varchar,
    gross_amount varchar, fee_amount varchar, refund_amount varchar, net_amount varchar, txn_count varchar,
    _business_date date, _file_name varchar, _row_number bigint, _run_id varchar
);

-- ---------------------------------------------------------------- dw: dimensions

create table if not exists dw.dim_date (
    date_key        integer   not null,      -- yyyymmdd
    full_date       date      not null,
    year            integer   not null,
    quarter         integer   not null,
    month           integer   not null,
    month_name      varchar   not null,
    month_start     date      not null,
    iso_week        integer   not null,
    day_of_week     integer   not null,      -- 1 = Monday ... 7 = Sunday
    day_name        varchar   not null,
    is_weekend      boolean   not null
);

insert into dw.dim_date
select
    cast(strftime(d, '%Y%m%d') as integer),
    d,
    year(d),
    quarter(d),
    month(d),
    monthname(d),
    date_trunc('month', d)::date,
    weekofyear(d),
    isodow(d),
    dayname(d),
    isodow(d) >= 6
from (select cast(unnest(generate_series(date '2025-01-01', date '2028-12-31', interval 1 day)) as date) as d)
where not exists (select 1 from dw.dim_date);

create table if not exists dw.dim_currency (
    currency_code   varchar   not null,
    currency_name   varchar   not null,
    minor_units     integer   not null
);

insert into dw.dim_currency
select * from (values
    ('USD', 'US Dollar', 2),
    ('EUR', 'Euro', 2),
    ('PKR', 'Pakistani Rupee', 2)
) as v(currency_code, currency_name, minor_units)
where not exists (select 1 from dw.dim_currency);

create sequence if not exists dw.merchant_sk_seq start 1;

-- Slowly changing dimension, type 2. One row per version of a merchant.
-- valid_from is inclusive, valid_to is exclusive; the open version ends 9999-12-31.
-- merchant_name is type 1 (corrections overwrite every version).
create table if not exists dw.dim_merchant (
    merchant_sk         bigint    not null,
    merchant_id         varchar   not null,  -- business key
    merchant_name       varchar   not null,
    category            varchar   not null,
    country             varchar   not null,
    city                varchar   not null,
    settlement_currency varchar   not null,
    fee_plan_code       varchar   not null,
    status              varchar   not null,
    onboarded_on        date,
    valid_from          date      not null,
    valid_to            date      not null,
    is_current          boolean   not null,
    is_inferred         boolean   not null,  -- created from a fact before the merchant extract arrived
    row_hash            varchar   not null,
    inserted_run_id     varchar   not null,
    updated_run_id      varchar   not null
);

-- ---------------------------------------------------------------- dw: facts

-- Grain: one row per payment. An accumulating snapshot: the row is updated as
-- the payment is captured, refunded and settled.
create table if not exists dw.fact_transaction (
    txn_id              varchar   not null,  -- degenerate dimension, business key
    merchant_sk         bigint    not null,  -- merchant version at authorization
    merchant_id         varchar   not null,
    currency_code       varchar   not null,
    card_brand          varchar   not null,
    status              varchar   not null,
    decline_reason      varchar,
    authorized_date_key integer   not null,
    captured_date_key   integer,
    settled_date_key    integer,
    first_seen_date_key integer   not null,  -- business date of the file that first delivered it
    authorized_at       timestamp not null,
    captured_at         timestamp,
    settled_at          timestamp,
    amount_minor        bigint    not null,  -- authorized amount
    captured_minor      bigint    not null,  -- 0 until captured
    fee_minor           bigint    not null,
    refunded_minor      bigint    not null,
    settlement_batch_id varchar,
    source_updated_at   timestamp not null,
    inserted_run_id     varchar   not null,
    updated_run_id      varchar   not null
);

-- Grain: one row per refund.
create table if not exists dw.fact_refund (
    refund_id           varchar   not null,
    txn_id              varchar   not null,
    merchant_sk         bigint    not null,
    merchant_id         varchar   not null,
    currency_code       varchar   not null,
    refund_date_key     integer   not null,
    refunded_at         timestamp not null,
    amount_minor        bigint    not null,
    reason              varchar,
    inserted_run_id     varchar   not null
);

-- Grain: one row per settlement batch (one merchant payout).
create table if not exists dw.fact_settlement (
    batch_id            varchar   not null,
    merchant_sk         bigint    not null,
    merchant_id         varchar   not null,
    currency_code       varchar   not null,
    settlement_date_key integer   not null,
    gross_minor         bigint    not null,
    fee_minor           bigint    not null,
    refund_minor        bigint    not null,
    net_minor           bigint    not null,
    txn_count           bigint    not null,
    inserted_run_id     varchar   not null
);

-- Aggregate maintained incrementally: only the dates touched by a run are rebuilt.
-- Grain: authorization date x merchant x currency.
create table if not exists dw.agg_daily_merchant (
    date_key            integer   not null,
    merchant_id         varchar   not null,
    currency_code       varchar   not null,
    attempts            bigint    not null,
    approved            bigint    not null,
    declined            bigint    not null,
    captured_count      bigint    not null,
    captured_minor      bigint    not null,
    fee_minor           bigint    not null,
    refunded_minor      bigint    not null
);
