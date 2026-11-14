-- Upsert the day's payments into the accumulating snapshot fact.
--
-- * Keyed on txn_id, so re-running a date changes nothing (idempotent).
-- * A row is only overwritten by a strictly newer source version, so dates can be
--   re-processed or backfilled out of order without moving a payment backwards.
-- * The merchant key is the dimension version in force on the authorization date,
--   not the version current at load time.

merge into dw.fact_transaction as f
using (
    select
        s.*,
        d.merchant_sk,
        cast(strftime(s.authorized_ts, '%Y%m%d') as integer) as authorized_date_key,
        cast(strftime(s.captured_ts,   '%Y%m%d') as integer) as captured_date_key,
        cast(strftime(s.settled_ts,    '%Y%m%d') as integer) as settled_date_key,
        cast(strftime(s.business_date, '%Y%m%d') as integer) as business_date_key,
        case when s.captured_ts is not null then s.amount_minor else 0 end as captured_minor
    from stg.transactions s
    join dw.dim_merchant d
      on d.merchant_id = s.merchant_id
     and cast(s.authorized_ts as date) >= d.valid_from
     and cast(s.authorized_ts as date) <  d.valid_to
) as s
on f.txn_id = s.txn_id
when matched and s.updated_ts > f.source_updated_at then update set
    status              = s.status,
    decline_reason      = s.decline_reason,
    captured_date_key   = s.captured_date_key,
    settled_date_key    = s.settled_date_key,
    captured_at         = s.captured_ts,
    settled_at          = s.settled_ts,
    captured_minor      = s.captured_minor,
    fee_minor           = s.fee_minor,
    settlement_batch_id = s.settlement_batch_id,
    source_updated_at   = s.updated_ts,
    updated_run_id      = $run_id
when not matched then insert (
    txn_id, merchant_sk, merchant_id, currency_code, card_brand, status, decline_reason,
    authorized_date_key, captured_date_key, settled_date_key, first_seen_date_key,
    authorized_at, captured_at, settled_at,
    amount_minor, captured_minor, fee_minor, refunded_minor,
    settlement_batch_id, source_updated_at, inserted_run_id, updated_run_id
) values (
    s.txn_id, s.merchant_sk, s.merchant_id, s.currency_code, s.card_brand, s.status, s.decline_reason,
    s.authorized_date_key, s.captured_date_key, s.settled_date_key, s.business_date_key,
    s.authorized_ts, s.captured_ts, s.settled_ts,
    s.amount_minor, s.captured_minor, s.fee_minor, 0,
    s.settlement_batch_id, s.updated_ts, $run_id, $run_id
);
