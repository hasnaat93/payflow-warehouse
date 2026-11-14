-- Late-arriving dimension: a payment or settlement references a merchant that the
-- merchant extract has not delivered yet. Rather than drop or hold the fact, a
-- placeholder member is created now and completed when the merchant arrives.

insert into dw.dim_merchant
select
    nextval('dw.merchant_sk_seq'),
    m.merchant_id,
    'Unknown merchant (inferred)',
    'Unclassified', 'ZZ', 'Unknown',
    m.currency_code,
    'UNKNOWN', 'ACTIVE', null,
    date '1900-01-01', date '9999-12-31',
    true, true,
    'inferred',
    $run_id, $run_id
from (
    select merchant_id, min(currency_code) as currency_code
    from (
        select merchant_id, currency_code from stg.transactions
        union all
        select merchant_id, currency_code from stg.settlements
    )
    group by merchant_id
) m
where not exists (select 1 from dw.dim_merchant d where d.merchant_id = m.merchant_id);
