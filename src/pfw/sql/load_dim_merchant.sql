-- Slowly changing dimension load for merchants, from the day's snapshot.
-- Must be applied in business-date order; the pipeline skips it for a date older
-- than the newest snapshot already applied.

-- 1. Type 1: a corrected name overwrites every version of the merchant.
update dw.dim_merchant as d
set merchant_name = s.merchant_name,
    updated_run_id = $run_id
from stg.merchants s
where d.merchant_id = s.merchant_id
  and d.merchant_name <> s.merchant_name;

-- 2. Complete inferred members: a placeholder created from a fact is filled in
--    in place, so facts already pointing at it keep their surrogate key.
update dw.dim_merchant as d
set category = s.category,
    country = s.country,
    city = s.city,
    settlement_currency = s.settlement_currency,
    fee_plan_code = s.fee_plan_code,
    status = s.status,
    onboarded_on = s.onboarded_on,
    row_hash = s.row_hash,
    is_inferred = false,
    updated_run_id = $run_id
from stg.merchants s
where d.merchant_id = s.merchant_id
  and d.is_inferred;

-- 3. Type 2: close the current version of every merchant whose tracked attributes changed.
update dw.dim_merchant as d
set valid_to = $business_date,
    is_current = false,
    updated_run_id = $run_id
from stg.merchants s
where d.merchant_id = s.merchant_id
  and d.is_current
  and d.row_hash <> s.row_hash;

-- 4. Insert a version for every merchant that now has no current row: brand-new
--    merchants and the ones closed in step 3. A merchant's first version is valid
--    from the beginning of time so that any fact date resolves to a version.
insert into dw.dim_merchant
select
    nextval('dw.merchant_sk_seq'),
    s.merchant_id, s.merchant_name, s.category, s.country, s.city,
    s.settlement_currency, s.fee_plan_code, s.status, s.onboarded_on,
    case when exists (select 1 from dw.dim_merchant p where p.merchant_id = s.merchant_id)
         then $business_date else date '1900-01-01' end,
    date '9999-12-31',
    true,
    false,
    s.row_hash,
    $run_id,
    $run_id
from stg.merchants s
where not exists (
    select 1 from dw.dim_merchant d where d.merchant_id = s.merchant_id and d.is_current
);
