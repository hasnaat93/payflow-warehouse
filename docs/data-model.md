# Data model

The schema is defined in [`src/pfw/sql/ddl.sql`](../src/pfw/sql/ddl.sql). The mart views are in [`src/pfw/sql/marts.sql`](../src/pfw/sql/marts.sql).

## Source: the daily delivery

One folder per business date, `data/landing/YYYY-MM-DD/`:

| File | Content | Shape |
|---|---|---|
| `transactions.csv` | Every payment that was created or changed that day, in its current state | Change feed: a payment appears again when it is captured, refunded or settled |
| `refunds.csv` | Refunds issued that day | Insert-only |
| `merchants.csv` | All merchants as of that day | Full snapshot |
| `settlements.csv` | Payout batches issued that day | Insert-only |
| `manifest.json` | Row count and SHA-256 checksum of each file | Control file |

Amounts are decimal text (`49.99`). Timestamps are UTC (`2026-08-01T10:15:30Z`).

## Layers

| Schema | Purpose | Lifetime |
|---|---|---|
| `raw` | The files exactly as delivered: every column text, plus business date, file name and row number | Kept; replaced per business date on replay |
| `stg` | The current run's rows: typed, validated, de-duplicated | Rebuilt every run |
| `dw` | Dimensions, facts and one aggregate | Permanent |
| `mart` | Views shaped for BI | Views |
| `ops` | Run log, per-file accounting, quarantine, check results | Permanent |

## Dimensions

### `dw.dim_merchant` (slowly changing, type 2)

One row per version of a merchant.

- `merchant_sk`: surrogate key, from a sequence. Facts store this.
- `merchant_id`: the business key from the source.
- **Type 2 attributes** (a change opens a new version): `category`, `country`, `city`, `settlement_currency`, `fee_plan_code`, `status`.
- **Type 1 attribute** (a change overwrites every version): `merchant_name`. A name change is treated as a correction, not history.
- `valid_from` (inclusive) and `valid_to` (exclusive). The open version ends `9999-12-31`. A merchant's first version starts `1900-01-01`, so any fact date resolves to a version.
- `is_current`: exactly one row per merchant.
- `is_inferred`: a placeholder created because a fact arrived before the merchant did.
- `row_hash`: MD5 of the type 2 attributes, used to detect change.

### `dw.dim_date`

One row per calendar day, 2025 to 2028. Key is `yyyymmdd`. Carries year, quarter, month, ISO week, day of week and a weekend flag.

### `dw.dim_currency`

USD, EUR, PKR. Also the list of currencies the pipeline accepts.

## Facts

### `dw.fact_transaction` — grain: one payment

An **accumulating snapshot**. The row is inserted when the payment is first delivered and updated as it moves through its life, so one row answers "how long from capture to settlement?" without self-joins.

| Column | Meaning |
|---|---|
| `txn_id` | Business key (degenerate dimension) |
| `merchant_sk` | The merchant version in force on the authorization date |
| `authorized_date_key`, `captured_date_key`, `settled_date_key` | Milestone dates; null until reached |
| `first_seen_date_key` | Business date of the file that first delivered the payment. Later than the authorization date for late arrivals |
| `amount_minor` | Authorized amount, integer minor units |
| `captured_minor` | Equal to `amount_minor` once captured, otherwise 0 |
| `fee_minor` | Processing fee assessed at capture |
| `refunded_minor` | Rolled up from `fact_refund` |
| `status` | `AUTHORIZED`, `DECLINED`, `CAPTURED`, `REVERSED`, `PARTIALLY_REFUNDED`, `REFUNDED` |
| `source_updated_at` | Version timestamp from the source; guards the merge |

### `dw.fact_refund` — grain: one refund

Linked to its payment by `txn_id`. Carries the payment's merchant key.

### `dw.fact_settlement` — grain: one payout batch

Gross, fees, refunds, net and payment count as stated by the source. `merchant_sk` is the version in force on the settlement date. `mart.settlement_reconciliation` compares each batch with the payments the warehouse holds for it.

### `dw.agg_daily_merchant` — grain: authorization date × merchant × currency

Attempts, approvals, declines, captured volume, fees and refunds. Maintained incrementally: each run rebuilds only the dates it touched. A quality check proves it equals the fact table for every date.

## Operational tables

| Table | Content |
|---|---|
| `ops.pipeline_run` | One row per run: business date, status, duration, error |
| `ops.task_run` | One row per task per run: status, duration, rows written |
| `ops.file_load` | Per file for each loaded date: declared, landed, loaded, quarantined and duplicate row counts |
| `ops.rejected_row` | Quarantined source rows as JSON, with file, row number and reason |
| `ops.dq_result` | Result of every check for every run |
| `ops.watermark` | Newest merchant snapshot applied |

## Rejection reasons

| Reason | Rule |
|---|---|
| `MISSING_TXN_ID`, `MISSING_MERCHANT_ID`, `MISSING_REFUND_ID`, `MISSING_BATCH_ID`, `MISSING_MERCHANT_NAME` | Key or required field blank |
| `INVALID_AMOUNT` | Not a plain non-negative decimal with at most two places, or not greater than zero |
| `UNKNOWN_CURRENCY` | Not in `dim_currency` |
| `INVALID_STATUS` | Not a known status |
| `INVALID_TIMESTAMP`, `INVALID_DATE` | Not in the agreed format |
| `INVALID_FEE`, `INVALID_COUNT` | Present but not parseable |
| `INCONSISTENT_STATE` | For example captured without a capture time, or settled without being captured |
| `ORPHAN_REFUND` | The refund's payment is not in the warehouse |

## Quality checks

Defined in [`src/pfw/quality.py`](../src/pfw/quality.py). Error-level checks fail the run and roll it back.

| Group | Checks |
|---|---|
| Keys | Unique `txn_id`, `refund_id`, `batch_id`, `merchant_sk` |
| Dimension integrity | One current version per merchant; versions contiguous with no overlap or gap |
| Referential integrity | Payment → merchant version valid on its date; payment → dates; refund → payment; batch → merchant |
| Business rules | Amount ranges; status agrees with capture and settlement columns; refund totals roll up; `net = gross − fees − refunds` |
| Reconciliation | Each batch equals its payments; aggregate equals fact; file row accounting balances |
| Warnings | Reject rate under 5%; payments delivered today; refund status agrees with loaded refunds; no placeholder merchants outstanding; no payment unsettled for more than 5 days |
