# Design notes

Why the pipeline is built the way it is.

## One transaction per business date

All warehouse changes for a date commit together or not at all. The quality gate is the last task inside that transaction, so a load that breaks an invariant is rolled back instead of being published and cleaned up afterwards. Consumers never see a half-loaded day.

The run log, task timings and check results are written after the commit or rollback. A failed run therefore leaves a record of what went wrong, but no data.

## Replays are safe by construction

Operations teams re-run jobs: after a fix, after a late file, by mistake. Each step is written so that a replay converges to the same state.

| Step | How |
|---|---|
| Landing | Raw rows for the date are deleted and reloaded |
| Quarantine and file accounting | Replaced for the date |
| Payments | `MERGE` on `txn_id`; an existing row is updated only by a strictly newer `source_updated_at` |
| Refunds, settlements | Insert if the key is absent |
| Merchant dimension | Comparing a snapshot with itself finds no change |
| Aggregate | Rebuilt for the touched dates |

The "strictly newer" rule also makes out-of-order replays safe: replaying an old file that still shows a payment as `AUTHORIZED` cannot undo a later capture.

## Constraints are checked, not declared

The fact tables have no primary or foreign key constraints. That matches how analytical databases are commonly run (Greenplum, Redshift and Snowflake either do not enforce them or make them expensive), and it means the pipeline has to prove integrity itself. The quality gate does that on every run, and a test injects a duplicate payment to show the gate catches it and rolls back.

## Late-arriving data

**Facts.** A payment delivered three days late is dated by its authorization date, not its delivery date, and joins to the merchant version valid on that date. `first_seen_date_key` records when it actually arrived, so lateness is measurable (`mart.late_arrivals`). The aggregate for the older date is rebuilt.

**Dimensions.** If a payment references a merchant that the merchant file has not delivered yet, the pipeline inserts a placeholder member (`is_inferred = true`) and loads the payment against it. When the merchant arrives the placeholder is completed in place, keeping its surrogate key, so no fact needs to be re-pointed. A warning check reports placeholders that are still outstanding.

**Cross-file timing.** A payment's status and its refunds arrive in different files. If a payment says it is refunded but the refund is in quarantine, that is reported as a warning, not an error: it resolves when the refund's date is replayed.

## Money

Amounts are parsed with a pattern (digits, optionally a point and one or two digits) and converted directly to integer minor units. There is no floating point step and no rounding of input: `7.255` is rejected as invalid. Marts convert back to exact decimals for display. Every view that reports money is grouped by currency.

## SQL in files, Python for control flow

Transformations live in `src/pfw/sql/*.sql` and can be read and reviewed as SQL. Python decides order, binds the business date and run id, counts rows and handles failure. This keeps the logic portable: moving to another warehouse means adapting SQL dialect details, not rewriting application code.

## Why DuckDB

It is an embedded columnar database: the whole project runs from `pip install` with no server, and CI can execute the full 92-day pipeline on every push. Its SQL (window functions, `MERGE`, CTEs, `FILTER`) is close to PostgreSQL's. The trade-off is that it is single-writer and file-based, so this is a model of the technique, not of a multi-user production warehouse.

## What a production version would add

- A real scheduler with retries, alerting and SLAs
- Partitioned raw storage and retention rules
- Schema evolution handling for new source columns
- Row-level quarantine for structurally broken CSV lines
- Out-of-order merchant snapshots merged into history
- Access control on the marts
