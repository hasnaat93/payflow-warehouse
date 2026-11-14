# Implementation status

An honest record of what was built, what was run, and what was not.

## Verified locally (Windows 11, Python 3.14, DuckDB 2.0.0) on 2026-11-14

| Check | Result |
|---|---|
| `pytest` | 46 tests pass |
| `ruff check .` | Clean |
| `pfw demo` | 92 of 92 daily runs succeed on the default synthetic feed (seed 7) |
| Replay of a loaded date (`pfw run --date 2026-10-31`) | Succeeds; no fact rows change |
| Dashboard | Opened in Microsoft Edge at 1440 px and 390 px wide: no console errors, no horizontal overflow, currency switch and chart tooltip work |
| Exports | 11 mart views written as Parquet and CSV; one Parquet file read back in a test |

Figures from that demo run (they are deterministic for seed 7):

- 73,409 rows delivered = 73,291 loaded + 39 quarantined + 79 duplicates dropped
- 34,564 payments, 1,249 refunds, 1,182 settlement batches, 14 merchants in 20 versions
- 462 payments arrived one to three days late
- 22 quality checks per run; the only warning in the feed is the placeholder merchant, outstanding for two runs

## Continuous integration

The workflow runs lint, tests and the full `pfw demo` on Python 3.11 and 3.13 on Linux, then publishes the dashboard to GitHub Pages. The first run (2026-11-14) passed on both versions, and the published page at https://hasnaat93.github.io/payflow-warehouse/ returned the dashboard with 92 successful runs. See the Actions tab for the current result.

## Not verified

- The SQL has only been run on DuckDB. It has not been run on PostgreSQL, Greenplum or any cloud warehouse.
- The exports were not opened in Power BI, Tableau or Metabase.
- macOS was not tested.
- Nothing was measured for performance; the dataset is small.
- The dashboard was checked in one browser (Edge/Chromium). Keyboard-only use and screen readers were not tested.

## Known limitations

Listed in the [README](README.md#limitations).
