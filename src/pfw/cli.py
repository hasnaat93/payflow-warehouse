"""Command line interface: `pfw <command>`."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

from pfw import __version__, db, generator, pipeline

DEFAULT_START = date(2026, 8, 1)
DEFAULT_DAYS = 92


def _default(name: str, fallback: str) -> Path:
    return Path(os.environ.get(name, fallback))


def _print_table(headers: list[str], records: list[list[object]]) -> None:
    cells = [[("" if v is None else str(v)) for v in record] for record in records]
    widths = [max(len(h), *(len(r[i]) for r in cells)) if cells else len(h) for i, h in enumerate(headers)]
    print("  ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True)))
    print("  ".join("-" * w for w in widths))
    for record in cells:
        print("  ".join(v.ljust(w) for v, w in zip(record, widths, strict=True)))


def _report_run(result: pipeline.RunResult, verbose: bool = False) -> None:
    loaded = next((t.rows for t in result.tasks if t.task == "load_raw"), None)
    warnings = len(result.warnings)
    line = f"{result.business_date}  {result.status:<7}  {result.duration_ms:>5} ms  rows landed: {loaded if loaded is not None else '-'}"
    if warnings:
        line += f"  warnings: {warnings}"
    print(line)
    if verbose or not result.ok:
        for task in result.tasks:
            rows = "" if task.rows is None else f"{task.rows} rows"
            print(f"    {task.status:<7}  {task.task:<28} {task.duration_ms:>5} ms  {rows}  {task.detail}".rstrip())
        for check in result.checks:
            if check.status != "PASS":
                print(f"    {check.status:<7}  {check.check.name}: {check.violations} violation(s)")
    if result.error:
        print(f"    error: {result.error}")


def cmd_generate(args: argparse.Namespace) -> int:
    summary = generator.generate(args.landing, args.start, args.days, args.seed)
    print(f"Wrote {summary.days} days of synthetic extracts to {summary.out_dir} (seed {summary.seed})")
    for name, count in summary.rows.items():
        print(f"  {name:<18} {count:>8} rows")
    return 0


def cmd_init(args: argparse.Namespace) -> int:
    db.connect(args.db).close()
    print(f"Warehouse ready at {args.db}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    con = db.connect(args.db)
    try:
        result = pipeline.run_date(con, args.landing, args.date)
    finally:
        con.close()
    _report_run(result, verbose=True)
    return 0 if result.ok else 1


def cmd_backfill(args: argparse.Namespace) -> int:
    con = db.connect(args.db)
    try:
        results = pipeline.backfill(
            con, args.landing, args.start, args.end,
            on_result=lambda r: _report_run(r, verbose=args.verbose),
        )
    finally:
        con.close()
    if not results:
        print(f"No deliveries found in {args.landing}")
        return 1
    failed = [r for r in results if not r.ok]
    print(f"\n{len(results) - len(failed)} of {len(results)} runs succeeded")
    return 1 if failed else 0


def cmd_status(args: argparse.Namespace) -> int:
    con = db.connect(args.db)
    try:
        print("Warehouse contents")
        _print_table(["table", "rows"], [
            [t, db.scalar(con, f"select count(*) from {t}")]
            for t in ("dw.dim_merchant", "dw.fact_transaction", "dw.fact_refund", "dw.fact_settlement",
                      "dw.agg_daily_merchant", "ops.rejected_row")
        ])
        print("\nLatest runs")
        runs = db.rows(con, """
            select business_date, status, duration_ms, coalesce(error, '') as error
            from ops.pipeline_run order by started_at desc, run_id desc limit 10""")
        _print_table(["business_date", "status", "ms", "error"], [list(r.values()) for r in runs])
        print("\nQuarantined rows by reason")
        reasons = db.rows(con, """
            select file_name, reason, count(*) as row_count from ops.rejected_row
            group by all order by file_name, row_count desc""")
        _print_table(["file", "reason", "rows"], [list(r.values()) for r in reasons])
        print("\nChecks that did not pass in the latest successful run")
        checks = db.rows(con, """
            select check_name, status, violations from ops.dq_result
            where run_id = (select run_id from ops.pipeline_run where status = 'SUCCESS'
                            order by business_date desc, started_at desc limit 1)
              and status <> 'PASS'""")
        _print_table(["check", "status", "violations"], [list(r.values()) for r in checks])
    finally:
        con.close()
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    from pfw import exports

    con = db.connect(args.db)
    try:
        written = exports.export_marts(con, args.out)
    finally:
        con.close()
    print(f"Exported {len(written)} views to {args.out} as Parquet and CSV")
    return 0


def cmd_dashboard(args: argparse.Namespace) -> int:
    from pfw import dashboard

    con = db.connect(args.db)
    try:
        path = dashboard.build(con, args.out)
    finally:
        con.close()
    print(f"Dashboard written to {path}")
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    """Generate data, load every day, then build the dashboard and exports."""
    if Path(args.db).exists():
        Path(args.db).unlink()
    args.start, args.days, args.seed = DEFAULT_START, DEFAULT_DAYS, 7
    cmd_generate(args)
    print()
    args.start = args.end = None
    args.verbose = False
    code = cmd_backfill(args)
    if code:
        return code
    print()
    args.out = args.site
    cmd_dashboard(args)
    args.out = args.exports
    cmd_export(args)
    return 0


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", type=Path, default=_default("PFW_DB", "warehouse/payflow.duckdb"),
                        help="warehouse file (env PFW_DB)")
    common.add_argument("--landing", type=Path, default=_default("PFW_LANDING", "data/landing"),
                        help="folder of daily deliveries (env PFW_LANDING)")

    parser = argparse.ArgumentParser(prog="pfw", description=__doc__)
    parser.add_argument("--version", action="version", version=f"pfw {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("generate", parents=[common], help="write synthetic daily extracts")
    p.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START)
    p.add_argument("--days", type=int, default=DEFAULT_DAYS)
    p.add_argument("--seed", type=int, default=7)
    p.set_defaults(func=cmd_generate)

    p = sub.add_parser("init", parents=[common], help="create the warehouse schema")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("run", parents=[common], help="process one business date")
    p.add_argument("--date", type=date.fromisoformat, required=True)
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("backfill", parents=[common], help="process every delivered date in order")
    p.add_argument("--from", dest="start", type=date.fromisoformat)
    p.add_argument("--to", dest="end", type=date.fromisoformat)
    p.add_argument("--verbose", action="store_true", help="show every task")
    p.set_defaults(func=cmd_backfill)

    p = sub.add_parser("status", parents=[common], help="row counts, recent runs, quarantine, warnings")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("export", parents=[common], help="write the mart views as Parquet and CSV")
    p.add_argument("--out", type=Path, default=Path("exports"))
    p.set_defaults(func=cmd_export)

    p = sub.add_parser("dashboard", parents=[common], help="build the static HTML dashboard")
    p.add_argument("--out", type=Path, default=Path("site/index.html"))
    p.set_defaults(func=cmd_dashboard)

    p = sub.add_parser("demo", parents=[common], help="generate, load everything, build dashboard and exports")
    p.add_argument("--site", type=Path, default=Path("site/index.html"))
    p.add_argument("--exports", type=Path, default=Path("exports"))
    p.set_defaults(func=cmd_demo)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
