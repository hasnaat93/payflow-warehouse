"""The daily batch: a small dependency graph of tasks run inside one transaction.

One run processes one business date. All warehouse changes for the date are
committed together or not at all: if any task fails, or a data quality check of
severity ERROR finds violations, the transaction is rolled back and the
warehouse stays exactly as the previous successful run left it. The run log,
task timings and check results are written afterwards in either case.

Re-running a date is safe. Raw rows for the date are replaced, facts are
upserted on their business keys, and a payment is only overwritten by a newer
source version.
"""

from __future__ import annotations

import csv
import hashlib
import json
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

from pfw import db, quality
from pfw.generator import FILES

# file name -> (raw table, staging table, checked table)
SOURCES = {
    "merchants.csv": ("raw.merchants", "stg.merchants", "stg.merchants_checked"),
    "transactions.csv": ("raw.transactions", "stg.transactions", "stg.transactions_checked"),
    "refunds.csv": ("raw.refunds", "stg.refunds", "stg.refunds_checked"),
    "settlements.csv": ("raw.settlements", "stg.settlements", "stg.settlements_checked"),
}


class PipelineError(RuntimeError):
    """A problem with the delivered files or the task graph."""


@dataclass
class Context:
    con: duckdb.DuckDBPyConnection
    business_date: date
    run_id: str
    folder: Path
    manifest: dict = field(default_factory=dict)
    checks: list[quality.CheckResult] = field(default_factory=list)

    @property
    def params(self) -> dict:
        return {"business_date": self.business_date, "run_id": self.run_id}


@dataclass(frozen=True)
class Outcome:
    rows: int | None = None
    detail: str = ""
    skipped: bool = False


@dataclass(frozen=True)
class Task:
    name: str
    run: Callable[[Context], Outcome]
    depends_on: tuple[str, ...] = ()


@dataclass
class TaskRecord:
    task: str
    status: str
    duration_ms: int
    rows: int | None = None
    detail: str = ""


@dataclass
class RunResult:
    run_id: str
    business_date: date
    status: str
    duration_ms: int
    tasks: list[TaskRecord]
    checks: list[quality.CheckResult]
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == "SUCCESS"

    @property
    def warnings(self) -> list[quality.CheckResult]:
        return [c for c in self.checks if c.status == "WARN"]


def topological_order(tasks: list[Task]) -> list[Task]:
    """Order tasks so each runs after its dependencies (stable: declaration order breaks ties)."""
    by_name = {t.name: t for t in tasks}
    if len(by_name) != len(tasks):
        raise PipelineError("Duplicate task name in the pipeline")
    for task in tasks:
        for dependency in task.depends_on:
            if dependency not in by_name:
                raise PipelineError(f"Task '{task.name}' depends on unknown task '{dependency}'")
    ordered: list[Task] = []
    done: set[str] = set()
    remaining = list(tasks)
    while remaining:
        ready = [t for t in remaining if all(d in done for d in t.depends_on)]
        if not ready:
            raise PipelineError("Dependency cycle between: " + ", ".join(t.name for t in remaining))
        ordered.append(ready[0])
        done.add(ready[0].name)
        remaining.remove(ready[0])
    return ordered


# --------------------------------------------------------------------- tasks

def verify_manifest(ctx: Context) -> Outcome:
    """Refuse to load anything unless the delivery is complete and intact."""
    manifest_path = ctx.folder / "manifest.json"
    if not manifest_path.is_file():
        raise PipelineError(f"No manifest for {ctx.business_date} at {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("business_date") != ctx.business_date.isoformat():
        raise PipelineError(
            f"Manifest is for {manifest.get('business_date')}, expected {ctx.business_date.isoformat()}"
        )
    for name, columns in FILES.items():
        declared = manifest.get("files", {}).get(name)
        path = ctx.folder / name
        if declared is None or not path.is_file():
            raise PipelineError(f"{name} is missing from the delivery for {ctx.business_date}")
        payload = path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != declared["sha256"]:
            raise PipelineError(f"{name}: checksum does not match the manifest (file changed or truncated)")
        header = next(csv.reader(payload.decode("utf-8").splitlines()), [])
        if header != columns:
            raise PipelineError(f"{name}: unexpected columns {header}; expected {columns}")
    ctx.manifest = manifest
    return Outcome(detail=f"{len(FILES)} files verified")


def load_raw(ctx: Context) -> Outcome:
    """Land the files unchanged. Replaces anything already landed for this date."""
    total = 0
    db.execute(ctx.con, "delete from ops.rejected_row where business_date = $business_date", ctx.params)
    db.execute(ctx.con, "delete from ops.file_load where business_date = $business_date", ctx.params)
    for name, columns in FILES.items():
        table = SOURCES[name][0]
        db.execute(ctx.con, f"delete from {table} where _business_date = $business_date", ctx.params)
        column_spec = ", ".join(f"'{c}': 'VARCHAR'" for c in columns)
        path = (ctx.folder / name).as_posix().replace("'", "''")
        loaded = db.execute(
            ctx.con,
            f"""insert into {table}
                select {", ".join(columns)}, $business_date, '{name}', row_number() over (), $run_id
                from read_csv('{path}', header = true, auto_detect = false, delim = ',', quote = '"',
                              columns = {{{column_spec}}})""",
            ctx.params,
        )
        declared = ctx.manifest["files"][name]["rows"]
        if loaded != declared:
            raise PipelineError(f"{name}: manifest declares {declared} rows but {loaded} were read")
        total += loaded
    return Outcome(rows=total)


def _script(name: str) -> Callable[[Context], Outcome]:
    def run(ctx: Context) -> Outcome:
        return Outcome(rows=db.run_script(ctx.con, name, ctx.params))
    return run


def load_dim_merchant(ctx: Context) -> Outcome:
    """Apply the merchant snapshot, unless a newer snapshot has already been applied."""
    newest = db.scalar(ctx.con, "select max(value_date) from ops.watermark where name = 'dim_merchant'")
    if newest is not None and ctx.business_date < newest:
        return Outcome(skipped=True, detail=f"snapshot older than the one applied for {newest}")
    written = db.run_script(ctx.con, "load_dim_merchant.sql", ctx.params)
    db.execute(ctx.con, "delete from ops.watermark where name = 'dim_merchant'")
    db.execute(ctx.con, "insert into ops.watermark values ('dim_merchant', $business_date)", ctx.params)
    return Outcome(rows=written)


def account_files(ctx: Context) -> Outcome:
    """Record, per file, where every delivered row went."""
    for name, (raw_table, staged_table, checked_table) in SOURCES.items():
        params = {**ctx.params, "file_name": name}
        raw_rows = db.scalar(ctx.con, f"select count(*) from {raw_table} where _business_date = $business_date", params)
        rejected = db.scalar(
            ctx.con,
            "select count(*) from ops.rejected_row where business_date = $business_date and file_name = $file_name",
            params,
        )
        orphans = db.scalar(
            ctx.con,
            """select count(*) from ops.rejected_row
               where business_date = $business_date and file_name = $file_name and reason = 'ORPHAN_REFUND'""",
            params,
        )
        valid = db.scalar(ctx.con, f"select count(*) from {checked_table} where reject_reason is null")
        distinct = db.scalar(ctx.con, f"select count(*) from {staged_table}")
        db.execute(
            ctx.con,
            """insert into ops.file_load values
               ($run_id, $business_date, $file_name, $sha256, $declared, $raw_rows, $staged, $rejected, $duplicates)""",
            {
                **params,
                "sha256": ctx.manifest["files"][name]["sha256"],
                "declared": ctx.manifest["files"][name]["rows"],
                "raw_rows": raw_rows,
                "staged": distinct - orphans,
                "rejected": rejected,
                "duplicates": valid - distinct,
            },
        )
    return Outcome(rows=len(SOURCES))


def quality_gate(ctx: Context) -> Outcome:
    ctx.checks = quality.run_checks(ctx.con, ctx.business_date)
    failures = [c for c in ctx.checks if c.status == "FAIL"]
    if failures:
        raise quality.QualityGateError(failures)
    warnings = sum(1 for c in ctx.checks if c.status == "WARN")
    return Outcome(rows=len(ctx.checks), detail=f"{len(ctx.checks)} checks, {warnings} warning(s)")


def default_tasks() -> list[Task]:
    return [
        Task("verify_manifest", verify_manifest),
        Task("load_raw", load_raw, ("verify_manifest",)),
        Task("stage_merchants", _script("stage_merchants.sql"), ("load_raw",)),
        Task("stage_transactions", _script("stage_transactions.sql"), ("load_raw",)),
        Task("stage_refunds", _script("stage_refunds.sql"), ("load_raw",)),
        Task("stage_settlements", _script("stage_settlements.sql"), ("load_raw",)),
        Task("load_dim_merchant", load_dim_merchant, ("stage_merchants",)),
        Task("load_inferred_merchants", _script("load_inferred_merchants.sql"),
             ("load_dim_merchant", "stage_transactions", "stage_settlements")),
        Task("load_fact_transaction", _script("load_fact_transaction.sql"), ("load_inferred_merchants",)),
        Task("load_fact_refund", _script("load_fact_refund.sql"), ("load_fact_transaction", "stage_refunds")),
        Task("load_fact_settlement", _script("load_fact_settlement.sql"), ("load_inferred_merchants",)),
        Task("refresh_agg_daily_merchant", _script("refresh_agg_daily_merchant.sql"), ("load_fact_refund",)),
        Task("account_files", account_files, ("load_fact_refund", "load_fact_settlement")),
        Task("quality_gate", quality_gate, ("refresh_agg_daily_merchant", "account_files")),
    ]


# -------------------------------------------------------------------- runner

def run_date(
    con: duckdb.DuckDBPyConnection,
    landing_dir: Path,
    business_date: date,
    tasks: list[Task] | None = None,
) -> RunResult:
    """Process one business date. Never raises for a failed run; inspect the result."""
    ordered = topological_order(tasks or default_tasks())
    started_at = datetime.now(UTC).replace(tzinfo=None)
    run_id = f"{business_date:%Y%m%d}-{started_at:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    ctx = Context(con, business_date, run_id, Path(landing_dir) / business_date.isoformat())
    records: list[TaskRecord] = []
    error: str | None = None
    clock = time.perf_counter()

    con.execute("begin transaction")
    try:
        for task in ordered:
            task_clock = time.perf_counter()
            try:
                outcome = task.run(ctx)
            except Exception as exc:
                elapsed = int((time.perf_counter() - task_clock) * 1000)
                records.append(TaskRecord(task.name, "FAILED", elapsed, detail=str(exc)[:500]))
                raise
            elapsed = int((time.perf_counter() - task_clock) * 1000)
            status = "SKIPPED" if outcome.skipped else "SUCCESS"
            records.append(TaskRecord(task.name, status, elapsed, outcome.rows, outcome.detail))
        con.execute("commit")
    except Exception as exc:
        con.execute("rollback")
        error = str(exc)[:1000]

    duration_ms = int((time.perf_counter() - clock) * 1000)
    result = RunResult(run_id, business_date, "FAILED" if error else "SUCCESS", duration_ms, records, ctx.checks, error)
    _write_run_log(con, result, started_at)
    return result


def _write_run_log(con: duckdb.DuckDBPyConnection, result: RunResult, started_at: datetime) -> None:
    finished_at = datetime.now(UTC).replace(tzinfo=None)
    con.execute(
        "insert into ops.pipeline_run values (?, ?, ?, ?, ?, ?, ?)",
        [result.run_id, result.business_date, result.status, started_at, finished_at, result.duration_ms, result.error],
    )
    if result.tasks:
        con.executemany(
            "insert into ops.task_run values (?, ?, ?, ?, ?, ?, ?)",
            [
                [result.run_id, seq, t.task, t.status, t.duration_ms, t.rows, t.detail or None]
                for seq, t in enumerate(result.tasks, start=1)
            ],
        )
    if result.checks:
        con.executemany(
            "insert into ops.dq_result values (?, ?, ?, ?, ?, ?, ?)",
            [
                [result.run_id, result.business_date, c.check.name, c.check.severity, c.status,
                 c.violations, c.check.description]
                for c in result.checks
            ],
        )


def landing_dates(landing_dir: Path) -> list[date]:
    """Business dates that have a delivery folder, oldest first."""
    found = []
    for child in Path(landing_dir).iterdir() if Path(landing_dir).is_dir() else []:
        try:
            found.append(date.fromisoformat(child.name))
        except ValueError:
            continue
    return sorted(found)


def backfill(
    con: duckdb.DuckDBPyConnection,
    landing_dir: Path,
    start: date | None = None,
    end: date | None = None,
    on_result: Callable[[RunResult], None] | None = None,
) -> list[RunResult]:
    """Run every delivered date in order, stopping at the first failure."""
    results = []
    for business_date in landing_dates(landing_dir):
        if (start and business_date < start) or (end and business_date > end):
            continue
        result = run_date(con, landing_dir, business_date)
        results.append(result)
        if on_result:
            on_result(result)
        if not result.ok:
            break
    return results
