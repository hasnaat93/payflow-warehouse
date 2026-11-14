"""Pure logic: fee arithmetic, formatting, SQL splitting, task ordering, determinism."""

from __future__ import annotations

import hashlib
from datetime import date

import pytest

from pfw import db, generator, pipeline
from pfw.pipeline import Outcome, PipelineError, Task


@pytest.mark.parametrize(
    ("amount", "plan", "expected"),
    [
        (4999, "STANDARD", 175),    # 2.90% of 49.99 = 1.44971 -> 1.45, + 0.30
        (10000, "STANDARD", 320),
        (1000, "PREMIUM", 50),      # 2.50% of 10.00 = 0.25, + 0.25
        (1, "STANDARD", 1),         # fee can never exceed the amount
        (500, "MICRO", 28),         # 3.50% of 5.00 = 0.175 -> 0.18 (half-up), + 0.10
        (220, "PREMIUM", 31),       # 2.50% of 2.20 = 0.055, an exact half -> 0.06, + 0.25
        (2586, "STANDARD", 105),    # 74.994 -> 75
        (1707, "STANDARD", 80),     # 49.503 -> 50
        (1706, "STANDARD", 79),     # 49.474 -> 49
    ],
)
def test_fee_is_rounded_half_up_and_capped(amount, plan, expected):
    assert generator.fee_for(amount, plan) == expected


@pytest.mark.parametrize(("minor", "text"), [(0, "0.00"), (7, "0.07"), (4999, "49.99"), (123456789, "1234567.89"), (-250, "-2.50")])
def test_format_minor(minor, text):
    assert generator.format_minor(minor) == text


def test_split_statements_ignores_semicolons_in_comments():
    sql = "-- a note; with a semicolon\nselect 1;\n\nselect 2 -- trailing; comment\n;\n"
    assert db.split_statements(sql) == ["select 1", "select 2"]


def test_execute_binds_only_the_parameters_a_statement_uses():
    con = db.connect(":memory:")
    assert db.scalar(con, "select $sha256 || '!'", {"sha256": "abc", "unused": 1}) == "abc!"
    con.close()


def _noop(_ctx):
    return Outcome()


def test_tasks_run_after_their_dependencies():
    tasks = [Task("c", _noop, ("a", "b")), Task("b", _noop, ("a",)), Task("a", _noop)]
    assert [t.name for t in pipeline.topological_order(tasks)] == ["a", "b", "c"]


def test_default_pipeline_is_a_valid_graph():
    order = [t.name for t in pipeline.topological_order(pipeline.default_tasks())]
    assert order[0] == "verify_manifest"
    assert order[-1] == "quality_gate"
    assert order.index("load_dim_merchant") < order.index("load_fact_transaction") < order.index("load_fact_refund")


def test_cycle_is_rejected():
    with pytest.raises(PipelineError, match="cycle"):
        pipeline.topological_order([Task("a", _noop, ("b",)), Task("b", _noop, ("a",))])


def test_unknown_dependency_is_rejected():
    with pytest.raises(PipelineError, match="unknown task"):
        pipeline.topological_order([Task("a", _noop, ("missing",))])


def _digest(folder) -> str:
    digest = hashlib.sha256()
    for path in sorted(folder.rglob("*")):
        if path.is_file():
            digest.update(path.relative_to(folder).as_posix().encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()


def test_generator_is_deterministic(tmp_path):
    generator.generate(tmp_path / "a", date(2026, 8, 1), 12, seed=3)
    generator.generate(tmp_path / "b", date(2026, 8, 1), 12, seed=3)
    generator.generate(tmp_path / "c", date(2026, 8, 1), 12, seed=4)
    assert _digest(tmp_path / "a") == _digest(tmp_path / "b")
    assert _digest(tmp_path / "a") != _digest(tmp_path / "c")
