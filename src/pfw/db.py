"""Database access: connecting, bootstrapping the schema and running SQL files."""

from __future__ import annotations

import re
from importlib import resources
from pathlib import Path
from typing import Any

import duckdb

_PARAM = re.compile(r"\$([a-z_][a-z0-9_]*)")
_DML = ("insert", "update", "delete", "merge")


def connect(path: Path | str) -> duckdb.DuckDBPyConnection:
    """Open the warehouse file (created on first use) with the schema in place."""
    if str(path) != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    init_schema(con)
    return con


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    run_script(con, "ddl.sql")
    run_script(con, "marts.sql")


def load_sql(name: str) -> str:
    return resources.files("pfw").joinpath("sql", name).read_text(encoding="utf-8")


def split_statements(sql: str) -> list[str]:
    """Split a SQL file into statements.

    Line comments are removed first so that a semicolon inside a comment does
    not end a statement. The project's SQL keeps semicolons and `--` out of
    string literals, which keeps this splitter trivial.
    """
    without_comments = "\n".join(line.split("--", 1)[0] for line in sql.splitlines())
    return [s.strip() for s in without_comments.split(";") if s.strip()]


def execute(con: duckdb.DuckDBPyConnection, statement: str, params: dict[str, Any] | None = None) -> int:
    """Run one statement, binding only the named parameters it uses.

    Returns the affected row count for INSERT/UPDATE/DELETE/MERGE, otherwise 0.
    """
    used = {name: (params or {})[name] for name in set(_PARAM.findall(statement))}
    result = con.execute(statement, used) if used else con.execute(statement)
    if statement.lstrip().lower().startswith(_DML):
        row = result.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    return 0


def run_script(con: duckdb.DuckDBPyConnection, name: str, params: dict[str, Any] | None = None) -> int:
    """Run every statement of a packaged SQL file. Returns the total rows written."""
    return sum(execute(con, statement, params) for statement in split_statements(load_sql(name)))


def scalar(con: duckdb.DuckDBPyConnection, sql: str, params: dict[str, Any] | None = None) -> Any:
    used = {name: (params or {})[name] for name in set(_PARAM.findall(sql))}
    row = (con.execute(sql, used) if used else con.execute(sql)).fetchone()
    return row[0] if row else None


def rows(con: duckdb.DuckDBPyConnection, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    used = {name: (params or {})[name] for name in set(_PARAM.findall(sql))}
    cursor = con.execute(sql, used) if used else con.execute(sql)
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, record, strict=True)) for record in cursor.fetchall()]
