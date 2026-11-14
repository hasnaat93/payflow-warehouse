"""Write the mart views to files that BI tools can open directly."""

from __future__ import annotations

from pathlib import Path

import duckdb

from pfw import db


def mart_views(con: duckdb.DuckDBPyConnection) -> list[str]:
    found = db.rows(con, "select table_name from information_schema.views where table_schema = 'mart' order by 1")
    return [r["table_name"] for r in found]


def export_marts(con: duckdb.DuckDBPyConnection, out_dir: Path) -> list[str]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    views = mart_views(con)
    for view in views:
        for extension, options in (("parquet", "format parquet"), ("csv", "format csv, header true")):
            target = (out_dir / f"{view}.{extension}").as_posix().replace("'", "''")
            con.execute(f"copy (select * from mart.{view}) to '{target}' ({options})")
    return views
