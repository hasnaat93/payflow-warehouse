from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from pfw import db, pipeline
from pfw.generator import Simulator, write_day

START = date(2026, 8, 1)
DAYS = 45


def ts(day: date, clock: str = "10:00:00") -> str:
    return f"{day.isoformat()}T{clock}Z"


def txn(txn_id: str, day: date, *, merchant: str = "M-1", amount: str = "10.00", currency: str = "USD",
        status: str = "AUTHORIZED", brand: str = "VISA", decline_reason: str = "", captured: str = "",
        fee: str = "", settled: str = "", batch: str = "", updated: str | None = None,
        authorized: str | None = None) -> list[str]:
    authorized = authorized or ts(day)
    return [txn_id, merchant, amount, currency, brand, status, decline_reason, authorized, captured, fee,
            settled, batch, updated or captured or authorized]


def merchant(merchant_id: str = "M-1", name: str = "Alpha Store", *, category: str = "Retail", country: str = "US",
             city: str = "Boston", currency: str = "USD", plan: str = "STANDARD", status: str = "ACTIVE") -> list[str]:
    return [merchant_id, name, category, country, city, currency, plan, status, "2026-01-01"]


class Lab:
    """A scratch landing folder and warehouse for hand-built scenarios."""

    def __init__(self, root: Path) -> None:
        self.landing = root / "landing"
        self.con = db.connect(root / "warehouse.duckdb")

    def deliver(self, day: date, *, transactions=(), refunds=(), merchants=(), settlements=()) -> Path:
        return write_day(self.landing, day, {
            "transactions.csv": [list(r) for r in transactions],
            "refunds.csv": [list(r) for r in refunds],
            "merchants.csv": [list(r) for r in merchants],
            "settlements.csv": [list(r) for r in settlements],
        })

    def run(self, day: date, **kwargs) -> pipeline.RunResult:
        return pipeline.run_date(self.con, self.landing, day, **kwargs)

    def load(self, day: date, **files) -> pipeline.RunResult:
        self.deliver(day, **files)
        result = self.run(day)
        assert result.ok, result.error
        return result

    def one(self, sql: str):
        return self.con.execute(sql).fetchone()

    def all(self, sql: str):
        return self.con.execute(sql).fetchall()

    def value(self, sql: str):
        return self.con.execute(sql).fetchone()[0]


@pytest.fixture
def lab(tmp_path: Path):
    lab = Lab(tmp_path)
    yield lab
    lab.con.close()


@pytest.fixture
def day() -> date:
    return START


@pytest.fixture(scope="session")
def loaded(tmp_path_factory):
    """A simulated 45-day feed, fully loaded. Shared and treated as read-only."""
    root = tmp_path_factory.mktemp("loaded")
    simulator = Simulator(START, DAYS, seed=7)
    simulator.run(root / "landing")
    con = db.connect(root / "warehouse.duckdb")
    results = pipeline.backfill(con, root / "landing")

    class Loaded:
        pass

    loaded = Loaded()
    loaded.con, loaded.results, loaded.simulator = con, results, simulator
    loaded.landing, loaded.last_day = root / "landing", START + timedelta(days=DAYS - 1)
    yield loaded
    con.close()
