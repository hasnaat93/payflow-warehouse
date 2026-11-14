"""Deterministic synthetic source system.

Simulates a small payment processor and writes what an upstream system would
hand to a warehouse team: one folder per business date containing CSV extracts
and a manifest with row counts and checksums.

Everything here is fictional. The same seed always produces byte-identical
files, so the pipeline's results are reproducible and testable.

The extracts deliberately contain the problems a real feed has:

* exact duplicate rows and superseded versions of the same record
* malformed rows (bad amounts, unknown currency, missing keys, bad timestamps)
* records that arrive one to three days after the event (late-arriving facts)
* a merchant that transacts before it appears in the merchant extract
  (late-arriving dimension)
* merchant attributes that change over time (slowly changing dimension)
* refunds that reference a payment the warehouse has never seen
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import random
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path

TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

TRANSACTION_COLUMNS = [
    "txn_id", "merchant_id", "amount", "currency", "card_brand", "status", "decline_reason",
    "authorized_at", "captured_at", "fee_amount", "settled_at", "settlement_batch_id", "updated_at",
]
REFUND_COLUMNS = ["refund_id", "txn_id", "amount", "currency", "refunded_at", "reason"]
MERCHANT_COLUMNS = [
    "merchant_id", "merchant_name", "category", "country", "city",
    "settlement_currency", "fee_plan_code", "status", "onboarded_on",
]
SETTLEMENT_COLUMNS = [
    "batch_id", "merchant_id", "settlement_date", "currency",
    "gross_amount", "fee_amount", "refund_amount", "net_amount", "txn_count",
]
FILES = {
    "transactions.csv": TRANSACTION_COLUMNS,
    "refunds.csv": REFUND_COLUMNS,
    "merchants.csv": MERCHANT_COLUMNS,
    "settlements.csv": SETTLEMENT_COLUMNS,
}

# fee plan -> (basis points, fixed fee in minor units)
FEE_PLANS = {"STANDARD": (290, 30), "PREMIUM": (250, 25), "MICRO": (350, 10)}
CARD_BRANDS = [("VISA", 48), ("MASTERCARD", 34), ("AMEX", 10), ("UNIONPAY", 8)]
DECLINE_REASONS = [
    ("INSUFFICIENT_FUNDS", 42), ("DO_NOT_HONOR", 22), ("EXPIRED_CARD", 12),
    ("SUSPECTED_FRAUD", 9), ("INVALID_CARD", 8), ("LIMIT_EXCEEDED", 7),
]
REFUND_REASONS = ["CUSTOMER_REQUEST", "DUPLICATE_CHARGE", "ITEM_NOT_RECEIVED", "DEFECTIVE_ITEM"]


@dataclass
class Merchant:
    merchant_id: str
    name: str
    category: str
    country: str
    city: str
    currency: str
    fee_plan: str
    daily_txns: float          # average payments per day
    ticket: tuple[int, int]    # typical amount range, minor units
    weekend_factor: float
    onboard_offset: int = -30  # days relative to the first generated date
    extract_delay: int = 0     # days the merchant extract lags behind onboarding
    settle_lag: int = 1        # T+N settlement
    status: str = "ACTIVE"


@dataclass
class Change:
    """A change to a merchant attribute that takes effect on a given day offset."""

    day_offset: int
    merchant_id: str
    attribute: str
    value: str


def _merchants() -> list[Merchant]:
    return [
        Merchant("M-1001", "Northwind Coffee Roasters", "Food & Beverage", "US", "Portland", "USD", "STANDARD", 46, (350, 2800), 1.35),
        Merchant("M-1002", "Harbor Lane Books", "Retail", "US", "Boston", "USD", "STANDARD", 21, (900, 6500), 1.20),
        Merchant("M-1003", "Kestrel Outdoor Supply", "Retail", "US", "Denver", "USD", "PREMIUM", 17, (2500, 42000), 1.10),
        Merchant("M-1004", "Bluefin Streaming", "Digital Services", "US", "Austin", "USD", "MICRO", 58, (499, 1999), 1.00),
        Merchant("M-1005", "Juniper Dental Studio", "Healthcare", "US", "Chicago", "USD", "PREMIUM", 8, (6000, 95000), 0.15, settle_lag=2),
        Merchant("M-1006", "Tidewater Cycle Works", "Retail", "US", "Seattle", "USD", "STANDARD", 12, (1800, 120000), 1.45),
        Merchant("M-2001", "Lumen Atelier", "Retail", "DE", "Berlin", "EUR", "STANDARD", 19, (1500, 28000), 1.15),
        Merchant("M-2002", "Cafe Aurelia", "Food & Beverage", "IT", "Milan", "EUR", "MICRO", 39, (250, 2400), 1.30),
        Merchant("M-2003", "Nordlicht Software", "Digital Services", "DE", "Hamburg", "EUR", "PREMIUM", 11, (2900, 49900), 0.60),
        Merchant("M-3001", "Chenab Electronics", "Retail", "PK", "Lahore", "PKR", "STANDARD", 24, (150000, 4500000), 1.10),
        Merchant("M-3002", "Margalla Grocers", "Food & Beverage", "PK", "Islamabad", "PKR", "MICRO", 41, (40000, 900000), 1.20),
        Merchant("M-3003", "Indus Travel Desk", "Travel", "PK", "Karachi", "PKR", "PREMIUM", 9, (800000, 9500000), 0.85, settle_lag=2),
        # Onboarded part-way through, and missing from the merchant extract for its first two days.
        Merchant("M-1007", "Saltmarsh Pottery", "Retail", "US", "Charleston", "USD", "STANDARD", 14, (1200, 16000), 1.40,
                 onboard_offset=24, extract_delay=2),
        Merchant("M-2004", "Verde Bike Rental", "Travel", "ES", "Valencia", "EUR", "STANDARD", 16, (900, 7500), 1.60,
                 onboard_offset=41),
    ]


def _changes() -> list[Change]:
    return [
        Change(13, "M-1002", "fee_plan", "PREMIUM"),                 # renegotiated pricing
        Change(19, "M-2001", "city", "Munich"),                      # relocation
        Change(27, "M-1004", "name", "Bluefin Streaming Co."),       # name correction (type 1)
        Change(33, "M-3002", "category", "Grocery"),                 # reclassification
        Change(52, "M-1006", "status", "SUSPENDED"),                 # stops trading
        Change(60, "M-2002", "fee_plan", "STANDARD"),
        Change(66, "M-1006", "status", "ACTIVE"),                    # reinstated
    ]


@dataclass
class Txn:
    txn_id: str
    merchant_id: str
    currency: str
    card_brand: str
    amount: int
    authorized_at: datetime
    status: str
    updated_at: datetime
    visible_from: date
    decline_reason: str = ""
    captured_at: datetime | None = None
    fee: int | None = None
    settled_at: datetime | None = None
    batch_id: str = ""
    refunded: int = 0


@dataclass
class Refund:
    refund_id: str
    txn_id: str
    amount: int
    currency: str
    refunded_at: datetime
    reason: str
    netted: bool = False


@dataclass
class _Event:
    at: datetime
    kind: str            # CAPTURE | REVERSE | REFUND
    txn_id: str
    amount: int = 0


@dataclass
class GenerationSummary:
    start: date
    days: int
    seed: int
    out_dir: Path
    files: int = 0
    rows: dict[str, int] = field(default_factory=dict)


def fee_for(amount_minor: int, plan: str) -> int:
    """Percentage fee rounded half-up to the minor unit, plus a fixed fee, capped at the amount."""
    bps, fixed = FEE_PLANS[plan]
    variable = (amount_minor * bps + 5000) // 10000
    return min(amount_minor, variable + fixed)


def format_minor(amount_minor: int) -> str:
    sign = "-" if amount_minor < 0 else ""
    whole, cents = divmod(abs(amount_minor), 100)
    return f"{sign}{whole}.{cents:02d}"


def _ts(value: datetime | None) -> str:
    return value.strftime(TS_FORMAT) if value else ""


def _weighted(rng: random.Random, options: list[tuple[str, int]]) -> str:
    return rng.choices([o[0] for o in options], weights=[o[1] for o in options], k=1)[0]


def _csv_bytes(columns: list[str], rows: list[list[str]]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(columns)
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


class Simulator:
    def __init__(self, start: date, days: int, seed: int) -> None:
        self.start = start
        self.days = days
        self.rng = random.Random(seed)
        self.merchants = {m.merchant_id: m for m in _merchants()}
        self.changes = _changes()
        self.txns: dict[str, Txn] = {}
        self.refunds: list[Refund] = []
        self.events: dict[date, list[_Event]] = {}
        self.unsettled: dict[str, list[Txn]] = {}
        self.txn_seq = 0
        self.refund_seq = 0
        self.batch_seq = 0
        self.bad_seq = 0

    # ----- merchant state -------------------------------------------------

    def _apply_changes(self, day_offset: int) -> None:
        for change in self.changes:
            if change.day_offset == day_offset:
                merchant = self.merchants[change.merchant_id]
                setattr(merchant, change.attribute, change.value)

    def _onboarded(self, merchant: Merchant) -> date:
        return self.start + timedelta(days=merchant.onboard_offset)

    def _merchant_rows(self, today: date) -> list[list[str]]:
        rows = []
        for m in self.merchants.values():
            if today < self._onboarded(m) + timedelta(days=m.extract_delay):
                continue
            rows.append([
                m.merchant_id, m.name, m.category, m.country, m.city,
                m.currency, m.fee_plan, m.status, self._onboarded(m).isoformat(),
            ])
        return rows

    # ----- payment activity ----------------------------------------------

    def _schedule(self, event: _Event) -> None:
        self.events.setdefault(event.at.date(), []).append(event)

    def _create_payments(self, today: date, day_offset: int) -> None:
        rng = self.rng
        weekend = today.weekday() >= 5
        growth = 1.0 + 0.0025 * day_offset
        for m in self.merchants.values():
            onboarded = self._onboarded(m)
            if today < onboarded or m.status != "ACTIVE":
                continue
            ramp = min(1.0, 0.35 + 0.05 * (today - onboarded).days) if m.onboard_offset > 0 else 1.0
            expected = m.daily_txns * growth * ramp * (m.weekend_factor if weekend else 1.0)
            count = max(0, round(rng.gauss(expected, expected ** 0.5)))
            for _ in range(count):
                self._create_payment(m, today)

    def _create_payment(self, m: Merchant, today: date) -> None:
        rng = self.rng
        self.txn_seq += 1
        txn_id = f"TXN-{self.txn_seq:07d}"
        low, high = m.ticket
        # skew towards smaller tickets
        amount = int(low + (high - low) * (rng.random() ** 2.2))
        hour = min(23, max(0, int(rng.gauss(14, 4.5))))
        authorized_at = datetime.combine(today, time(hour, rng.randrange(60), rng.randrange(60)))

        # Roughly 1.5% of records miss the extract cutoff and arrive one to three days late.
        delay = rng.choice([1, 1, 2, 3]) if rng.random() < 0.015 else 0
        txn = Txn(
            txn_id=txn_id, merchant_id=m.merchant_id, currency=m.currency,
            card_brand=_weighted(rng, CARD_BRANDS), amount=amount,
            authorized_at=authorized_at, status="AUTHORIZED", updated_at=authorized_at,
            visible_from=today + timedelta(days=delay),
        )
        self.txns[txn_id] = txn

        decline_rate = 0.07 + (0.05 if amount > 0.8 * high else 0.0) + (0.02 if txn.card_brand == "AMEX" else 0.0)
        if rng.random() < decline_rate:
            txn.status = "DECLINED"
            txn.decline_reason = _weighted(rng, DECLINE_REASONS)
            return

        outcome = rng.random()
        if outcome < 0.86:      # captured shortly after authorization
            capture_at = authorized_at + timedelta(minutes=rng.randint(1, 240))
        elif outcome < 0.95:    # captured the next day (ship-then-capture)
            capture_at = authorized_at + timedelta(hours=rng.randint(14, 30))
        elif outcome < 0.98:    # authorization reversed
            self._schedule(_Event(authorized_at + timedelta(hours=rng.randint(2, 40)), "REVERSE", txn_id))
            return
        else:                   # never captured
            return
        self._schedule(_Event(capture_at, "CAPTURE", txn_id))

        if rng.random() < 0.045:
            earliest = max(capture_at, datetime.combine(txn.visible_from, time(0))) + timedelta(days=1)
            refund_at = earliest + timedelta(days=rng.randint(0, 9), minutes=rng.randint(0, 600))
            full = rng.random() < 0.6
            refund_amount = amount if full else max(1, int(amount * rng.uniform(0.15, 0.7)))
            self._schedule(_Event(refund_at, "REFUND", txn_id, refund_amount))

    def _apply_events(self, today: date) -> list[Refund]:
        new_refunds: list[Refund] = []
        for event in sorted(self.events.pop(today, []), key=lambda e: (e.at, e.txn_id)):
            txn = self.txns[event.txn_id]
            merchant = self.merchants[txn.merchant_id]
            if event.kind == "CAPTURE" and txn.status == "AUTHORIZED":
                txn.status = "CAPTURED"
                txn.captured_at = event.at
                txn.fee = fee_for(txn.amount, merchant.fee_plan)
                txn.updated_at = event.at
                self.unsettled.setdefault(txn.merchant_id, []).append(txn)
            elif event.kind == "REVERSE" and txn.status == "AUTHORIZED":
                txn.status = "REVERSED"
                txn.updated_at = event.at
            elif event.kind == "REFUND" and txn.status in ("CAPTURED", "PARTIALLY_REFUNDED"):
                amount = min(event.amount, txn.amount - txn.refunded)
                if amount <= 0:
                    continue
                txn.refunded += amount
                txn.status = "REFUNDED" if txn.refunded == txn.amount else "PARTIALLY_REFUNDED"
                txn.updated_at = event.at
                self.refund_seq += 1
                refund = Refund(
                    f"RFD-{self.refund_seq:06d}", txn.txn_id, amount, txn.currency,
                    event.at, self.rng.choice(REFUND_REASONS),
                )
                self.refunds.append(refund)
                new_refunds.append(refund)
        return new_refunds

    def _settle(self, today: date) -> list[list[str]]:
        """End-of-day settlement: one batch per merchant with something to pay out."""
        run_at = datetime.combine(today, time(23, 0))
        rows = []
        for m in self.merchants.values():
            cutoff = today - timedelta(days=m.settle_lag)
            waiting = self.unsettled.get(m.merchant_id, [])
            eligible = [
                t for t in waiting
                if t.captured_at is not None and t.captured_at.date() <= cutoff and t.visible_from <= today
            ]
            pending_refunds = [
                r for r in self.refunds
                if not r.netted and self.txns[r.txn_id].merchant_id == m.merchant_id
                and r.refunded_at.date() < today
            ]
            if not eligible:
                continue
            gross = sum(t.amount for t in eligible)
            fees = sum(t.fee or 0 for t in eligible)
            refunds = sum(r.amount for r in pending_refunds)
            net = gross - fees - refunds
            if net <= 0:
                continue  # carried forward until the merchant is owed something
            self.batch_seq += 1
            batch_id = f"STL-{today:%Y%m%d}-{self.batch_seq:05d}"
            settled_ids = {t.txn_id for t in eligible}
            self.unsettled[m.merchant_id] = [t for t in waiting if t.txn_id not in settled_ids]
            for t in eligible:
                t.batch_id = batch_id
                t.settled_at = run_at
                t.updated_at = run_at
            for r in pending_refunds:
                r.netted = True
            rows.append([
                batch_id, m.merchant_id, today.isoformat(), m.currency,
                format_minor(gross), format_minor(fees), format_minor(refunds), format_minor(net),
                str(len(eligible)),
            ])
        return rows

    # ----- extract rows ---------------------------------------------------

    def _txn_row(self, t: Txn) -> list[str]:
        return [
            t.txn_id, t.merchant_id, format_minor(t.amount), t.currency, t.card_brand, t.status,
            t.decline_reason, _ts(t.authorized_at), _ts(t.captured_at),
            format_minor(t.fee) if t.fee is not None else "", _ts(t.settled_at), t.batch_id, _ts(t.updated_at),
        ]

    def _transaction_rows(self, today: date, day_offset: int) -> list[list[str]]:
        changed = [
            t for t in self.txns.values()
            if t.visible_from <= today and (t.updated_at.date() == today or t.visible_from == today)
        ]
        changed.sort(key=lambda t: t.txn_id)
        rows = [self._txn_row(t) for t in changed]
        rng = self.rng

        # Exact duplicates: the upstream job re-sends a few rows.
        if rows and day_offset % 4 == 1:
            for row in rng.sample(rows, k=min(3, len(rows))):
                rows.append(list(row))

        # Superseded version: an older copy of a row appears after the current one.
        if day_offset % 9 == 3:
            captured = [t for t in changed if t.status == "CAPTURED" and t.captured_at]
            if captured:
                t = rng.choice(captured)
                stale = self._txn_row(t)
                stale[5], stale[8], stale[9], stale[10], stale[11] = "AUTHORIZED", "", "", "", ""
                stale[12] = _ts(t.authorized_at)
                rows.append(stale)

        # Malformed rows that must be quarantined, never loaded.
        if day_offset % 5 == 2:
            rows.extend(self._bad_rows(today, day_offset))
        return rows

    def _bad_rows(self, today: date, day_offset: int) -> list[list[str]]:
        at = _ts(datetime.combine(today, time(12, 0)))
        variants = [
            lambda i: [f"TXN-BAD-{i:04d}", "M-1001", "12.3x", "USD", "VISA", "AUTHORIZED", "", at, "", "", "", "", at],
            lambda i: [f"TXN-BAD-{i:04d}", "M-1002", "-45.00", "USD", "VISA", "AUTHORIZED", "", at, "", "", "", "", at],
            lambda i: [f"TXN-BAD-{i:04d}", "M-2001", "18.50", "XXX", "MASTERCARD", "AUTHORIZED", "", at, "", "", "", "", at],
            lambda i: [f"TXN-BAD-{i:04d}", "", "22.00", "USD", "VISA", "AUTHORIZED", "", at, "", "", "", "", at],
            lambda i: [f"TXN-BAD-{i:04d}", "M-1004", "9.99", "USD", "VISA", "AUTHORIZED", "", "yesterday", "", "", "", "", at],
            lambda i: [f"TXN-BAD-{i:04d}", "M-3001", "1500.00", "PKR", "VISA", "PENDING_REVIEW", "", at, "", "", "", "", at],
            lambda i: ["", "M-1003", "64.00", "USD", "AMEX", "AUTHORIZED", "", at, "", "", "", "", at],
            lambda i: [f"TXN-BAD-{i:04d}", "M-2002", "7.255", "EUR", "VISA", "AUTHORIZED", "", at, "", "", "", "", at],
        ]
        rows = []
        for k in range(2):
            self.bad_seq += 1
            rows.append(variants[(day_offset // 5 + k) % len(variants)](self.bad_seq))
        return rows

    def _refund_rows(self, new_refunds: list[Refund], today: date, day_offset: int) -> list[list[str]]:
        rows = [
            [r.refund_id, r.txn_id, format_minor(r.amount), r.currency, _ts(r.refunded_at), r.reason]
            for r in new_refunds
        ]
        if day_offset in (17, 48):  # refund for a payment that was never delivered
            at = _ts(datetime.combine(today, time(15, 30)))
            rows.append([f"RFD-ORPHAN-{day_offset:02d}", f"TXN-UNKNOWN-{day_offset:02d}", "25.00", "USD", at, "CUSTOMER_REQUEST"])
        if day_offset == 30:        # malformed refund amount
            at = _ts(datetime.combine(today, time(9, 5)))
            rows.append(["RFD-BAD-0001", "TXN-0000001", "ten", "USD", at, "CUSTOMER_REQUEST"])
        return rows

    # ----- driver ---------------------------------------------------------

    def run(self, out_dir: Path) -> GenerationSummary:
        summary = GenerationSummary(self.start, self.days, 0, out_dir, rows={name: 0 for name in FILES})
        for day_offset in range(self.days):
            today = self.start + timedelta(days=day_offset)
            self._apply_changes(day_offset)
            self._create_payments(today, day_offset)
            new_refunds = self._apply_events(today)
            settlement_rows = self._settle(today)
            content = {
                "transactions.csv": self._transaction_rows(today, day_offset),
                "refunds.csv": self._refund_rows(new_refunds, today, day_offset),
                "merchants.csv": self._merchant_rows(today),
                "settlements.csv": settlement_rows,
            }
            write_day(out_dir, today, content)
            for name, rows in content.items():
                summary.rows[name] += len(rows)
            summary.files += len(content) + 1
        return summary


def write_day(out_dir: Path, business_date: date, content: dict[str, list[list[str]]]) -> Path:
    """Write one day's extracts plus a manifest of row counts and SHA-256 checksums."""
    folder = out_dir / business_date.isoformat()
    folder.mkdir(parents=True, exist_ok=True)
    manifest_files = {}
    for name, columns in FILES.items():
        rows = content.get(name, [])
        payload = _csv_bytes(columns, rows)
        (folder / name).write_bytes(payload)
        manifest_files[name] = {"rows": len(rows), "sha256": hashlib.sha256(payload).hexdigest()}
    manifest = {
        "business_date": business_date.isoformat(),
        "source_system": "payflow-simulator",
        "files": manifest_files,
    }
    (folder / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return folder


def generate(out_dir: Path, start: date, days: int, seed: int = 7) -> GenerationSummary:
    summary = Simulator(start, days, seed).run(out_dir)
    summary.seed = seed
    return summary
