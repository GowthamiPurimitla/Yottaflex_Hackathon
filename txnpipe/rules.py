"""Constants and pure validation helpers (no I/O, fully unit-testable)."""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

# ---------------------------------------------------------------- schemas
LEG_REQUIRED_COLUMNS = (
    "event_id", "txn_id", "leg_seq", "account_id",
    "direction", "amount", "currency", "event_ts",
)
ACCOUNT_REQUIRED_COLUMNS = ("account_id", "currency", "account_type", "opened_at")
ACCOUNT_OPTIONAL_COLUMNS = ("closed_at", "account_name")
LEG_OPTIONAL_COLUMNS = ("description",)
ACCOUNT_TYPES = ("customer", "internal")

# ISO-4217 minor-unit exponent for the currencies we accept.
CURRENCY_EXPONENT = {"USD": 2, "EUR": 2, "GBP": 2, "INR": 2, "JPY": 0}
MAX_AMOUNT_MINOR = 10**14  # sanity ceiling (1e12 major units for 2dp currencies)

# ---------------------------------------------------------------- reason codes
MALFORMED_ROW = "MALFORMED_ROW"
MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
INVALID_LEG_SEQ = "INVALID_LEG_SEQ"
INVALID_DIRECTION = "INVALID_DIRECTION"
INVALID_CURRENCY = "INVALID_CURRENCY"
INVALID_AMOUNT = "INVALID_AMOUNT"
INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
FUTURE_TIMESTAMP = "FUTURE_TIMESTAMP"
UNKNOWN_ACCOUNT = "UNKNOWN_ACCOUNT"
ACCOUNT_CURRENCY_MISMATCH = "ACCOUNT_CURRENCY_MISMATCH"
EVENT_OUTSIDE_ACCOUNT_LIFETIME = "EVENT_OUTSIDE_ACCOUNT_LIFETIME"
DUPLICATE_EVENT = "DUPLICATE_EVENT"
CONFLICTING_DUPLICATE = "CONFLICTING_DUPLICATE"
CONFLICTING_REPLAY = "CONFLICTING_REPLAY"
TXN_QUARANTINED = "TXN_QUARANTINED"
TXN_TOO_FEW_LEGS = "TXN_TOO_FEW_LEGS"
TXN_LEG_SEQ_INVALID = "TXN_LEG_SEQ_INVALID"
TXN_LEG_ORDER_VIOLATION = "TXN_LEG_ORDER_VIOLATION"
TXN_IMBALANCED = "TXN_IMBALANCED"
TXN_ALREADY_PUBLISHED = "TXN_ALREADY_PUBLISHED"
# account-file reasons
ACCOUNT_DUPLICATE = "ACCOUNT_DUPLICATE"
ACCOUNT_CURRENCY_CHANGE = "ACCOUNT_CURRENCY_CHANGE"
INVALID_ACCOUNT_TYPE = "INVALID_ACCOUNT_TYPE"


REASON_DESCRIPTIONS = {
    MALFORMED_ROW: "Row has more or fewer fields than the header",
    MISSING_REQUIRED_FIELD: "A required field is empty",
    INVALID_LEG_SEQ: "leg_seq is not a whole number >= 1",
    INVALID_DIRECTION: "direction must be D (debit) or C (credit)",
    INVALID_CURRENCY: "Currency is not supported",
    INVALID_AMOUNT: "Amount is not a positive number with valid decimals for its currency",
    INVALID_TIMESTAMP: "Timestamp is not ISO-8601 with a time zone",
    FUTURE_TIMESTAMP: "Event time is in the future",
    UNKNOWN_ACCOUNT: "Account does not exist in the accounts list",
    ACCOUNT_CURRENCY_MISMATCH: "Leg currency differs from the account's currency",
    EVENT_OUTSIDE_ACCOUNT_LIFETIME: "Event is before the account opened or after it closed",
    DUPLICATE_EVENT: "Same event already appeared earlier in this file (exact copy)",
    CONFLICTING_DUPLICATE: "Same event_id appeared again with different values",
    CONFLICTING_REPLAY: "event_id was already loaded earlier with different values",
    TXN_QUARANTINED: "Another leg of this transaction was rejected, so the whole transaction is held back",
    TXN_TOO_FEW_LEGS: "A transaction needs at least 2 legs",
    TXN_LEG_SEQ_INVALID: "leg_seq values are not 1..n without gaps or repeats",
    TXN_LEG_ORDER_VIOLATION: "Event time goes backwards as leg_seq increases",
    TXN_IMBALANCED: "Total debits do not equal total credits (per currency)",
    TXN_ALREADY_PUBLISHED: "This transaction was already loaded; published transactions cannot change",
    ACCOUNT_DUPLICATE: "account_id appears more than once in the accounts file",
    ACCOUNT_CURRENCY_CHANGE: "An existing account cannot change currency",
    INVALID_ACCOUNT_TYPE: "account_type must be customer or internal",
}


def format_minor(minor: int, currency: str) -> str:
    """Integer minor units -> plain decimal text, e.g. 12550 USD -> '125.50', 500 JPY -> '500'."""
    exp = CURRENCY_EXPONENT[currency]
    if exp == 0:
        return str(minor)
    sign = "-" if minor < 0 else ""
    minor = abs(minor)
    return f"{sign}{minor // 10**exp}.{minor % 10**exp:0{exp}d}"


def pretty_ts(ts: str) -> str:
    """'2025-01-01T00:00:07.000000Z' -> '2025-01-01T00:00:07Z' (still ISO-8601, easier to read)."""
    return ts[:-8] + "Z" if ts and ts.endswith(".000000Z") else ts


class RowError(Exception):
    """A row-level validation failure carrying a reason code."""

    def __init__(self, code: str, detail: str):
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


# ---------------------------------------------------------------- parsers
_AMOUNT_RE = re.compile(r"^\d+(\.\d+)?$")


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp with explicit offset/Z; return aware UTC."""
    v = value.strip()
    if v.endswith(("Z", "z")):
        v = v[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(v)
    except ValueError:
        raise RowError(INVALID_TIMESTAMP, f"not ISO-8601: {value!r}")
    if dt.tzinfo is None:
        raise RowError(INVALID_TIMESTAMP, f"timezone offset required: {value!r}")
    return dt.astimezone(timezone.utc)


def format_ts(dt: datetime) -> str:
    """Canonical, lexicographically sortable UTC form."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_amount_minor(value: str, currency: str) -> int:
    """Strict decimal -> integer minor units. Rejects <=0, junk, excess scale."""
    v = value.strip()
    if not _AMOUNT_RE.match(v):
        raise RowError(INVALID_AMOUNT, f"not a positive plain decimal: {value!r}")
    exp = CURRENCY_EXPONENT[currency]
    d = Decimal(v)
    if d.as_tuple().exponent < -exp and d != d.quantize(Decimal(1).scaleb(-exp)):
        raise RowError(INVALID_AMOUNT, f"{value!r} has more than {exp} decimals for {currency}")
    minor = int(d.scaleb(exp))
    if minor <= 0:
        raise RowError(INVALID_AMOUNT, f"amount must be > 0: {value!r}")
    if minor > MAX_AMOUNT_MINOR:
        raise RowError(INVALID_AMOUNT, f"amount exceeds ceiling: {value!r}")
    return minor


@dataclass(frozen=True)
class Leg:
    """A fully validated, normalised ledger leg."""
    event_id: str
    txn_id: str
    leg_seq: int
    account_id: str
    direction: str          # 'D' or 'C'
    currency: str
    amount_minor: int
    event_ts: str           # canonical UTC string
    source_row_number: int
    description: str = ""   # optional, informational; not part of the content hash

    @property
    def content_hash(self) -> str:
        key = "|".join((self.txn_id, str(self.leg_seq), self.account_id, self.direction,
                        self.currency, str(self.amount_minor), self.event_ts))
        return hashlib.sha256(key.encode()).hexdigest()

    @property
    def signed_minor(self) -> int:
        """Net-credit convention: credit +, debit -."""
        return self.amount_minor if self.direction == "C" else -self.amount_minor


def validate_leg_fields(rec: dict, row_number: int) -> Leg:
    """Field-level (single-row, reference-free) validation of a leg record."""
    missing = [c for c in LEG_REQUIRED_COLUMNS if not (rec.get(c) or "").strip()]
    if missing:
        raise RowError(MISSING_REQUIRED_FIELD, "empty: " + ",".join(missing))
    r = {k: rec[k].strip() for k in LEG_REQUIRED_COLUMNS}
    try:
        seq = int(r["leg_seq"])
        if seq < 1 or not r["leg_seq"].isdigit():
            raise ValueError
    except ValueError:
        raise RowError(INVALID_LEG_SEQ, f"leg_seq must be integer >= 1: {r['leg_seq']!r}")
    if r["direction"] not in ("D", "C"):
        raise RowError(INVALID_DIRECTION, f"direction must be D or C: {r['direction']!r}")
    if r["currency"] not in CURRENCY_EXPONENT:
        raise RowError(INVALID_CURRENCY, f"unsupported currency: {r['currency']!r}")
    minor = parse_amount_minor(r["amount"], r["currency"])
    ts = format_ts(parse_timestamp(r["event_ts"]))
    return Leg(r["event_id"], r["txn_id"], seq, r["account_id"], r["direction"],
               r["currency"], minor, ts, row_number, (rec.get("description") or "").strip())


@dataclass(frozen=True)
class Account:
    account_id: str
    currency: str
    account_type: str
    opened_at: str
    closed_at: Optional[str]
    account_name: Optional[str] = None


def validate_account_fields(rec: dict) -> Account:
    missing = [c for c in ACCOUNT_REQUIRED_COLUMNS if not (rec.get(c) or "").strip()]
    if missing:
        raise RowError(MISSING_REQUIRED_FIELD, "empty: " + ",".join(missing))
    cur = rec["currency"].strip()
    if cur not in CURRENCY_EXPONENT:
        raise RowError(INVALID_CURRENCY, f"unsupported currency: {cur!r}")
    typ = rec["account_type"].strip()
    if typ not in ACCOUNT_TYPES:
        raise RowError(INVALID_ACCOUNT_TYPE, f"account_type must be one of {ACCOUNT_TYPES}: {typ!r}")
    opened = format_ts(parse_timestamp(rec["opened_at"]))
    closed_raw = (rec.get("closed_at") or "").strip()
    closed = format_ts(parse_timestamp(closed_raw)) if closed_raw else None
    if closed and closed < opened:
        raise RowError(INVALID_TIMESTAMP, "closed_at earlier than opened_at")
    return Account(rec["account_id"].strip(), cur, typ, opened, closed,
                   (rec.get("account_name") or "").strip() or None)
