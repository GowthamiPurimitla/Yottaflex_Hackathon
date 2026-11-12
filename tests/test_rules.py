"""Unit tests for pure validation helpers: boundaries, nulls, malformed values."""
import pytest

from txnpipe import rules as R

GOOD = {"event_id": "e1", "txn_id": "t1", "leg_seq": "1", "account_id": "a", "direction": "D",
        "amount": "10.00", "currency": "USD", "event_ts": "2025-01-01T00:00:00Z"}


def code_of(**over):
    with pytest.raises(R.RowError) as ei:
        R.validate_leg_fields({**GOOD, **over}, 2)
    return ei.value.code


def test_happy_leg_normalised():
    l = R.validate_leg_fields({**GOOD, "event_ts": "2025-01-01T05:30:00+05:30"}, 2)
    assert l.amount_minor == 1000 and l.event_ts == "2025-01-01T00:00:00.000000Z" and l.signed_minor == -1000


@pytest.mark.parametrize("amt,minor", [("0.01", 1), ("1", 100), ("1.5", 150), ("1.500", 150)])
def test_amount_boundaries_valid(amt, minor):
    assert R.parse_amount_minor(amt, "USD") == minor


@pytest.mark.parametrize("amt", ["0", "0.00", "-1", "abc", "1.234", "1e3", "NaN", "Infinity", "", "1,000.00", " "])
def test_amount_invalid(amt):
    with pytest.raises(R.RowError) as ei:
        R.parse_amount_minor(amt, "USD")
    assert ei.value.code == R.INVALID_AMOUNT


def test_jpy_has_no_minor_units():
    assert R.parse_amount_minor("500", "JPY") == 500
    with pytest.raises(R.RowError):
        R.parse_amount_minor("500.5", "JPY")


def test_amount_ceiling():
    with pytest.raises(R.RowError):
        R.parse_amount_minor("99999999999999.99", "USD")


@pytest.mark.parametrize("field", list(GOOD))
def test_each_required_field_null_or_blank(field):
    assert code_of(**{field: ""}) == R.MISSING_REQUIRED_FIELD
    assert code_of(**{field: "   "}) == R.MISSING_REQUIRED_FIELD


def test_field_codes():
    assert code_of(direction="d") == R.INVALID_DIRECTION
    assert code_of(currency="usd") == R.INVALID_CURRENCY
    assert code_of(currency="XXX") == R.INVALID_CURRENCY
    assert code_of(leg_seq="0") == R.INVALID_LEG_SEQ
    assert code_of(leg_seq="1_0") == R.INVALID_LEG_SEQ
    assert code_of(leg_seq="x") == R.INVALID_LEG_SEQ


@pytest.mark.parametrize("ts", ["yesterday", "2025-01-01", "2025-01-01T00:00:00", "2025-02-30T00:00:00Z"])
def test_timestamp_invalid_or_naive(ts):
    assert code_of(event_ts=ts) == R.INVALID_TIMESTAMP


def test_content_hash_ignores_row_number_and_detects_change():
    a = R.validate_leg_fields(GOOD, 2)
    b = R.validate_leg_fields(GOOD, 99)
    c = R.validate_leg_fields({**GOOD, "amount": "10.01"}, 2)
    assert a.content_hash == b.content_hash != c.content_hash


def test_account_validation():
    ok = R.validate_account_fields({"account_id": "a", "currency": "USD", "account_type": "customer",
                                    "opened_at": "2024-01-01T00:00:00Z", "closed_at": ""})
    assert ok.closed_at is None
    with pytest.raises(R.RowError):
        R.validate_account_fields({"account_id": "a", "currency": "USD", "account_type": "weird",
                                   "opened_at": "2024-01-01T00:00:00Z"})
    with pytest.raises(R.RowError):
        R.validate_account_fields({"account_id": "a", "currency": "USD", "account_type": "customer",
                                   "opened_at": "2024-01-01T00:00:00Z", "closed_at": "2023-01-01T00:00:00Z"})
