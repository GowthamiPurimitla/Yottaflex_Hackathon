import csv

import pytest

from txnpipe.pipeline import PipelineConfig, run_pipeline

LEG_HDR = ["event_id", "txn_id", "leg_seq", "account_id", "direction", "amount", "currency", "event_ts"]
ACC_HDR = ["account_id", "currency", "account_type", "opened_at", "closed_at"]

ACCOUNTS = [
    ["a_usd_1", "USD", "customer", "2024-01-01T00:00:00Z", ""],
    ["a_usd_2", "USD", "customer", "2024-01-01T00:00:00Z", ""],
    ["a_usd_int", "USD", "internal", "2024-01-01T00:00:00Z", ""],
    ["a_eur_1", "EUR", "customer", "2024-01-01T00:00:00Z", ""],
    ["a_eur_int", "EUR", "internal", "2024-01-01T00:00:00Z", ""],
    ["a_jpy_1", "JPY", "customer", "2024-01-01T00:00:00Z", ""],
    ["a_jpy_int", "JPY", "internal", "2024-01-01T00:00:00Z", ""],
    ["a_closed", "USD", "customer", "2024-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
]


def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        if header is not None:
            w.writerow(header)
        w.writerows(rows)
    return str(path)


def leg(event_id, txn_id, seq, acc, d, amt, cur="USD", ts="2025-03-01T10:00:00Z"):
    return [event_id, txn_id, str(seq), acc, d, amt, cur, ts]


def txn(tid, amt="10.00", a="a_usd_1", b="a_usd_2", cur="USD", ts="2025-03-01T10:00:00Z"):
    return [leg(f"{tid}_1", tid, 1, a, "D", amt, cur, ts), leg(f"{tid}_2", tid, 2, b, "C", amt, cur, ts)]


class Env:
    def __init__(self, tmp_path):
        self.tmp = tmp_path
        self.db = str(tmp_path / "t.db")
        self.accounts = write_csv(tmp_path / "accounts.csv", ACC_HDR, ACCOUNTS)

    def legs(self, rows, name="legs.csv", header=LEG_HDR):
        return write_csv(self.tmp / name, header, rows)

    def run(self, legs_path, accounts=True, **kw):
        cfg = PipelineConfig(legs_path, self.db, self.accounts if accounts else None, **kw)
        return run_pipeline(cfg)

    def q(self, sql, args=()):
        from txnpipe.store import connect
        c = connect(self.db)
        try:
            return [dict(r) for r in c.execute(sql, args)]
        finally:
            c.close()

    def count(self, table, where="1=1"):
        return self.q(f"SELECT COUNT(*) n FROM {table} WHERE {where}")[0]["n"]

    def reasons(self):
        return {r["source_row_number"]: r["reason_code"] for r in self.q("SELECT * FROM reject_record WHERE entity='leg'")}


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)
