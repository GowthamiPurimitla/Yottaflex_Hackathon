"""Deterministic synthetic data generator.

Writes three files:
  accounts.csv         reference data
  legs.csv             pipeline INPUT (clean + deliberately defective rows, out-of-order)
  expected_labels.csv  ground truth, kept SEPARATE from the input (never read by the pipeline)
"""
from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import rules as R

CURRENCIES = ["USD", "EUR", "GBP", "INR", "JPY"]
CUR_WEIGHTS = [50, 25, 10, 10, 5]
BASE_TS = datetime(2025, 1, 1, tzinfo=timezone.utc)
HEADER = list(R.LEG_REQUIRED_COLUMNS) + ["description"]   # description is optional for inputs
FIRST = ["Alice", "Ben", "Carla", "Dev", "Elena", "Farid", "Grace", "Hiro", "Isla", "Jon"]
LAST = ["Nguyen", "Patel", "Garcia", "Smith", "Kim", "Okafor", "Rossi", "Silva", "Novak", "Berg"]
INTERNAL_NAME = "Bank clearing account"

DEFECTS = {  # kind -> weight
    "imbalanced": 3, "unknown_account": 2, "currency_mismatch": 2, "missing_field": 2,
    "bad_amount": 2, "bad_direction": 1, "bad_timestamp": 1, "future_ts": 1,
    "duplicate_exact": 3, "duplicate_conflict": 1, "leg_order": 1, "single_leg": 1,
    "malformed": 1, "leg_seq_gap": 1,
}


fmt_amount = R.format_minor


def make_accounts(n_customers: int):
    rows = []
    per_cur = {c: [] for c in CURRENCIES}
    for c in CURRENCIES:
        rows.append({"account_id": f"acc_int_{c.lower()}", "currency": c, "account_type": "internal",
                     "opened_at": "2024-01-01T00:00:00Z", "closed_at": "",
                     "account_name": f"{INTERNAL_NAME} ({c})"})
    for i in range(n_customers):
        c = CURRENCIES[i % len(CURRENCIES)]
        aid = f"acc_cust_{i + 1:06d}"
        per_cur[c].append(aid)
        name = f"{FIRST[i % len(FIRST)]} {LAST[(i // len(FIRST)) % len(LAST)]} #{i + 1}"
        rows.append({"account_id": aid, "currency": c, "account_type": "customer",
                     "opened_at": "2024-01-01T00:00:00Z", "closed_at": "",
                     "account_name": f"{name} ({c})"})
    return rows, per_cur


def generate(out_dir: str, n_txns: int = 40, seed: int = 42, defect_rate: float = 0.12,
             n_customers: int = 20, disorder: float = 0.05) -> dict:
    rng = random.Random(seed)
    n_customers = max(n_customers, 3 * len(CURRENCIES))  # >=3 customers/currency (split txns need 3)
    accounts, per_cur = make_accounts(n_customers)
    kinds, weights = list(DEFECTS), list(DEFECTS.values())
    names = {a["account_id"]: a["account_name"].rsplit(" (", 1)[0] for a in accounts}

    txns = []  # list of (txn_legs, defect_kind)
    for aid_list in per_cur.values():  # funding deposits so balances start positive
        for aid in aid_list:
            cur = next(a["currency"] for a in accounts if a["account_id"] == aid)
            txns.append(([("D", f"acc_int_{cur.lower()}", 1_000_000, "Opening funding - bank side"),
                          ("C", aid, 1_000_000, "Opening funding deposit")], cur, None))
    for _ in range(n_txns):
        cur = rng.choices(CURRENCIES, CUR_WEIGHTS)[0]
        custs, internal = per_cur[cur], f"acc_int_{cur.lower()}"
        amt = 1 if rng.random() < 0.03 else rng.randint(1, 500_000)   # boundary: 0.01
        kind = rng.choices(["transfer", "deposit", "withdrawal", "split"], [45, 20, 20, 15])[0]
        if kind == "transfer":
            a, b = rng.sample(custs, 2)
            legs = [("D", a, amt, f"Transfer to {names[b]}"), ("C", b, amt, f"Transfer from {names[a]}")]
        elif kind == "deposit":
            legs = [("D", internal, amt, "Cash deposit - bank side"), ("C", rng.choice(custs), amt, "Cash deposit")]
        elif kind == "withdrawal":
            legs = [("D", rng.choice(custs), amt, "ATM withdrawal"), ("C", internal, amt, "ATM withdrawal - bank side")]
        else:
            amt = max(amt, 2)
            part = rng.randint(1, amt - 1)
            a, b, c = rng.sample(custs, 3)
            legs = [("D", a, amt, "Split payment out"), ("C", b, part, f"Split payment from {names[a]}"),
                    ("C", c, amt - part, f"Split payment from {names[a]}")]
        defect = rng.choices(kinds, weights)[0] if rng.random() < defect_rate else None
        txns.append((legs, cur, defect))

    # ---- materialise rows with expected outcomes -----------------------------
    records = []   # dict(row=<dict|list>, exp=(status, code), txn=tid, defect=kind)
    dup_jobs = []  # (record, copy_record)
    for i, (legs, cur, defect) in enumerate(txns, start=1):
        tid = f"txn_{i:08d}"
        ts = BASE_TS + timedelta(seconds=7 * i)
        tz_variant = rng.random() < 0.1  # same instant written with +05:30 offset
        def ts_str(t):
            if tz_variant:
                return t.astimezone(timezone(timedelta(hours=5, minutes=30))).isoformat(timespec="seconds")
            return t.strftime("%Y-%m-%dT%H:%M:%SZ")
        recs = []
        for seq, (d, acc, minor, desc) in enumerate(legs, start=1):
            recs.append({"row": {"event_id": f"evt_{i:08d}_{seq}", "txn_id": tid, "leg_seq": str(seq),
                                 "account_id": acc, "direction": d, "amount": fmt_amount(minor, cur),
                                 "currency": cur, "event_ts": ts_str(ts), "description": desc},
                         "exp": ("loaded", ""), "txn": tid, "defect": defect or "", "minor": minor})

        def reject_all(code):
            for r in recs:
                r["exp"] = ("rejected", code)

        def reject_one(idx, code, sibling="TXN_QUARANTINED"):
            for j, r in enumerate(recs):
                r["exp"] = ("rejected", code if j == idx else sibling)

        if defect == "imbalanced":
            recs[-1]["row"]["amount"] = fmt_amount(recs[-1]["minor"] + 10 ** R.CURRENCY_EXPONENT[cur], cur)
            reject_all("TXN_IMBALANCED")
        elif defect == "unknown_account":
            recs[0]["row"]["account_id"] = f"acc_unknown_{i}"
            reject_one(0, "UNKNOWN_ACCOUNT")
        elif defect == "currency_mismatch":
            other = "USD" if cur == "JPY" else next(c for c in ("EUR", "GBP", "INR", "USD") if c != cur)
            recs[0]["row"]["currency"] = other
            reject_one(0, "ACCOUNT_CURRENCY_MISMATCH")
        elif defect == "missing_field":
            recs[0]["row"][rng.choice(["account_id", "amount", "event_ts"])] = ""
            reject_one(0, "MISSING_REQUIRED_FIELD")
        elif defect == "bad_amount":
            recs[0]["row"]["amount"] = rng.choice(["abc", "-5.00", "0", "1.234", "1e3"])
            reject_one(0, "INVALID_AMOUNT")
        elif defect == "bad_direction":
            recs[0]["row"]["direction"] = "X"
            reject_one(0, "INVALID_DIRECTION")
        elif defect == "bad_timestamp":
            recs[0]["row"]["event_ts"] = rng.choice(["not-a-date", "2025-13-45T00:00:00Z", "2025-01-01 10:00:00"])
            reject_one(0, "INVALID_TIMESTAMP")
        elif defect == "future_ts":
            recs[0]["row"]["event_ts"] = "2099-01-01T00:00:00Z"
            reject_one(0, "FUTURE_TIMESTAMP")
        elif defect == "leg_order":
            recs[1]["row"]["event_ts"] = ts_str(ts - timedelta(seconds=5))
            reject_all("TXN_LEG_ORDER_VIOLATION")
        elif defect == "single_leg":
            recs = recs[:1]
            reject_all("TXN_TOO_FEW_LEGS")
        elif defect == "leg_seq_gap":
            recs[1]["row"]["leg_seq"] = "5"
            reject_all("TXN_LEG_SEQ_INVALID")
        elif defect == "malformed":
            recs[0]["row"] = [recs[0]["row"][c] for c in HEADER[:5]]  # truncated, txn_id intact
            reject_one(0, "MALFORMED_ROW")
        elif defect == "duplicate_exact":
            src = rng.choice(recs)
            dup_jobs.append((src, {"row": dict(src["row"]), "exp": ("rejected", "DUPLICATE_EVENT"),
                                   "txn": tid, "defect": defect, "minor": src["minor"]}))
        elif defect == "duplicate_conflict":
            src = rng.choice(recs)
            row = dict(src["row"])
            row["amount"] = fmt_amount(src["minor"] + 1, cur)
            dup_jobs.append((src, {"row": row, "exp": ("rejected", "CONFLICTING_DUPLICATE"),
                                   "txn": tid, "defect": defect, "minor": src["minor"] + 1}))
            for r in recs:
                r["exp"] = ("rejected", "TXN_QUARANTINED")
        records.extend(recs)

    # ---- out-of-order delivery: local shuffles --------------------------------
    n = len(records)
    for i in range(n):
        if rng.random() < disorder:
            j = min(n - 1, max(0, i + rng.randint(-200, 200)))
            records[i], records[j] = records[j], records[i]
    # duplicates always arrive AFTER their original (so "first copy wins" labels are exact)
    for src, copy in dup_jobs:
        pos = records.index(src)
        records.insert(min(len(records), pos + rng.randint(1, 50)), copy)

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "accounts.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["account_id", "account_name", "currency", "account_type", "opened_at", "closed_at"],
                           lineterminator="\n")
        w.writeheader()
        w.writerows(accounts)
    with open(out / "legs.csv", "w", newline="", encoding="utf-8") as f, \
         open(out / "expected_labels.csv", "w", newline="", encoding="utf-8") as g:
        w = csv.writer(f, lineterminator="\n")
        lw = csv.writer(g, lineterminator="\n")
        w.writerow(HEADER)
        lw.writerow(["source_file", "row_number", "txn_id", "expected_status", "expected_reason_code",
                     "reason_description", "injected_defect"])
        for idx, rec in enumerate(records):
            row = rec["row"]
            w.writerow(row if isinstance(row, list) else [row[c] for c in HEADER])
            lw.writerow(["legs.csv", idx + 2, rec["txn"], rec["exp"][0], rec["exp"][1],
                         R.REASON_DESCRIPTIONS.get(rec["exp"][1], ""), rec["defect"]])
    stats = {"rows": len(records), "txns": len(txns), "seed": seed,
             "expected_loaded": sum(r["exp"][0] == "loaded" for r in records),
             "expected_rejected": sum(r["exp"][0] == "rejected" for r in records)}
    return {"dir": str(out), "stats": stats}
