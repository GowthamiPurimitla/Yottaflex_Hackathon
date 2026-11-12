"""Reconciliation checks, label evaluation and CSV/JSON/Markdown exports."""
from __future__ import annotations

import csv
import json
from pathlib import Path

from . import rules as R
from .store import connect


def _one(conn, sql, args=()):
    return conn.execute(sql, args).fetchone()[0]


def reconcile(db_path: str, run_id: str | None = None) -> list[dict]:
    """Return a list of {check, passed, detail}. All must pass for the curated layer to be trusted."""
    conn = connect(db_path)
    checks = []

    def add(name, passed, detail=""):
        checks.append({"check": name, "passed": bool(passed), "detail": str(detail)})

    # 1. global double-entry balance, per currency
    bad = conn.execute("""SELECT currency, SUM(CASE direction WHEN 'D' THEN amount_minor END) d,
                                 SUM(CASE direction WHEN 'C' THEN amount_minor END) c
                          FROM fact_ledger_entry GROUP BY currency HAVING d IS NOT c""").fetchall()
    add("curated debits == credits per currency", not bad, [dict(r) for r in bad] or "balanced")
    # 2. every curated txn balances per currency
    n = _one(conn, """SELECT COUNT(*) FROM (SELECT txn_id, currency,
                         SUM(CASE direction WHEN 'D' THEN amount_minor ELSE 0 END) -
                         SUM(CASE direction WHEN 'C' THEN amount_minor ELSE 0 END) diff
                      FROM fact_ledger_entry GROUP BY txn_id, currency HAVING diff != 0)""")
    add("every curated transaction balances per currency", n == 0, f"{n} unbalanced")
    # 3. referential integrity
    n = _one(conn, "SELECT COUNT(*) FROM fact_ledger_entry l LEFT JOIN dim_account a USING(account_id) WHERE a.account_id IS NULL")
    add("ledger.account_id exists in dim_account", n == 0, f"{n} orphans")
    n = _one(conn, "SELECT COUNT(*) FROM fact_ledger_entry l LEFT JOIN fact_transaction t USING(txn_id) WHERE t.txn_id IS NULL")
    add("ledger.txn_id exists in fact_transaction", n == 0, f"{n} orphans")
    n = _one(conn, """SELECT COUNT(*) FROM fact_transaction t WHERE leg_count !=
                      (SELECT COUNT(*) FROM fact_ledger_entry l WHERE l.txn_id = t.txn_id)""")
    add("fact_transaction.leg_count matches ledger", n == 0, f"{n} mismatches")
    n = _one(conn, "SELECT COUNT(*) FROM fact_ledger_entry l JOIN dim_account a USING(account_id) WHERE a.currency != l.currency")
    add("leg currency == account currency", n == 0, f"{n} mismatches")
    # 4. uniqueness (also enforced by PK/UNIQUE constraints)
    n = _one(conn, "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM fact_ledger_entry")
    add("event_id unique in curated ledger", n == 0, f"{n} duplicates")
    # 5. traceability
    n = _one(conn, "SELECT COUNT(*) FROM fact_ledger_entry WHERE source_file IS NULL OR source_row_number IS NULL OR run_id IS NULL")
    add("every curated row has run_id/source_file/row", n == 0, f"{n} untraceable")
    n = _one(conn, "SELECT COUNT(*) FROM reject_record WHERE source_file='' OR source_row_number < 1 OR raw_record=''")
    add("every reject has source_file/row/raw_record", n == 0, f"{n} untraceable")
    # 6. no rejected row is also curated
    n = _one(conn, """SELECT COUNT(*) FROM reject_record r JOIN fact_ledger_entry l
                      ON l.source_file=r.source_file AND l.source_row_number=r.source_row_number
                      WHERE r.entity='leg'""")
    add("no source row is both curated and rejected", n == 0, f"{n} overlaps")
    # 7. source-to-target row accounting per successful run
    bad = conn.execute("""SELECT run_id, rows_read, rows_loaded, rows_already_loaded, rows_rejected FROM pipeline_run
                          WHERE status='success' AND rows_read != rows_loaded + rows_already_loaded + rows_rejected""").fetchall()
    add("per run: rows_read == loaded + already_loaded + rejected", not bad, [dict(r) for r in bad] or "all runs reconcile")
    # 8. latest successful run for each file: its reject_record count matches the run's own count
    for r in conn.execute("""SELECT legs_file, run_id, rows_rejected FROM pipeline_run p WHERE status='success'
                             AND started_at = (SELECT MAX(started_at) FROM pipeline_run WHERE legs_file=p.legs_file AND status='success')"""):
        n = _one(conn, "SELECT COUNT(*) FROM reject_record WHERE source_file=? AND entity='leg'", (r["legs_file"],))
        add(f"reject_record rows for {r['legs_file']} == run {r['run_id']} rows_rejected", n == r["rows_rejected"],
            f"table={n} run={r['rows_rejected']}")
    # 8b. raw layer holds every source row of the latest successful run of each file
    for r in conn.execute("""SELECT legs_file, run_id, rows_read FROM pipeline_run p WHERE status='success'
                             AND started_at = (SELECT MAX(started_at) FROM pipeline_run WHERE legs_file=p.legs_file AND status='success')"""):
        n = _one(conn, "SELECT COUNT(*) FROM raw_record WHERE source_file=? AND entity='leg'", (r["legs_file"],))
        add(f"raw_record rows for {r['legs_file']} == run {r['run_id']} rows_read", n == r["rows_read"],
            f"raw={n} read={r['rows_read']}")
    n = _one(conn, """SELECT COUNT(*) FROM fact_ledger_entry l LEFT JOIN raw_record w
                      ON w.entity='leg' AND w.source_file=l.source_file AND w.source_row_number=l.source_row_number
                      WHERE w.source_file IS NULL""")
    add("every curated row traces to a raw_record", n == 0, f"{n} without raw")
    # 9. every flagged imbalance is quarantined (never curated)
    n = _one(conn, "SELECT COUNT(*) FROM imbalance_report i JOIN fact_transaction t USING(txn_id)")
    add("imbalanced transactions absent from curated layer", n == 0, f"{n} leaked")
    conn.close()
    return checks


def evaluate_labels(db_path: str, labels_path: str) -> dict:
    """Compare actual per-row outcomes to the generator's ground truth."""
    conn = connect(db_path)
    mismatches, total = [], 0
    loaded = {(r[0], r[1]) for r in conn.execute("SELECT source_file, source_row_number FROM fact_ledger_entry")}
    rejected = {(r[0], r[1]): r[2] for r in conn.execute(
        "SELECT source_file, source_row_number, reason_code FROM reject_record WHERE entity='leg'")}
    with open(labels_path, newline="", encoding="utf-8") as f:
        for lab in csv.DictReader(f):
            total += 1
            key = (lab["source_file"], int(lab["row_number"]))
            row = key[1]
            if key in loaded:
                actual = ("loaded", "")
            elif key in rejected:
                actual = ("rejected", rejected[key])
            else:
                actual = ("missing", "")
            expected = (lab["expected_status"], lab["expected_reason_code"])
            if actual != expected:
                mismatches.append({"row": row, "txn_id": lab["txn_id"], "expected": expected, "actual": actual,
                                   "defect": lab["injected_defect"]})
    conn.close()
    return {"rows_labelled": total, "mismatches": len(mismatches), "examples": mismatches[:10]}


def pretty_raw(raw_json: str) -> str:
    """JSON blob -> 'field=value; field=value' so a human can read the original row."""
    d = json.loads(raw_json)
    if "_raw_values" in d:
        return "MALFORMED ROW: " + " | ".join(str(v) for v in d["_raw_values"])
    if "amount_minor" in d and "currency" in d:
        d["amount"] = R.format_minor(d.pop("amount_minor"), d["currency"])
    if "event_ts" in d:
        d["event_ts"] = R.pretty_ts(d["event_ts"])
    order = list(R.LEG_REQUIRED_COLUMNS) + ["description", "account_name", "closed_at"]
    keys = sorted(d, key=lambda k: order.index(k) if k in order else len(order))
    return "; ".join(f"{k}={d[k]}" for k in keys if d[k] not in ("", None))


def _csv(path, header, rows):
    n = 0
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        for r in rows:
            w.writerow(r)
            n += 1
    return n


def _write_readable_exports(conn, out: Path) -> dict:
    """Human-friendly CSVs: names not just ids, decimal amounts, DEBIT/CREDIT words, plain-English reasons."""
    m = R.format_minor
    c = {}
    c["ledger_entries.csv"] = _csv(out / "ledger_entries.csv",
        ["txn_id", "leg_seq", "event_id", "event_time", "account_id", "account_name", "direction",
         "amount", "currency", "description", "arrived_late", "source_file", "source_row", "run_id"],
        ([r["txn_id"], r["leg_seq"], r["event_id"], R.pretty_ts(r["event_ts"]), r["account_id"], r["account_name"] or "",
          "DEBIT" if r["direction"] == "D" else "CREDIT", m(r["amount_minor"], r["currency"]), r["currency"],
          r["description"] or "", "yes" if r["is_late_arrival"] else "no", r["source_file"],
          r["source_row_number"], r["run_id"]]
         for r in conn.execute("""SELECT l.*, a.account_name FROM fact_ledger_entry l JOIN dim_account a USING(account_id)
                                  ORDER BY l.event_ts, l.txn_id, l.leg_seq""")))
    debits = {}
    for r in conn.execute("SELECT txn_id, currency, SUM(amount_minor) t FROM fact_ledger_entry WHERE direction='D' GROUP BY 1,2"):
        debits.setdefault(r["txn_id"], []).append(f"{m(r['t'], r['currency'])} {r['currency']}")
    c["transactions.csv"] = _csv(out / "transactions.csv",
        ["txn_id", "number_of_legs", "total_amount", "first_event_time", "last_event_time", "source_file", "run_id"],
        ([r["txn_id"], r["leg_count"], " + ".join(debits.get(r["txn_id"], [])), R.pretty_ts(r["first_event_ts"]),
          R.pretty_ts(r["last_event_ts"]), r["source_file"], r["run_id"]]
         for r in conn.execute("SELECT * FROM fact_transaction ORDER BY first_event_ts, txn_id")))
    c["rejects.csv"] = _csv(out / "rejects.csv",
        ["source_file", "source_row", "entity", "txn_id", "event_id", "reason_code", "reason_explained",
         "detail", "original_row", "run_id"],
        ([r["source_file"], r["source_row_number"], r["entity"], r["txn_id"] or "", r["event_id"] or "", r["reason_code"],
          R.REASON_DESCRIPTIONS.get(r["reason_code"], ""), r["detail"] or "", pretty_raw(r["raw_record"]), r["run_id"]]
         for r in conn.execute("SELECT * FROM reject_record ORDER BY source_file, source_row_number")))
    c["imbalance_report.csv"] = _csv(out / "imbalance_report.csv",
        ["txn_id", "currency", "total_debits", "total_credits", "difference", "source_file", "run_id"],
        ([r["txn_id"], r["currency"], m(r["debit_minor"], r["currency"]), m(r["credit_minor"], r["currency"]),
          m(r["diff_minor"], r["currency"]), r["source_file"], r["run_id"]]
         for r in conn.execute("SELECT * FROM imbalance_report ORDER BY txn_id, currency")))
    c["raw_records.csv"] = _csv(out / "raw_records.csv",
        ["entity", "source_file", "source_row", "original_row", "run_id"],
        ([r["entity"], r["source_file"], r["source_row_number"], pretty_raw(r["raw_json"]), r["run_id"]]
         for r in conn.execute("SELECT * FROM raw_record ORDER BY entity, source_file, source_row_number")))
    c["account_balances.csv"] = _csv(out / "account_balances.csv",
        ["account_id", "account_name", "account_type", "currency", "balance", "number_of_entries", "is_overdrawn"],
        ([r["account_id"], r["account_name"] or "", r["account_type"], r["currency"], m(r["balance_minor"], r["currency"]),
          r["entry_count"], "yes" if r["account_type"] == "customer" and r["balance_minor"] < 0 else "no"]
         for r in conn.execute("""SELECT b.*, a.account_name FROM v_account_balance b JOIN dim_account a USING(account_id)
                                  ORDER BY b.account_id""")))
    c["pipeline_runs.csv"] = _csv(out / "pipeline_runs.csv",
        ["run_id", "status", "started_at", "finished_at", "legs_file", "rows_read", "rows_loaded",
         "rows_already_loaded", "rows_rejected", "txns_loaded", "txns_quarantined", "failure_reason"],
        ([r[k] if r[k] is not None else "" for k in r.keys()] for r in conn.execute(
            "SELECT run_id,status,started_at,finished_at,legs_file,rows_read,rows_loaded,rows_already_loaded,"
            "rows_rejected,txns_loaded,txns_quarantined,failure_reason FROM pipeline_run ORDER BY started_at")))
    (out / "README.md").write_text(GUIDE, encoding="utf-8")
    return c


GUIDE = """# What is in this folder

| file | what it shows |
|---|---|
| ledger_entries.csv | The accepted (curated) transactions, one line per debit/credit leg, with account names and descriptions |
| transactions.csv | One line per accepted transaction: number of legs and total amount |
| rejects.csv | Every rejected row: why (`reason_explained`), where it came from (`source_file`, `source_row`) and what it contained (`original_row`) |
| imbalance_report.csv | Transactions whose debits did not equal credits, with the difference |
| raw_records.csv | Every incoming row exactly as received (accepted or not) |
| account_balances.csv | Balance per account (credits minus debits) and whether a customer account is overdrawn |
| pipeline_runs.csv | One line per run: status, row counts, failure reason |
| reconciliation_report.md / .json | Pass/fail of each correctness check and rejects by reason |
| run_summary_<run_id>.json | Full metrics of one run, including timings |

Amounts are normal decimals (e.g. 125.50). DEBIT takes money out of an account, CREDIT puts money in.
"""


def export_outputs(db_path: str, out_dir: str, labels_path: str | None = None) -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    counts = _write_readable_exports(conn, out)
    summary = {
        "rejects_by_reason": {r[0]: r[1] for r in conn.execute(
            "SELECT reason_code, COUNT(*) FROM reject_record GROUP BY 1 ORDER BY 2 DESC, 1")},
        "late_arrival_entries": _one(conn, "SELECT COUNT(*) FROM fact_ledger_entry WHERE is_late_arrival=1"),
        "customer_accounts_overdrawn_now": _one(conn, "SELECT COUNT(*) FROM v_account_balance WHERE account_type='customer' AND balance_minor<0"),
        "ever_overdrawn_customer_accounts": _one(conn, """SELECT COUNT(DISTINCT b.account_id) FROM v_ledger_running_balance b
                                     JOIN dim_account a USING(account_id)
                                     WHERE a.account_type='customer' AND b.running_balance_minor < 0"""),
    }
    conn.close()
    checks = reconcile(db_path)
    report = {"checks": checks, "all_passed": all(c["passed"] for c in checks), "summary": summary,
              "export_row_counts": counts}
    if labels_path:
        report["label_evaluation"] = evaluate_labels(db_path, labels_path)
    with open(out / "reconciliation_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    lines = ["# Reconciliation report", "", f"Overall: **{'PASS' if report['all_passed'] else 'FAIL'}**", "",
             "| check | result | detail |", "|---|---|---|"]
    for c in checks:
        lines.append(f"| {c['check']} | {'PASS' if c['passed'] else 'FAIL'} | {str(c['detail'])[:120]} |")
    lines += ["", "## Rejects by reason", "", "| reason | rows |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in summary["rejects_by_reason"].items()]
    lines += ["", f"Late-arriving entries flagged: {summary['late_arrival_entries']}  ",
              f"Customer accounts currently overdrawn: {summary['customer_accounts_overdrawn_now']}  ",
              f"Customer accounts that were overdrawn at some point: {summary['ever_overdrawn_customer_accounts']}"]
    if "label_evaluation" in report:
        le = report["label_evaluation"]
        lines += ["", "## Ground-truth label check", "",
                  f"{le['rows_labelled']} rows labelled, {le['mismatches']} mismatches."]
    (out / "reconciliation_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report
