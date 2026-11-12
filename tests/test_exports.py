"""Exported CSVs must be understandable without knowing the schema."""
import csv

from conftest import LEG_HDR, leg, write_csv

from txnpipe.generate import generate
from txnpipe.pipeline import PipelineConfig, run_pipeline
from txnpipe.report import export_outputs


def rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_exports_are_human_readable(tmp_path):
    d = tmp_path / "d"
    generate(str(d), 80, seed=15, defect_rate=0.4)
    db = str(tmp_path / "x.db")
    run_pipeline(PipelineConfig(str(d / "legs.csv"), db, str(d / "accounts.csv")))
    out = tmp_path / "out"
    export_outputs(db, str(out))

    led = rows(out / "ledger_entries.csv")
    assert led and "amount_minor" not in led[0] and "content_hash" not in led[0]
    assert {r["direction"] for r in led} == {"DEBIT", "CREDIT"}
    assert all(r["account_name"] and r["description"] for r in led)
    assert all("." not in r["event_time"] for r in led)            # no microsecond noise
    assert any(r["amount"].count(".") == 1 and len(r["amount"].split(".")[1]) == 2 for r in led)

    rej = rows(out / "rejects.csv")
    assert rej and all(r["reason_explained"] for r in rej)
    assert all(not r["original_row"].startswith("{") for r in rej)  # no JSON blobs
    assert any("event_id=" in r["original_row"] for r in rej)

    assert all(r["total_amount"] for r in rows(out / "transactions.csv"))
    assert (out / "README.md").exists()
    assert {"account_name", "balance", "is_overdrawn"} <= set(rows(out / "account_balances.csv")[0])


def test_optional_columns_accepted_and_stored(env):
    hdr = LEG_HDR + ["description"]
    r1 = leg("e1", "t1", 1, "a_usd_1", "D", "5.00") + ["Coffee"]
    r2 = leg("e2", "t1", 2, "a_usd_2", "C", "5.00") + ["Coffee income"]
    res = env.run(write_csv(env.tmp / "d.csv", hdr, [r1, r2]))
    assert res.status == "success" and res.metrics["unexpected_columns"] == []
    assert {r["description"] for r in env.q("SELECT description FROM fact_ledger_entry")} == {"Coffee", "Coffee income"}
