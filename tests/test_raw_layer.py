"""Raw landing layer: every source row is kept exactly as received."""
import json

from conftest import leg, txn

from txnpipe.report import reconcile


def raw(env, entity="leg"):
    return {r["source_row_number"]: json.loads(r["raw_json"])
            for r in env.q("SELECT * FROM raw_record WHERE entity=?", (entity,))}


def test_every_row_stored_valid_or_not_and_unmodified(env):
    messy = leg("e1", "t1", 1, "a_usd_1", "D", " 10.00 ", "USD")      # padding must survive untouched
    rows = [messy, leg("e2", "t1", 2, "a_usd_2", "C", "10.00"), ["short", "row"], leg("e3", "t3", 1, "nope", "D", "1")]
    res = env.run(env.legs(rows))
    r = raw(env)
    assert len(r) == res.metrics["rows_read"] == 4
    assert r[2]["amount"] == " 10.00 "                      # not trimmed/normalised
    assert r[4] == {"_raw_values": ["short", "row"]}        # malformed kept verbatim
    assert env.count("raw_record", "entity='account'") == 8  # accounts file landed too


def test_rerun_and_force_do_not_duplicate_raw(env):
    p = env.legs(txn("t1"))
    env.run(p)
    n = env.count("raw_record")
    env.run(p)
    env.run(p, force=True)
    assert env.count("raw_record") == n


def test_failed_run_leaves_no_raw(env):
    rows = []
    for i in range(1500):
        rows += txn(f"t{i}")
    assert env.run(env.legs(rows), fail_after_rows=1000).status == "failed"
    assert env.count("raw_record") == 0


def test_raw_reconciles(env):
    env.run(env.legs(txn("t1") + [txn("t2")[0]]))
    bad = [c for c in reconcile(env.db) if not c["passed"]]
    assert not bad
    assert any("raw_record" in c["check"] for c in reconcile(env.db))
