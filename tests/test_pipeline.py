"""End-to-end behaviour of the pipeline against hand-built inputs."""
import os

from conftest import LEG_HDR, leg, txn, write_csv



# ------------------------------------------------------------ happy path / empty
def test_happy_path(env):
    res = env.run(env.legs(txn("t1") + txn("t2", "5.25")))
    m = res.metrics
    assert res.status == "success"
    assert (m["rows_read"], m["rows_loaded"], m["rows_rejected"], m["txns_loaded"]) == (4, 4, 0, 2)
    assert env.count("fact_ledger_entry") == 4 and env.count("fact_transaction") == 2
    bal = {r["account_id"]: r["balance_minor"] for r in env.q("SELECT * FROM v_account_balance")}
    assert bal["a_usd_1"] == -1525 and bal["a_usd_2"] == 1525
    run = env.q("SELECT * FROM pipeline_run")[0]
    assert run["status"] == "success" and run["finished_at"] and run["rows_read"] == 4


def test_empty_batch_header_only(env):
    res = env.run(env.legs([]))
    assert res.status == "success" and res.metrics["rows_read"] == 0 and env.count("fact_ledger_entry") == 0


def test_empty_batch_zero_bytes(env):
    p = env.tmp / "empty.csv"
    p.write_bytes(b"")
    res = env.run(str(p))
    assert res.status == "success" and res.metrics["rows_read"] == 0


# ------------------------------------------------------------ malformed / nulls / schema
def test_malformed_row_quarantines_its_txn(env):
    rows = txn("t1")
    rows[1] = rows[1][:5]  # truncated, txn_id intact
    res = env.run(env.legs(rows + txn("t2")))
    assert res.metrics["rows_loaded"] == 2
    assert env.reasons() == {2: "TXN_QUARANTINED", 3: "MALFORMED_ROW"}
    rec = env.q("SELECT * FROM reject_record WHERE reason_code='MALFORMED_ROW'")[0]
    assert rec["txn_id"] == "t1" and "_raw_values" in rec["raw_record"]


def test_extra_fields_malformed(env):
    rows = txn("t1")
    rows[0] = rows[0] + ["surprise"]
    env.run(env.legs(rows))
    assert env.reasons()[2] == "MALFORMED_ROW"


def test_null_required_field(env):
    rows = txn("t1")
    rows[0][3] = ""
    env.run(env.legs(rows))
    assert env.reasons() == {2: "MISSING_REQUIRED_FIELD", 3: "TXN_QUARANTINED"}
    assert env.count("fact_ledger_entry") == 0


def test_schema_change_missing_column_fails_whole_file(env):
    hdr = [c for c in LEG_HDR if c != "currency"]
    p = write_csv(env.tmp / "bad.csv", hdr, [[r[i] for i, c in enumerate(LEG_HDR) if c != "currency"] for r in txn("t1")])
    res = env.run(p)
    assert res.status == "failed" and "missing required columns" in res.failure_reason
    assert env.count("fact_ledger_entry") == 0
    run = env.q("SELECT * FROM pipeline_run")[0]
    assert run["status"] == "failed" and "currency" in run["failure_reason"]


def test_schema_change_extra_column_tolerated_reordered_ok(env):
    hdr = ["extra"] + list(reversed(LEG_HDR))
    rows = [["x"] + list(reversed(r)) for r in txn("t1")]
    res = env.run(write_csv(env.tmp / "wide.csv", hdr, rows))
    assert res.status == "success" and res.metrics["rows_loaded"] == 2
    assert res.metrics["unexpected_columns"] == ["extra"]


def test_unreadable_encoding_fails_cleanly(env):
    p = env.tmp / "bin.csv"
    p.write_bytes(b"\xff\xfe\x00\x01garbage\n\x80\x81")
    res = env.run(str(p))
    assert res.status == "failed" and env.count("fact_ledger_entry") == 0


# ------------------------------------------------------------ duplicates
def test_exact_duplicate_event_rejected_once_txn_still_loads(env):
    rows = txn("t1")
    res = env.run(env.legs(rows + [rows[0]]))
    assert res.metrics["rows_loaded"] == 2
    assert env.reasons() == {4: "DUPLICATE_EVENT"}
    assert env.count("fact_ledger_entry") == 2


def test_conflicting_duplicate_quarantines_txn(env):
    rows = txn("t1")
    conflict = list(rows[0])
    conflict[5] = "99.00"
    env.run(env.legs(rows + [conflict]))
    assert env.reasons() == {2: "TXN_QUARANTINED", 3: "TXN_QUARANTINED", 4: "CONFLICTING_DUPLICATE"}
    assert env.count("fact_ledger_entry") == 0


def test_duplicate_leg_seq_different_event_ids(env):
    rows = [leg("e1", "t1", 1, "a_usd_1", "D", "5.00"), leg("e2", "t1", 1, "a_usd_2", "C", "5.00")]
    env.run(env.legs(rows))
    assert set(env.reasons().values()) == {"TXN_LEG_SEQ_INVALID"}


# ------------------------------------------------------------ domain rules
def test_imbalance_flagged_and_never_curated(env):
    bad = [leg("e1", "t1", 1, "a_usd_1", "D", "10.00"), leg("e2", "t1", 2, "a_usd_2", "C", "9.99")]
    res = env.run(env.legs(bad + txn("t2")))
    assert res.metrics["txns_imbalanced"] == 1 and res.metrics["txns_loaded"] == 1
    imb = env.q("SELECT * FROM imbalance_report")
    assert len(imb) == 1 and imb[0]["txn_id"] == "t1" and imb[0]["diff_minor"] == 1
    assert env.count("fact_ledger_entry", "txn_id='t1'") == 0
    assert set(env.reasons().values()) == {"TXN_IMBALANCED"}


def test_multi_currency_must_balance_per_currency(env):
    # 10 USD debit vs 10 EUR credit nominally "equal" but is NOT balanced
    rows = [leg("e1", "t1", 1, "a_usd_1", "D", "10.00", "USD"), leg("e2", "t1", 2, "a_eur_1", "C", "10.00", "EUR")]
    env.run(env.legs(rows))
    assert set(env.reasons().values()) == {"TXN_IMBALANCED"} and env.count("imbalance_report") == 2


def test_multi_currency_balanced_pairs_load(env):
    rows = [leg("e1", "t1", 1, "a_usd_1", "D", "10.00", "USD"), leg("e2", "t1", 2, "a_usd_int", "C", "10.00", "USD"),
            leg("e3", "t1", 3, "a_eur_int", "D", "9.00", "EUR"), leg("e4", "t1", 4, "a_eur_1", "C", "9.00", "EUR")]
    assert env.run(env.legs(rows)).metrics["txns_loaded"] == 1


def test_single_leg_txn(env):
    env.run(env.legs([leg("e1", "t1", 1, "a_usd_1", "D", "1.00")]))
    assert env.reasons() == {2: "TXN_TOO_FEW_LEGS"}


def test_invalid_reference_key_and_currency_mismatch(env):
    r1 = [leg("e1", "t1", 1, "nope", "D", "1.00"), leg("e2", "t1", 2, "a_usd_2", "C", "1.00")]
    r2 = [leg("e3", "t2", 1, "a_usd_1", "D", "1.00", "EUR"), leg("e4", "t2", 2, "a_eur_1", "C", "1.00", "EUR")]
    env.run(env.legs(r1 + r2))
    assert env.reasons() == {2: "UNKNOWN_ACCOUNT", 3: "TXN_QUARANTINED",
                             4: "ACCOUNT_CURRENCY_MISMATCH", 5: "TXN_QUARANTINED"}


def test_account_lifetime_rule(env):
    rows = txn("t1", a="a_closed", b="a_usd_2", ts="2025-06-01T00:00:00Z")
    env.run(env.legs(rows))
    assert env.reasons()[2] == "EVENT_OUTSIDE_ACCOUNT_LIFETIME"


def test_future_timestamp_uses_as_of(env):
    from datetime import datetime, timezone
    rows = txn("t1", ts="2025-03-01T10:10:00Z")
    res = env.run(env.legs(rows), as_of=datetime(2025, 3, 1, 10, 0, tzinfo=timezone.utc))
    assert set(env.reasons().values()) == {"FUTURE_TIMESTAMP"}
    # within skew (5 min) is accepted
    rows = txn("t2", ts="2025-03-01T10:04:00Z")
    res = env.run(env.legs(rows, "l2.csv"), as_of=datetime(2025, 3, 1, 10, 0, tzinfo=timezone.utc))
    assert res.metrics["rows_loaded"] == 2


def test_leg_event_order_violation(env):
    rows = [leg("e1", "t1", 1, "a_usd_1", "D", "1.00", ts="2025-03-01T10:00:05Z"),
            leg("e2", "t1", 2, "a_usd_2", "C", "1.00", ts="2025-03-01T10:00:00Z")]
    env.run(env.legs(rows))
    assert set(env.reasons().values()) == {"TXN_LEG_ORDER_VIOLATION"}


def test_legs_arriving_out_of_file_order_still_load(env):
    rows = txn("t1") + txn("t2", ts="2025-03-01T09:00:00Z")
    rows.reverse()  # leg 2 before leg 1, later txn before earlier
    res = env.run(env.legs(rows))
    assert res.metrics["rows_loaded"] == 4


# ------------------------------------------------------------ late data / history
def test_late_arriving_event_is_flagged_and_balance_is_order_correct(env):
    env.run(env.legs(txn("t_new", "10.00", ts="2025-03-02T00:00:00Z"), "b1.csv"))
    res = env.run(env.legs(txn("t_old", "4.00", ts="2025-03-01T00:00:00Z"), "b2.csv"))
    assert res.metrics["late_arrivals"] == 2
    late = env.q("SELECT event_id FROM fact_ledger_entry WHERE is_late_arrival=1 ORDER BY 1")
    assert [r["event_id"] for r in late] == ["t_old_1", "t_old_2"]
    rb = env.q("SELECT event_id, running_balance_minor FROM v_ledger_running_balance "
               "WHERE account_id='a_usd_2' ORDER BY event_ts")
    assert [r["running_balance_minor"] for r in rb] == [400, 1400]  # old (late) event slots in before the new one


def test_backfill_order_independent(env, tmp_path):
    from conftest import Env
    a = txn("t1", ts="2025-03-01T00:00:00Z")
    b = txn("t2", "7.00", ts="2025-03-05T00:00:00Z")
    env.run(env.legs(a, "a.csv"))
    env.run(env.legs(b, "b.csv"))
    os.makedirs(tmp_path / "x", exist_ok=True)
    other = Env(tmp_path / "x")
    other.run(other.legs(b, "b.csv"))
    other.run(other.legs(a, "a.csv"))
    q = "SELECT account_id, balance_minor FROM v_account_balance ORDER BY 1"
    assert env.q(q) == other.q(q)


# ------------------------------------------------------------ idempotency / recovery
def test_rerun_same_input_is_skipped_and_creates_no_duplicates(env):
    p = env.legs(txn("t1") + [txn("t2")[0]])  # includes an imbalanced txn too
    first = env.run(p)
    before = (env.count("fact_ledger_entry"), env.count("reject_record"))
    second = env.run(p)
    assert first.status == "success" and second.status == "skipped"
    assert (env.count("fact_ledger_entry"), env.count("reject_record")) == before
    assert env.count("pipeline_run", "status='skipped'") == 1


def test_forced_rerun_is_idempotent(env):
    p = env.legs(txn("t1") + [txn("t2")[0]])
    env.run(p)
    before = (env.count("fact_ledger_entry"), env.count("reject_record"), env.count("imbalance_report"))
    res = env.run(p, force=True)
    assert res.status == "success" and res.metrics["rows_loaded"] == 0 and res.metrics["rows_already_loaded"] == 2
    assert (env.count("fact_ledger_entry"), env.count("reject_record"), env.count("imbalance_report")) == before


def test_partial_failure_rolls_back_then_retry_succeeds(env):
    rows = []
    for i in range(1500):
        rows += txn(f"t{i}")
    p = env.legs(rows)
    bad = env.run(p, fail_after_rows=1000)
    assert bad.status == "failed" and "simulated crash" in bad.failure_reason
    assert env.count("fact_ledger_entry") == 0 and env.count("fact_transaction") == 0  # all-or-nothing
    assert env.q("SELECT status FROM pipeline_run")[0]["status"] == "failed"
    good = env.run(p)  # retry: failed runs never block a rerun
    assert good.status == "success" and good.metrics["rows_loaded"] == 3000
    assert env.count("fact_ledger_entry") == 3000


def test_corrected_file_reprocess_replaces_old_rejects(env):
    bad = [leg("e1", "t1", 1, "a_usd_1", "D", "10.00"), leg("e2", "t1", 2, "a_usd_2", "C", "9.00")]
    env.run(env.legs(bad, "same_name.csv"))
    assert env.count("reject_record") == 2
    fixed = [leg("e1", "t1", 1, "a_usd_1", "D", "10.00"), leg("e2", "t1", 2, "a_usd_2", "C", "10.00")]
    res = env.run(env.legs(fixed, "same_name.csv"))
    assert res.metrics["rows_loaded"] == 2
    assert env.count("reject_record") == 0 and env.count("imbalance_report") == 0


def test_conflicting_replay_across_runs(env):
    env.run(env.legs(txn("t1"), "a.csv"))
    changed = txn("t1")
    changed[0][5] = "11.00"
    env.run(env.legs(changed, "b.csv"))
    r = env.reasons()
    assert r[2] == "CONFLICTING_REPLAY" and env.count("fact_ledger_entry") == 2


def test_published_transaction_is_immutable(env):
    env.run(env.legs(txn("t1"), "a.csv"))
    extra = txn("t1") + [leg("t1_3", "t1", 3, "a_usd_1", "D", "1.00")]
    res = env.run(env.legs(extra, "b.csv"))
    assert res.metrics["rows_already_loaded"] == 2
    assert env.reasons() == {4: "TXN_ALREADY_PUBLISHED"}


def test_reference_data_catching_up_requires_new_fingerprint(env):
    """Rows rejected for UNKNOWN_ACCOUNT load once the account arrives (accounts change the fingerprint)."""
    rows = txn("t1", a="a_late", b="a_usd_2")
    p = env.legs(rows)
    assert env.run(p).metrics["rows_loaded"] == 0
    accs = write_csv(env.tmp / "accounts2.csv", ["account_id", "currency", "account_type", "opened_at", "closed_at"],
                     [["a_late", "USD", "customer", "2024-01-01T00:00:00Z", ""]])
    from txnpipe.pipeline import PipelineConfig, run_pipeline
    res = run_pipeline(PipelineConfig(p, env.db, accs))
    assert res.status == "success" and res.metrics["rows_loaded"] == 2


def test_account_file_validation(env):
    accs = write_csv(env.tmp / "acc.csv", ["account_id", "currency", "account_type", "opened_at", "closed_at"],
                     [["a_usd_1", "EUR", "customer", "2024-01-01T00:00:00Z", ""],   # currency change
                      ["new1", "USD", "customer", "2024-01-01T00:00:00Z", ""],
                      ["new1", "USD", "customer", "2024-01-01T00:00:00Z", ""],       # duplicate
                      ["new2", "ZZZ", "customer", "2024-01-01T00:00:00Z", ""],       # bad currency
                      ["", "USD", "customer", "2024-01-01T00:00:00Z", ""]])          # null key
    env.run(env.legs([]), accounts=True)
    from txnpipe.pipeline import PipelineConfig, run_pipeline
    res = run_pipeline(PipelineConfig(env.legs([], "l2.csv"), env.db, accs))
    assert res.metrics["accounts_rejected"] == 4
    codes = {r["reason_code"] for r in env.q("SELECT * FROM reject_record WHERE entity='account'")}
    assert codes == {"ACCOUNT_CURRENCY_CHANGE", "ACCOUNT_DUPLICATE", "INVALID_CURRENCY", "MISSING_REQUIRED_FIELD"}
    assert env.q("SELECT currency FROM dim_account WHERE account_id='a_usd_1'")[0]["currency"] == "USD"


def test_database_constraints_are_last_line_of_defence(env):
    import sqlite3
    import pytest
    env.run(env.legs(txn("t1")))
    from txnpipe.store import connect
    c = connect(env.db)
    with pytest.raises(sqlite3.IntegrityError):
        c.execute("INSERT INTO fact_ledger_entry(event_id,txn_id,leg_seq,account_id,direction,currency,amount_minor,signed_amount_minor,event_ts,is_late_arrival,content_hash,run_id,source_file,source_row_number) VALUES('x','t1',9,'ghost','D','USD',5,-5,'2025','0','h','r','f',1)")
    with pytest.raises(sqlite3.IntegrityError):
        c.execute("INSERT INTO fact_ledger_entry(event_id,txn_id,leg_seq,account_id,direction,currency,amount_minor,signed_amount_minor,event_ts,is_late_arrival,content_hash,run_id,source_file,source_row_number) VALUES('x','t1',9,'a_usd_1','D','USD',-5,5,'2025',0,'h','r','f',1)")
    c.close()
