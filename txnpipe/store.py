"""SQLite curated store: DDL and small query helpers."""
from __future__ import annotations

import sqlite3
from pathlib import Path

DDL = """
CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id            TEXT PRIMARY KEY,
    status            TEXT NOT NULL CHECK (status IN ('running','success','failed','skipped')),
    started_at        TEXT NOT NULL,
    finished_at       TEXT,
    legs_file         TEXT NOT NULL,
    accounts_file     TEXT,
    input_fingerprint TEXT,
    rows_read         INTEGER,
    rows_loaded       INTEGER,
    rows_already_loaded INTEGER,
    rows_rejected     INTEGER,
    txns_loaded       INTEGER,
    txns_quarantined  INTEGER,
    accounts_read     INTEGER,
    accounts_rejected INTEGER,
    failure_reason    TEXT,
    metrics_json      TEXT
);

CREATE TABLE IF NOT EXISTS dim_account (
    account_id   TEXT PRIMARY KEY,
    currency     TEXT NOT NULL,
    account_type TEXT NOT NULL CHECK (account_type IN ('customer','internal')),
    opened_at    TEXT NOT NULL,
    closed_at    TEXT,
    run_id       TEXT NOT NULL,
    account_name TEXT
);

CREATE TABLE IF NOT EXISTS fact_transaction (
    txn_id        TEXT PRIMARY KEY,
    leg_count     INTEGER NOT NULL CHECK (leg_count >= 2),
    first_event_ts TEXT NOT NULL,
    last_event_ts  TEXT NOT NULL,
    run_id        TEXT NOT NULL,
    source_file   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_ledger_entry (
    event_id            TEXT PRIMARY KEY,
    txn_id              TEXT NOT NULL REFERENCES fact_transaction(txn_id),
    leg_seq             INTEGER NOT NULL CHECK (leg_seq >= 1),
    account_id          TEXT NOT NULL REFERENCES dim_account(account_id),
    direction           TEXT NOT NULL CHECK (direction IN ('D','C')),
    currency            TEXT NOT NULL,
    amount_minor        INTEGER NOT NULL CHECK (amount_minor > 0),
    signed_amount_minor INTEGER NOT NULL,
    event_ts            TEXT NOT NULL,
    is_late_arrival     INTEGER NOT NULL CHECK (is_late_arrival IN (0,1)),
    content_hash        TEXT NOT NULL,
    run_id              TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_row_number   INTEGER NOT NULL,
    description         TEXT,
    UNIQUE (txn_id, leg_seq)
);
CREATE INDEX IF NOT EXISTS ix_ledger_source_row ON fact_ledger_entry(source_file, source_row_number);
CREATE INDEX IF NOT EXISTS ix_ledger_account_ts ON fact_ledger_entry(account_id, event_ts, event_id);

-- Raw landing layer: every incoming row exactly as received (values untouched, before any validation).
CREATE TABLE IF NOT EXISTS raw_record (
    entity            TEXT NOT NULL CHECK (entity IN ('leg','account')),
    source_file       TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    run_id            TEXT NOT NULL,
    raw_json          TEXT NOT NULL,
    PRIMARY KEY (entity, source_file, source_row_number)
);

CREATE TABLE IF NOT EXISTS reject_record (
    reject_id         TEXT PRIMARY KEY,
    run_id            TEXT NOT NULL,
    source_file       TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    entity            TEXT NOT NULL CHECK (entity IN ('leg','account')),
    event_id          TEXT,
    txn_id            TEXT,
    reason_code       TEXT NOT NULL,
    detail            TEXT,
    raw_record        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_reject_source ON reject_record(source_file, entity);

CREATE TABLE IF NOT EXISTS imbalance_report (
    txn_id       TEXT NOT NULL,
    currency     TEXT NOT NULL,
    debit_minor  INTEGER NOT NULL,
    credit_minor INTEGER NOT NULL,
    diff_minor   INTEGER NOT NULL,
    run_id       TEXT NOT NULL,
    source_file  TEXT NOT NULL,
    PRIMARY KEY (txn_id, currency, source_file)
);

-- Running balance is derived (not stored) so late-arriving data can never leave it stale.
-- Convention: balance = credits - debits (net credit).
CREATE VIEW IF NOT EXISTS v_ledger_running_balance AS
SELECT l.*,
       SUM(signed_amount_minor) OVER (
           PARTITION BY account_id ORDER BY event_ts, event_id
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance_minor
FROM fact_ledger_entry l;

CREATE VIEW IF NOT EXISTS v_account_balance AS
SELECT a.account_id, a.account_type, a.currency,
       COALESCE(SUM(l.signed_amount_minor), 0) AS balance_minor,
       COUNT(l.event_id) AS entry_count
FROM dim_account a LEFT JOIN fact_ledger_entry l USING (account_id)
GROUP BY a.account_id;
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    p = Path(db_path)
    if str(db_path) != ":memory:":
        p.parent.mkdir(parents=True, exist_ok=True)
    # isolation_level=None -> we issue BEGIN/COMMIT explicitly for atomic publishing.
    conn = sqlite3.connect(str(db_path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    if str(db_path) != ":memory:":
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
    conn.executescript(DDL)
    return conn
