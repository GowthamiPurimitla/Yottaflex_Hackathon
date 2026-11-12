"""Ingest -> validate -> quarantine -> atomically publish.

Stages (each timed in the run metrics):
  read      parse CSV, check header against the expected schema
  row       per-row field / reference validation, in-file duplicate handling
  replay    compare against already-curated events (idempotent rerun / conflicting replay)
  txn       per-transaction rules: leg count, leg_seq, event ordering, debit == credit
  publish   one SQLite transaction: dimension upsert, facts, rejects, run status
"""
from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from . import rules as R
from .store import connect

log = logging.getLogger("txnpipe")

CHUNK = 500  # keeps IN (...) lists under SQLite's variable limit


class SchemaError(Exception):
    """Input file does not have the expected structure; the whole file is refused."""


class InjectedFailure(Exception):
    """Raised by the fail_after_rows test hook to simulate a mid-load crash."""


@dataclass
class PipelineConfig:
    legs_path: str
    db_path: str
    accounts_path: Optional[str] = None
    out_dir: Optional[str] = None
    as_of: Optional[datetime] = None            # reference "now" for the future-timestamp rule
    max_future_skew_seconds: int = 300
    force: bool = False                          # reprocess even if identical input already succeeded
    run_id: Optional[str] = None
    fail_after_rows: Optional[int] = None        # test hook: crash mid-publish after N ledger rows


@dataclass
class RunResult:
    run_id: str
    status: str                                  # success | failed | skipped
    metrics: dict
    failure_reason: Optional[str] = None


@dataclass
class Reject:
    entity: str
    line: int
    code: str
    detail: str
    raw: object
    event_id: Optional[str] = None
    txn_id: Optional[str] = None

    def reject_id(self, source_file: str) -> str:
        return hashlib.sha1(f"{self.entity}|{source_file}|{self.line}|{self.code}".encode()).hexdigest()[:16]


# ------------------------------------------------------------------ helpers
def _now() -> datetime:
    return datetime.now(timezone.utc)


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def read_csv(path: str, required: tuple, optional: tuple = ()):
    """Return (header, rows[(line_no, values)]). Empty file -> (None, [])."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        try:
            header = [h.strip().lower() for h in next(reader)]
        except StopIteration:
            return None, []
        missing = [c for c in required if c not in header]
        if missing:
            raise SchemaError(f"{os.path.basename(path)}: missing required columns {missing}; header={header}")
        if len(set(header)) != len(header):
            raise SchemaError(f"{os.path.basename(path)}: duplicate column names in header {header}")
        rows = [(reader.line_num, vals) for vals in reader if vals]
    return header, rows


def _chunks(seq, n=CHUNK):
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _stage(timings: dict, name: str, t_start: float) -> float:
    now = time.perf_counter()
    timings[name] = round(now - t_start, 4)
    return now


# ------------------------------------------------------------------ accounts
def _process_accounts(cfg, conn, run_id):
    """Validate the accounts file; return (effective_accounts, to_upsert, rejects, n_read, src_name)."""
    existing = {r["account_id"]: R.Account(r["account_id"], r["currency"], r["account_type"],
                                           r["opened_at"], r["closed_at"], r["account_name"])
                for r in conn.execute("SELECT * FROM dim_account")}
    effective = dict(existing)
    to_upsert, rejects, n_read, src, raw = [], [], 0, None, []
    if cfg.accounts_path:
        src = os.path.basename(cfg.accounts_path)
        header, rows = read_csv(cfg.accounts_path, R.ACCOUNT_REQUIRED_COLUMNS, R.ACCOUNT_OPTIONAL_COLUMNS)
        raw = [(line, _raw_json(header, vals)) for line, vals in rows]
        seen = set()
        for line, vals in rows:
            n_read += 1
            if len(vals) != len(header):
                rejects.append(Reject("account", line, R.MALFORMED_ROW,
                                      f"expected {len(header)} fields, got {len(vals)}", {"_raw_values": vals}))
                continue
            rec = dict(zip(header, vals))
            try:
                acc = R.validate_account_fields(rec)
                if acc.account_id in seen:
                    raise R.RowError(R.ACCOUNT_DUPLICATE, f"account_id repeated in file: {acc.account_id}")
                prev = effective.get(acc.account_id)
                if prev and prev.currency != acc.currency:
                    raise R.RowError(R.ACCOUNT_CURRENCY_CHANGE,
                                     f"{acc.account_id}: {prev.currency} -> {acc.currency} not allowed")
            except R.RowError as e:
                rejects.append(Reject("account", line, e.code, e.detail, rec))
                continue
            seen.add(acc.account_id)
            if effective.get(acc.account_id) != acc:
                effective[acc.account_id] = acc
                to_upsert.append(acc)
    return effective, to_upsert, rejects, n_read, src, raw


def _raw_json(header, vals) -> str:
    """Original values exactly as read (no trimming/casing); keyed by header when the shape is intact."""
    return json.dumps(dict(zip(header, vals)) if len(vals) == len(header) else {"_raw_values": vals}, sort_keys=True)


def _accounts_fingerprint(accounts: dict) -> str:
    h = hashlib.sha256()
    for k in sorted(accounts):
        h.update(repr(accounts[k]).encode())
    return h.hexdigest()


# ------------------------------------------------------------------ main entry
def run_pipeline(cfg: PipelineConfig) -> RunResult:
    started = _now()
    run_id = cfg.run_id or f"run-{started.strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
    src = os.path.basename(cfg.legs_path)
    conn = connect(cfg.db_path)
    timings: dict = {}
    metrics: dict = {"run_id": run_id, "legs_file": src, "started_at": R.format_ts(started)}
    t_run = time.perf_counter()
    conn.execute(
        "INSERT INTO pipeline_run(run_id,status,started_at,legs_file,accounts_file) VALUES(?,?,?,?,?)",
        (run_id, "running", metrics["started_at"], src,
         os.path.basename(cfg.accounts_path) if cfg.accounts_path else None))
    log.info("[%s] start legs=%s accounts=%s db=%s", run_id, cfg.legs_path, cfg.accounts_path, cfg.db_path)

    try:
        result = _execute(cfg, conn, run_id, src, metrics, timings, t_run, started)
    except Exception as exc:  # any failure => rollback, record, report; never leave partial data
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        reason = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, SchemaError):
            log.error("[%s] FAILED (schema) %s", run_id, reason)
        else:
            log.exception("[%s] FAILED %s", run_id, reason)
        finished = _now()
        metrics.update(status="failed", failure_reason=reason, finished_at=R.format_ts(finished),
                       duration_seconds=round(time.perf_counter() - t_run, 4), timings=timings)
        conn.execute("UPDATE pipeline_run SET status='failed', finished_at=?, failure_reason=?, metrics_json=? "
                     "WHERE run_id=?", (metrics["finished_at"], reason, json.dumps(metrics), run_id))
        result = RunResult(run_id, "failed", metrics, reason)
    finally:
        conn.close()
    _write_summary(cfg, result)
    return result


def _write_summary(cfg, result):
    if not cfg.out_dir:
        return
    Path(cfg.out_dir).mkdir(parents=True, exist_ok=True)
    with open(Path(cfg.out_dir) / f"run_summary_{result.run_id}.json", "w", encoding="utf-8") as f:
        json.dump(result.metrics, f, indent=2, sort_keys=True)


def _execute(cfg, conn, run_id, src, metrics, timings, t_run, started) -> RunResult:
    t = time.perf_counter()
    as_of = cfg.as_of or started
    future_cutoff = R.format_ts(as_of + timedelta(seconds=cfg.max_future_skew_seconds))

    # ---- read ---------------------------------------------------------
    effective, acc_upsert, acc_rejects, acc_read, acc_src, acc_raw = _process_accounts(cfg, conn, run_id)
    header, rows = read_csv(cfg.legs_path, R.LEG_REQUIRED_COLUMNS)
    extra = [c for c in (header or []) if c not in R.LEG_REQUIRED_COLUMNS + R.LEG_OPTIONAL_COLUMNS]
    if extra:
        log.warning("[%s] unexpected extra columns ignored: %s", run_id, extra)
    metrics["unexpected_columns"] = extra
    fingerprint = hashlib.sha256((_sha256_file(cfg.legs_path) + _accounts_fingerprint(effective)).encode()).hexdigest()
    t = _stage(timings, "read", t)
    if header is None:
        log.warning("[%s] input file is empty (0 bytes): treated as an empty batch", run_id)

    # ---- idempotency gate ---------------------------------------------
    prior = conn.execute("SELECT run_id FROM pipeline_run WHERE status='success' AND legs_file=? "
                         "AND input_fingerprint=?", (src, fingerprint)).fetchone()
    if prior and not cfg.force:
        note = f"identical input already processed by {prior['run_id']}; nothing done (use --force to reprocess)"
        log.info("[%s] SKIPPED %s", run_id, note)
        metrics.update(status="skipped", note=note, finished_at=R.format_ts(_now()), rows_read=0,
                       duration_seconds=round(time.perf_counter() - t_run, 4), timings=timings)
        conn.execute("UPDATE pipeline_run SET status='skipped', finished_at=?, input_fingerprint=?, "
                     "metrics_json=? WHERE run_id=?",
                     (metrics["finished_at"], fingerprint, json.dumps(metrics), run_id))
        return RunResult(run_id, "skipped", metrics)

    # ---- row-level validation -----------------------------------------
    rejects: list[Reject] = []
    taint: dict[str, set] = defaultdict(set)   # txn_id -> reason codes of rejected sibling rows
    valid: list[R.Leg] = []
    seen: dict[str, str] = {}                   # event_id -> content_hash of first valid copy
    txn_idx = header.index("txn_id") if header else None
    for line, vals in rows:
        if len(vals) != len(header):
            tid = vals[txn_idx].strip() if len(vals) > txn_idx and vals[txn_idx].strip() else None
            rejects.append(Reject("leg", line, R.MALFORMED_ROW,
                                  f"expected {len(header)} fields, got {len(vals)}", {"_raw_values": vals}, txn_id=tid))
            if tid:
                taint[tid].add(R.MALFORMED_ROW)
            continue
        rec = dict(zip(header, vals))
        eid, tid = (rec.get("event_id") or "").strip() or None, (rec.get("txn_id") or "").strip() or None
        try:
            leg = R.validate_leg_fields(rec, line)
            if leg.event_ts > future_cutoff:
                raise R.RowError(R.FUTURE_TIMESTAMP, f"{leg.event_ts} is after as_of+skew {future_cutoff}")
            acc = effective.get(leg.account_id)
            if acc is None:
                raise R.RowError(R.UNKNOWN_ACCOUNT, f"account_id not in dim_account: {leg.account_id}")
            if acc.currency != leg.currency:
                raise R.RowError(R.ACCOUNT_CURRENCY_MISMATCH,
                                 f"leg {leg.currency} vs account {acc.currency} ({leg.account_id})")
            if leg.event_ts < acc.opened_at or (acc.closed_at and leg.event_ts > acc.closed_at):
                raise R.RowError(R.EVENT_OUTSIDE_ACCOUNT_LIFETIME,
                                 f"{leg.event_ts} outside [{acc.opened_at}, {acc.closed_at}]")
            if leg.event_id in seen:
                if seen[leg.event_id] == leg.content_hash:
                    raise R.RowError(R.DUPLICATE_EVENT, f"event_id already seen in this file: {leg.event_id}")
                raise R.RowError(R.CONFLICTING_DUPLICATE,
                                 f"event_id {leg.event_id} repeated with different content")
        except R.RowError as e:
            rejects.append(Reject("leg", line, e.code, e.detail, rec, eid, tid))
            if tid and e.code != R.DUPLICATE_EVENT:   # an exact duplicate is harmless to its txn
                taint[tid].add(e.code)
            continue
        seen[leg.event_id] = leg.content_hash
        valid.append(leg)
    t = _stage(timings, "row", t)

    # ---- replay check against curated data -----------------------------
    existing_hash: dict[str, str] = {}
    for ch in _chunks({l.event_id for l in valid}):
        q = "SELECT event_id, content_hash FROM fact_ledger_entry WHERE event_id IN (%s)" % ",".join("?" * len(ch))
        existing_hash.update({r[0]: r[1] for r in conn.execute(q, ch)})
    candidates, already_loaded = [], 0
    for leg in valid:
        h = existing_hash.get(leg.event_id)
        if h is None:
            candidates.append(leg)
        elif h == leg.content_hash:
            already_loaded += 1
        else:
            rejects.append(Reject("leg", leg.source_row_number, R.CONFLICTING_REPLAY,
                                  f"event_id {leg.event_id} already curated with different content",
                                  _leg_raw(leg), leg.event_id, leg.txn_id))
    t = _stage(timings, "replay", t)

    # ---- transaction-level rules ---------------------------------------
    by_txn: dict[str, list[R.Leg]] = defaultdict(list)
    for leg in candidates:
        by_txn[leg.txn_id].append(leg)
    published = set()
    for ch in _chunks(by_txn):
        q = "SELECT txn_id FROM fact_transaction WHERE txn_id IN (%s)" % ",".join("?" * len(ch))
        published.update(r[0] for r in conn.execute(q, ch))
    watermark = {r[0]: r[1] for r in conn.execute(
        "SELECT account_id, MAX(event_ts) FROM fact_ledger_entry GROUP BY account_id")}

    good_txns, good_legs, imbalance_rows = [], [], []
    imbalanced = 0
    for tid, legs in by_txn.items():
        code = detail = None
        if tid in published:
            code, detail = R.TXN_ALREADY_PUBLISHED, "txn_id already curated; published transactions are immutable (post a reversal instead)"
        elif tid in taint:
            code, detail = R.TXN_QUARANTINED, f"other rows of this txn were rejected: {sorted(taint[tid])}"
        else:
            legs.sort(key=lambda l: l.leg_seq)
            totals = defaultdict(lambda: [0, 0])  # currency -> [debit, credit]
            for l in legs:
                totals[l.currency][0 if l.direction == "D" else 1] += l.amount_minor
            if len(legs) < 2:
                code, detail = R.TXN_TOO_FEW_LEGS, f"{len(legs)} leg(s); need >= 2"
            elif [l.leg_seq for l in legs] != list(range(1, len(legs) + 1)):
                code, detail = R.TXN_LEG_SEQ_INVALID, f"leg_seq must be 1..n without gaps/repeats: {[l.leg_seq for l in legs]}"
            elif any(a.event_ts > b.event_ts for a, b in zip(legs, legs[1:])):
                code, detail = R.TXN_LEG_ORDER_VIOLATION, "event_ts decreases as leg_seq increases"
            elif any(d != c for d, c in totals.values()):
                code, detail = R.TXN_IMBALANCED, "debits != credits: " + "; ".join(
                    f"{cur} D={d} C={c}" for cur, (d, c) in sorted(totals.items()) if d != c)
                imbalanced += 1
                for cur, (d, c) in sorted(totals.items()):
                    if d != c:
                        imbalance_rows.append((tid, cur, d, c, d - c, run_id, src))
        if code:
            for l in legs:
                rejects.append(Reject("leg", l.source_row_number, code, detail, _leg_raw(l), l.event_id, tid))
        else:
            good_txns.append((tid, len(legs), legs[0].event_ts, max(l.event_ts for l in legs), run_id, src))
            good_legs.extend(legs)
    t = _stage(timings, "txn", t)

    # ---- counts + invariant ---------------------------------------------
    leg_rejects = rejects
    rows_read = len(rows)
    if len(good_legs) + already_loaded + len(leg_rejects) != rows_read:
        raise AssertionError(f"row accounting broken: loaded={len(good_legs)} already={already_loaded} "
                             f"rejected={len(leg_rejects)} read={rows_read}")
    reasons: dict[str, int] = defaultdict(int)
    for r in leg_rejects:
        reasons[r.code] += 1
    quarantined = {r.txn_id for r in leg_rejects if r.txn_id and r.code != R.DUPLICATE_EVENT}
    late = 0
    ledger_rows = []
    for l in good_legs:
        wm = watermark.get(l.account_id)
        is_late = 1 if (wm is not None and l.event_ts < wm) else 0
        late += is_late
        ledger_rows.append((l.event_id, l.txn_id, l.leg_seq, l.account_id, l.direction, l.currency,
                            l.amount_minor, l.signed_minor, l.event_ts, is_late, l.content_hash,
                            run_id, src, l.source_row_number, l.description))

    # ---- publish: one atomic transaction ---------------------------------
    conn.execute("BEGIN")
    conn.executemany(
        "INSERT INTO dim_account(account_id,currency,account_type,opened_at,closed_at,run_id,account_name) "
        "VALUES(?,?,?,?,?,?,?) "
        "ON CONFLICT(account_id) DO UPDATE SET account_type=excluded.account_type, "
        "opened_at=excluded.opened_at, closed_at=excluded.closed_at, run_id=excluded.run_id, "
        "account_name=excluded.account_name",
        [(a.account_id, a.currency, a.account_type, a.opened_at, a.closed_at, run_id, a.account_name)
         for a in acc_upsert])
    # Replace-by-source-file semantics: a re-processed file never leaves stale rejects behind.
    conn.execute("DELETE FROM reject_record WHERE source_file=? AND entity='leg'", (src,))
    conn.execute("DELETE FROM imbalance_report WHERE source_file=?", (src,))
    if acc_src:
        conn.execute("DELETE FROM reject_record WHERE source_file=? AND entity='account'", (acc_src,))
    # Raw landing layer: replace-by-source-file, every row as received (valid or not).
    conn.execute("DELETE FROM raw_record WHERE source_file=? AND entity='leg'", (src,))
    conn.executemany("INSERT INTO raw_record VALUES('leg',?,?,?,?)",
                     [(src, line, run_id, _raw_json(header, vals)) for line, vals in rows])
    if acc_src:
        conn.execute("DELETE FROM raw_record WHERE source_file=? AND entity='account'", (acc_src,))
        conn.executemany("INSERT INTO raw_record VALUES('account',?,?,?,?)",
                         [(acc_src, line, run_id, js) for line, js in acc_raw])
    conn.executemany("INSERT INTO fact_transaction VALUES(?,?,?,?,?,?)", good_txns)
    for i, ch in enumerate(_chunks(ledger_rows, 1000)):
        conn.executemany("INSERT INTO fact_ledger_entry(event_id,txn_id,leg_seq,account_id,direction,currency,"
                         "amount_minor,signed_amount_minor,event_ts,is_late_arrival,content_hash,run_id,"
                         "source_file,source_row_number,description) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ch)
        if cfg.fail_after_rows is not None and (i + 1) * 1000 >= cfg.fail_after_rows:
            raise InjectedFailure(f"simulated crash after {(i + 1) * 1000} ledger rows")
    conn.executemany("INSERT INTO imbalance_report VALUES(?,?,?,?,?,?,?)", imbalance_rows)
    conn.executemany(
        "INSERT OR REPLACE INTO reject_record VALUES(?,?,?,?,?,?,?,?,?,?)",
        [(r.reject_id(src), run_id, src, r.line, "leg", r.event_id, r.txn_id, r.code, r.detail,
          json.dumps(r.raw, sort_keys=True)) for r in leg_rejects] +
        [(r.reject_id(acc_src), run_id, acc_src, r.line, "account", None, None, r.code, r.detail,
          json.dumps(r.raw, sort_keys=True)) for r in acc_rejects])
    t = _stage(timings, "publish", t)

    finished = _now()
    metrics.update(
        status="success", finished_at=R.format_ts(finished), rows_read=rows_read,
        rows_loaded=len(good_legs), rows_already_loaded=already_loaded, rows_rejected=len(leg_rejects),
        txns_loaded=len(good_txns), txns_quarantined=len(quarantined), txns_imbalanced=imbalanced,
        late_arrivals=late, accounts_read=acc_read, accounts_rejected=len(acc_rejects),
        accounts_upserted=len(acc_upsert), rejects_by_reason=dict(sorted(reasons.items())),
        duration_seconds=round(time.perf_counter() - t_run, 4), timings=timings,
        rows_per_second=round(rows_read / max(time.perf_counter() - t_run, 1e-9)))
    conn.execute(
        "UPDATE pipeline_run SET status='success', finished_at=?, input_fingerprint=?, rows_read=?, rows_loaded=?, "
        "rows_already_loaded=?, rows_rejected=?, txns_loaded=?, txns_quarantined=?, accounts_read=?, "
        "accounts_rejected=?, metrics_json=? WHERE run_id=?",
        (metrics["finished_at"], fingerprint, rows_read, len(good_legs), already_loaded, len(leg_rejects),
         len(good_txns), len(quarantined), acc_read, len(acc_rejects), json.dumps(metrics), run_id))
    conn.execute("COMMIT")
    log.info("[%s] SUCCESS read=%d loaded=%d already_loaded=%d rejected=%d txns_loaded=%d imbalanced=%d late=%d in %.2fs",
             run_id, rows_read, len(good_legs), already_loaded, len(leg_rejects), len(good_txns), imbalanced,
             late, metrics["duration_seconds"])
    return RunResult(run_id, "success", metrics)


def _leg_raw(l: R.Leg) -> dict:
    return {"event_id": l.event_id, "txn_id": l.txn_id, "leg_seq": l.leg_seq, "account_id": l.account_id,
            "direction": l.direction, "amount_minor": l.amount_minor, "currency": l.currency,
            "event_ts": l.event_ts}
