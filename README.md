# Double-entry transaction validation pipeline

Ingests ledger **legs** (debit/credit lines) from CSV, validates fields, references, ordering and
debit = credit balance, and publishes only valid transactions to a curated SQLite store. Everything
else goes to a traceable reject/quarantine table. Runs are idempotent, atomic and fully logged.

## Requirements

| Need | Detail |
|---|---|
| Python | **3.10 or newer** (3.12 and 3.14 were used for testing; check with `py --version` on Windows or `python3 --version` on macOS/Linux) |
| Operating system | Developed and tested on **Windows 11**. macOS and Linux should work (standard library only) but were not tested |
| Packages | Only `pytest` (for the tests). The pipeline itself uses only the Python standard library, including the built-in SQLite |
| Internet | Only once, for `pip install` of pytest. Running the pipeline needs no internet |
| Accounts / keys / database server | None. Everything is local files plus a SQLite file the pipeline creates |
| Disk space | About 25 MB for this folder, plus about 160 MB of new files if you run the large dataset (the `out/` folder; delete it afterwards) |

## Quick start (clean environment)

Open a terminal **inside this folder** (the one that contains this README). Every command below is run from here.
Windows uses the `py` launcher; macOS/Linux use `python3` for the first line and `source .venv/bin/activate`
to activate. After the environment is activated, `python` and `pip` work on every system.

```bash
py -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python -m pytest -q
```
Expected: `77 passed` in a few seconds.

**1) Small sample (214 rows, every defect type)**
```bash
python -m txnpipe run --legs data/sample/legs.csv --accounts data/sample/accounts.csv --db out/curated.db --out-dir out
python -m txnpipe report --db out/curated.db --out-dir out --labels data/sample/expected_labels.csv
```
Expected: the run prints `rows_read` 214, `rows_loaded` 160, `rows_rejected` 54, `txns_loaded` 75, `txns_imbalanced` 3.
The report prints 15 lines starting with `PASS` and ends with `labels: 214 rows, 0 mismatches`.
The run takes well under a second. Results are written to the new `out/` folder.

**2) Large dataset (131,423 rows)** - already included in `data/large`
```bash
python -m txnpipe run --legs data/large/legs.csv --accounts data/large/accounts.csv --db out/large.db --out-dir out/large
python -m txnpipe report --db out/large.db --out-dir out/large --labels data/large/expected_labels.csv
```
Expected: `rows_read` 131423, `rows_loaded` 125350, `rows_rejected` 6073, `txns_loaded` 58390, `txns_imbalanced` 440
(10 to 20 seconds). The report again shows all `PASS` and `labels: 131423 rows, 0 mismatches` (about 15 seconds;
slower machines may need up to a minute). Results go to `out/large/`.

**3) Optional: regenerate the data and measure speed**
```bash
python -m txnpipe generate --out-dir data/large --txns 60000 --seed 7 --defect-rate 0.05 --customers 1000
python scripts/benchmark.py --txns 60000
```
The fixed seed regenerates `data/large` with identical content. The benchmark prints rows per second per stage.

**Running the same `run` command twice** reports `"status": "skipped"` the second time - this is the rerun safety
(nothing is loaded twice). Add `--force` to reprocess; rows already loaded are then counted as `already_loaded`.

**Cleaning up:** the `out/` folder and `.venv` are created by the steps above and are safe to delete.

### If something does not work

| Symptom | Fix |
|---|---|
| `python` or `pip` "not recognized" / opens the Microsoft Store | Use `py` instead (e.g. `py -m venv .venv`), then activate the environment so `python` works |
| PowerShell says running scripts is disabled when activating | Use Command Prompt (cmd) instead, or skip activation and run `.venv\Scripts\python.exe -m pytest -q` |
| `No module named pytest` | The environment is not activated, or `pip install -r requirements.txt` was skipped |
| `No module named txnpipe` | You are not in this folder; `cd` into the folder that contains `txnpipe/` and `README.md` |
| Run says `skipped` | Identical input was already processed into that database; use `--force` or a new `--db` path |

Exit codes: `0` success/skipped, `2` run failed (nothing published), `3` reconciliation failed.
Committed example outputs are in [sample_output/](sample_output/) (start with its
[README.md](sample_output/README.md); correctness evidence in
[reconciliation_report.md](sample_output/reconciliation_report.md)). Results of the 131k-row run are in
[large_output/](large_output/). The CSV exports use account names, DEBIT/CREDIT words, decimal amounts and
plain-English reject reasons.

## Flow

```
accounts.csv ─┐                       ┌─► raw_record  (every row as received, unmodified)
              ├─► read+schema check ─►├─► fact_transaction ┐
legs.csv ─────┘   row validation      │                    ├─ one atomic SQLite txn ─► CSV/JSON/MD exports
                                      ├─► fact_ledger_entry┘   + pipeline_run status      + reconciliation
                  replay check        ├─► reject_record   (every rejected row, raw payload + reason)
                  txn-level rules     └─► imbalance_report (per-currency debit/credit/diff)
```

Stages (timed per run): `read → row → replay → txn → publish`. Modules:
[rules.py](txnpipe/rules.py) pure validators · [pipeline.py](txnpipe/pipeline.py) orchestration ·
[store.py](txnpipe/store.py) DDL · [generate.py](txnpipe/generate.py) synthetic data ·
[report.py](txnpipe/report.py) reconciliation/exports · [cli.py](txnpipe/cli.py).

## Business rules

Row level (first failure wins, one reason code per row): malformed column count; null/blank required
field; `leg_seq` integer ≥ 1; `direction` ∈ {D,C}; currency in {USD,EUR,GBP,INR,JPY}; amount strictly
positive plain decimal within the currency's minor units (JPY 0 dp, others 2 dp); timestamp ISO-8601
**with** offset (stored as UTC); not later than `as_of` + 300 s; `account_id` exists (referential
integrity); leg currency = account currency; event inside the account's `opened_at..closed_at`;
duplicate `event_id` in the file.

Transaction level (a transaction is all-or-nothing): `txn_id` not already published; no sibling row
rejected (`TXN_QUARANTINED`); ≥ 2 legs; `leg_seq` exactly 1..n; `event_ts` non-decreasing in `leg_seq`
(event ordering); **debits = credits separately for each currency** (`TXN_IMBALANCED`, recorded in
`imbalance_report`).

Every incoming row (valid or not) is also landed unmodified in `raw_record`, so any curated or rejected row
can be traced back to exactly what the source sent. What each input/output CSV contains is listed in the
[data dictionary](docs/data_dictionary.md#what-each-csv-contains).

Full reason-code list: [docs/data_dictionary.md](docs/data_dictionary.md).

## Idempotency, restart, backfill

- **Rerun of identical input** → run is `skipped` (key = legs file name + sha256 of file + fingerprint of the
  effective accounts dimension). `--force` reprocesses; already-curated events are recognised by
  `event_id` + `content_hash` and counted as `rows_already_loaded`, so no duplicates are possible
  (also enforced by PK/UNIQUE constraints).
- **Atomic publish**: dimension upserts, facts, rejects and the run's `success` status commit in one SQLite
  transaction. A crash/exception rolls back everything; the run is recorded `failed` with `failure_reason`.
  **Retry** = simply rerun the same command (failed runs never block). Tested with an injected mid-load crash.
- **Corrected file, same name** → rejects/imbalances for that `source_file` are replaced (no stale rows);
  events already curated are untouched; previously rejected rows now load.
- **Reference data arrives late** (rows rejected as `UNKNOWN_ACCOUNT`): load the accounts file with the same
  legs file; the changed accounts fingerprint triggers reprocessing.
- **Backfill / late data**: files can be loaded in any order. An event older than the account's latest
  curated event is accepted and flagged `is_late_arrival=1`; running balances live in a view
  (`v_ledger_running_balance`) so they can never go stale. Tested: order of batches does not change balances.
- **History**: published transactions are immutable (`TXN_ALREADY_PUBLISHED`; a conflicting re-send is
  `CONFLICTING_REPLAY`). Corrections are new reversal transactions. Rejected rows keep their raw payload.

## Observability

Structured logs (`run_id` on every line) plus a row in `pipeline_run` and `out/run_summary_<run_id>.json`:
start/end, duration, rows read/loaded/already-loaded/rejected, txns loaded/quarantined/imbalanced, late arrivals,
rejects by reason, per-stage timings, rows/s, failure reason. Invariant enforced in code and re-checked in
reconciliation: `rows_read = loaded + already_loaded + rejected`.

## Correctness evidence

- **77 automated tests** (full run log: [docs/test_run_evidence.txt](docs/test_run_evidence.txt)): happy path, empty (header-only & zero-byte), malformed rows, null fields, schema change
  (missing column → whole file refused; extra/reordered columns tolerated), exact & conflicting duplicates,
  duplicate leg_seq, imbalance, multi-currency, single-leg, bad reference key, currency mismatch, account lifetime,
  future timestamp, leg-order violation, out-of-order rows, late arrival, backfill order independence, rerun,
  forced rerun, partial failure + retry, corrected-file reprocess, conflicting replay, immutability, DB constraints.
- **Ground-truth labels** (`expected_labels.csv`, never read by the pipeline) are compared per source row:
  0 mismatches on the sample and on generated batches of up to 1,500 transactions (also in tests).
- **Reconciliation** ([report.py](txnpipe/report.py)): global & per-transaction debit=credit, FK checks, leg count,
  currency agreement, uniqueness, traceability, source→target row accounting, no row both curated and rejected.

Sample queries:

```sql
SELECT reason_code, COUNT(*) FROM reject_record GROUP BY 1 ORDER BY 2 DESC;      -- why rows were rejected
SELECT * FROM imbalance_report;                                                 -- flagged imbalances
SELECT account_id, balance_minor FROM v_account_balance WHERE balance_minor < 0 AND account_type='customer';
SELECT * FROM reject_record WHERE txn_id='txn_00000012';                        -- trace a transaction back to source rows
```

## Performance

Measured on a Windows laptop, ~131k rows (60k transactions, 5% defect rate), `scripts/benchmark.py`, after the raw layer was added:

| stage | seconds |
|---|---|
| read | 0.38 |
| row validation | 2.15 |
| replay check | 0.16 |
| txn rules | 0.35 |
| publish (incl. raw landing layer) | 4.40 |
| **total** | **7.4 s ≈ 17,700 rows/s** |

Timings vary with machine load (the same run took 14.5 s while other jobs were running). A forced rerun of the
same file takes 6.6 s with 0 new rows loaded.

**Optimization applied:** publishing uses a single transaction with `executemany` batches instead of per-row
autocommit inserts: 374k vs 18k rows/s in the micro-benchmark (~21×). Duplicate/replay/reference checks use
in-memory dict/sets and chunked `IN` lookups rather than a query per row. **Next:** streaming in chunks to bound
memory, drop the `(account_id,event_ts)` index during bulk load, or move to DuckDB/Parquet.

## Assumptions & naming

snake_case tables/columns; deterministic IDs (`txn_00000012`, `evt_00000012_1`, reject_id = hash of
entity|source_file|line|reason); ISO-8601 UTC timestamps (microsecond precision so strings sort correctly);
money as integer minor units; every curated and reject row carries `run_id`, `source_file`, `source_row_number`
(physical line number, header = 1). Balance convention: credit − debit. Assumptions: all legs of a transaction
arrive in the same file; `source_file` = file name, so distinct batches need distinct names; overdrafts are
informational only (reported, not rejected).

## Known limitations / next steps

Single-writer SQLite (no concurrent runs); whole file held in memory; one reason code per row (first failure);
no FX conversion (multi-currency transactions must balance per currency); account history is current-state upsert
(no SCD2); exact-duplicate detection across files relies on `event_id`. Next: chunked streaming, SCD2 account
history, Parquet/DuckDB target, orchestration (cron/Airflow), alert on failed reconciliation.

Fee and business-meaning rules (for example the fee amount, or that a fee must go to a fee account) are not
validated. The pipeline only guarantees that debits equal credits per currency; the `description` column is
informational and is not used by any rule. Next step: add fee rules once the business defines them.

See also [docs/DESIGN.md](docs/DESIGN.md) (trade-offs) and [docs/data_dictionary.md](docs/data_dictionary.md).
