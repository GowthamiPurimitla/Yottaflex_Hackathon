# Data dictionary

## What each CSV contains

Inputs are written to be readable: customer names, a plain-language `description` on every leg, normal decimal
amounts. (Rows are deliberately shuffled and a few contain deliberate defects; that is the test data.)

| file | direction | contents |
|---|---|---|
| `data/*/accounts.csv` | input | one row per account: id, **name**, currency, customer/internal, open/close dates |
| `data/*/legs.csv` | input | one row per debit/credit leg: ids, account, `D`/`C`, amount, currency, time, **description** |
| `data/*/expected_labels.csv` | answer key | per `legs.csv` line: should it load or be rejected, the reason and a plain-English explanation. Never read by the pipeline |
| `*_output/ledger_entries.csv` | output | accepted legs with account name, DEBIT/CREDIT, decimal amount, description, `arrived_late` |
| `*_output/transactions.csv` | output | one row per accepted transaction: number of legs, total amount |
| `*_output/rejects.csv` | output | every rejected row: `reason_code`, `reason_explained`, `detail`, and `original_row` as readable `field=value` text |
| `*_output/imbalance_report.csv` | output | transactions where debits != credits: total debits, total credits, difference |
| `*_output/raw_records.csv` | output | every incoming row exactly as received, as `field=value` text |
| `*_output/account_balances.csv` | output | balance per account (credits - debits) and `is_overdrawn` |
| `*_output/pipeline_runs.csv` | output | one row per run: status, counts, failure reason |
| `*_output/reconciliation_report.md/.json` | output | pass/fail of every check, rejects by reason |
| `*_output/README.md` | output | one-page guide to the files above (written on every export) |

The database tables keep compact internal forms (integer minor units, JSON raw rows); the CSV exports convert
them to the readable forms above.

## Source: `legs.csv` (grain: one ledger leg = one row; key `event_id`)
| column | type | null | rules |
|---|---|---|---|
| event_id | text | no | unique per leg; same id re-sent with identical content = duplicate, different content = conflict |
| txn_id | text | no | groups legs of one business transaction |
| leg_seq | int ≥1 | no | 1..n, no gaps; `(txn_id, leg_seq)` unique |
| account_id | text | no | must exist in `accounts.csv`/`dim_account` |
| direction | `D`/`C` | no | debit / credit |
| amount | decimal text | no | > 0, ≤ currency minor units (JPY 0 dp, others 2 dp), plain digits only |
| currency | ISO-4217 | no | USD, EUR, GBP, INR, JPY; must equal the account's currency |
| event_ts | ISO-8601 with offset | no | normalised to UTC; ≤ as_of + 300 s; within account lifetime |

Optional column `description` (free text, informational, not part of the duplicate check). Other extra columns are ignored with a warning; any missing required column fails the whole file.

## Source: `accounts.csv` (key `account_id`)
`account_id, currency, account_type (customer|internal), opened_at` required; `closed_at`, `account_name` optional.

## Curated / operational tables (SQLite)
| table | grain / key | notes |
|---|---|---|
| `dim_account` | account_id | currency immutable (`ACCOUNT_CURRENCY_CHANGE` rejected) |
| `fact_transaction` | txn_id | `leg_count`, first/last `event_ts`; only balanced transactions exist |
| `fact_ledger_entry` | event_id (UNIQUE txn_id+leg_seq) | `amount_minor` int > 0; `signed_amount_minor` = credit +, debit −; `is_late_arrival`; `content_hash`; `run_id, source_file, source_row_number`. FKs to account & txn |
| `raw_record` | (entity, source_file, source_row_number) | **raw landing layer**: every incoming row (legs and accounts) as received, values untouched, as JSON; replaced per source file so reruns never duplicate |
| `reject_record` | reject_id = hash(entity, source_file, line, reason) | `reason_code, detail, raw_record` (JSON), `run_id, source_file, source_row_number, entity (leg/account)` |
| `imbalance_report` | (txn_id, currency, source_file) | `debit_minor, credit_minor, diff_minor` |
| `pipeline_run` | run_id | status running/success/failed/skipped, counts, `input_fingerprint`, `failure_reason`, `metrics_json` |
| `v_ledger_running_balance` (view) | event | running balance per account ordered by `(event_ts, event_id)` |
| `v_account_balance` (view) | account_id | net balance (credits − debits) |

## Reason codes
Row: `MALFORMED_ROW, MISSING_REQUIRED_FIELD, INVALID_LEG_SEQ, INVALID_DIRECTION, INVALID_CURRENCY, INVALID_AMOUNT,
INVALID_TIMESTAMP, FUTURE_TIMESTAMP, UNKNOWN_ACCOUNT, ACCOUNT_CURRENCY_MISMATCH, EVENT_OUTSIDE_ACCOUNT_LIFETIME,
DUPLICATE_EVENT, CONFLICTING_DUPLICATE, CONFLICTING_REPLAY`.
Transaction: `TXN_QUARANTINED` (sibling row rejected), `TXN_TOO_FEW_LEGS, TXN_LEG_SEQ_INVALID,
TXN_LEG_ORDER_VIOLATION, TXN_IMBALANCED, TXN_ALREADY_PUBLISHED`.
Account: `ACCOUNT_DUPLICATE, ACCOUNT_CURRENCY_CHANGE, INVALID_ACCOUNT_TYPE` (plus the shared field codes).

## Ground truth (separate from input): `expected_labels.csv`
`source_file, row_number, txn_id, expected_status (loaded|rejected), expected_reason_code, injected_defect`.
