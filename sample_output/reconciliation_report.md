# Reconciliation report

Overall: **PASS**

| check | result | detail |
|---|---|---|
| curated debits == credits per currency | PASS | balanced |
| every curated transaction balances per currency | PASS | 0 unbalanced |
| ledger.account_id exists in dim_account | PASS | 0 orphans |
| ledger.txn_id exists in fact_transaction | PASS | 0 orphans |
| fact_transaction.leg_count matches ledger | PASS | 0 mismatches |
| leg currency == account currency | PASS | 0 mismatches |
| event_id unique in curated ledger | PASS | 0 duplicates |
| every curated row has run_id/source_file/row | PASS | 0 untraceable |
| every reject has source_file/row/raw_record | PASS | 0 untraceable |
| no source row is both curated and rejected | PASS | 0 overlaps |
| per run: rows_read == loaded + already_loaded + rejected | PASS | all runs reconcile |
| reject_record rows for legs.csv == run run-20261112T082804Z-4150f8 rows_rejected | PASS | table=54 run=54 |
| raw_record rows for legs.csv == run run-20261112T082804Z-4150f8 rows_read | PASS | raw=214 read=214 |
| every curated row traces to a raw_record | PASS | 0 without raw |
| imbalanced transactions absent from curated layer | PASS | 0 leaked |

## Rejects by reason

| reason | rows |
|---|---|
| TXN_QUARANTINED | 21 |
| TXN_IMBALANCED | 6 |
| TXN_LEG_ORDER_VIOLATION | 4 |
| FUTURE_TIMESTAMP | 3 |
| MALFORMED_ROW | 3 |
| MISSING_REQUIRED_FIELD | 3 |
| ACCOUNT_CURRENCY_MISMATCH | 2 |
| DUPLICATE_EVENT | 2 |
| INVALID_TIMESTAMP | 2 |
| TXN_LEG_SEQ_INVALID | 2 |
| TXN_TOO_FEW_LEGS | 2 |
| CONFLICTING_DUPLICATE | 1 |
| INVALID_AMOUNT | 1 |
| INVALID_DIRECTION | 1 |
| UNKNOWN_ACCOUNT | 1 |

Late-arriving entries flagged: 0  
Customer accounts currently overdrawn: 2  
Customer accounts that were overdrawn at some point: 2

## Ground-truth label check

214 rows labelled, 0 mismatches.
