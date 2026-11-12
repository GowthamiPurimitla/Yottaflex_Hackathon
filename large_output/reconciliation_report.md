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
| reject_record rows for legs.csv == run run-20261112T082904Z-6668b6 rows_rejected | PASS | table=6073 run=6073 |
| raw_record rows for legs.csv == run run-20261112T082904Z-6668b6 rows_read | PASS | raw=131423 read=131423 |
| every curated row traces to a raw_record | PASS | 0 without raw |
| imbalanced transactions absent from curated layer | PASS | 0 leaked |

## Rejects by reason

| reason | rows |
|---|---|
| TXN_QUARANTINED | 2179 |
| TXN_IMBALANCED | 966 |
| DUPLICATE_EVENT | 416 |
| TXN_LEG_ORDER_VIOLATION | 331 |
| TXN_LEG_SEQ_INVALID | 297 |
| INVALID_AMOUNT | 292 |
| ACCOUNT_CURRENCY_MISMATCH | 291 |
| UNKNOWN_ACCOUNT | 263 |
| MISSING_REQUIRED_FIELD | 229 |
| CONFLICTING_DUPLICATE | 148 |
| INVALID_TIMESTAMP | 144 |
| INVALID_DIRECTION | 143 |
| TXN_TOO_FEW_LEGS | 138 |
| MALFORMED_ROW | 126 |
| FUTURE_TIMESTAMP | 110 |

Late-arriving entries flagged: 0  
Customer accounts currently overdrawn: 328  
Customer accounts that were overdrawn at some point: 585

## Ground-truth label check

131423 rows labelled, 0 mismatches.
