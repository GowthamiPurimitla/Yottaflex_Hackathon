# What is in this folder

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
