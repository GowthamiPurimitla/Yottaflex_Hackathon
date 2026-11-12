# Design & trade-offs

| decision | why | cost |
|---|---|---|
| SQLite as curated store | zero setup, real constraints (PK/FK/CHECK) as a last line of defence, window functions, atomic transactions | single writer, not for huge volume |
| Transaction (not row) as the unit of acceptance | a lone valid leg of a broken transaction would corrupt balances; siblings of any rejected row are quarantined (`TXN_QUARANTINED`) | one bad leg rejects good rows (still traceable, replayable) |
| Integer minor units + strict decimal regex | no float error; rejects `1e3`, `NaN`, `1,000` | currencies need an exponent table |
| Balance rule per currency | multi-currency "net zero" would hide real imbalances; FX must go through clearing accounts | no implicit FX |
| Running balance as a view | late/out-of-order data cannot leave stored balances stale | recomputed on read |
| Published transactions immutable | audit trail; fixes are reversals; replays are detectable via `content_hash` | no in-place correction |
| Input fingerprint skip + `--force` | cheap idempotent reruns; reference-data changes still reprocess | file-name based identity |
| One atomic publish incl. run status | no partial loads; failure leaves DB untouched | whole batch in memory / one transaction |
| Replace-by-source-file rejects | a corrected resend leaves no stale rejects | reject history lives in `pipeline_run.metrics_json`, not the table |
| Ground-truth labels in a separate file | tests validate the pipeline without leaking answers into input | labels must be maintained with the generator |

**Event ordering** is handled three ways: legs inside a transaction must be time-ordered by `leg_seq`;
file order is irrelevant (everything is keyed and sorted by `event_ts`); events older than an account's
watermark are accepted but flagged late, never silently re-ordered into stale balances.

**Limitations:** see README. Not implemented: concurrent runs/locking, streaming, SCD2 accounts, FX, a full
schema-evolution framework (strategy: additive columns tolerated, missing required columns refuse the file).
