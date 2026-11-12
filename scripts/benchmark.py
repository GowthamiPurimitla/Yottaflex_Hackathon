"""Performance measurement: end-to-end throughput + the batching optimisation.

  python scripts/benchmark.py --txns 50000
"""
import argparse
import os
import sqlite3
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from txnpipe.generate import generate  # noqa: E402
from txnpipe.pipeline import PipelineConfig, run_pipeline  # noqa: E402


def insert_benchmark(n=100_000):
    """Same rows, same table: one INSERT+COMMIT per row  vs  executemany in one transaction."""
    rows = [(f"e{i}", f"t{i // 2}", i % 2 + 1, "a", "D", 100, i) for i in range(n)]
    ddl = "CREATE TABLE x(event_id TEXT PRIMARY KEY, txn_id, leg_seq, account_id, direction, amount, n)"
    out = {}
    for mode in ("row_by_row_autocommit", "executemany_single_txn"):
        with tempfile.TemporaryDirectory() as d:
            c = sqlite3.connect(os.path.join(d, "b.db"), isolation_level=None)
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=NORMAL")
            c.execute(ddl)
            sample = rows[:5000] if mode.startswith("row") else rows
            t = time.perf_counter()
            if mode.startswith("row"):
                for r in sample:
                    c.execute("INSERT INTO x VALUES(?,?,?,?,?,?,?)", r)
            else:
                c.execute("BEGIN")
                c.executemany("INSERT INTO x VALUES(?,?,?,?,?,?,?)", sample)
                c.execute("COMMIT")
            dt = time.perf_counter() - t
            out[mode] = len(sample) / dt
            c.close()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--txns", type=int, default=50_000)
    a = ap.parse_args()
    with tempfile.TemporaryDirectory() as d:
        t = time.perf_counter()
        g = generate(d, a.txns, seed=7, defect_rate=0.05, n_customers=1000)
        print(f"generated {g['stats']['rows']:,} rows in {time.perf_counter() - t:.2f}s")
        res = run_pipeline(PipelineConfig(os.path.join(d, "legs.csv"), os.path.join(d, "b.db"),
                                          os.path.join(d, "accounts.csv")))
        m = res.metrics
        print(f"pipeline: {m['rows_read']:,} rows in {m['duration_seconds']}s = {m['rows_per_second']:,} rows/s")
        print("  stage timings (s):", m["timings"])
        print(f"  loaded={m['rows_loaded']:,} rejected={m['rows_rejected']:,}")
        res2 = run_pipeline(PipelineConfig(os.path.join(d, "legs.csv"), os.path.join(d, "b.db"),
                                           os.path.join(d, "accounts.csv"), force=True))
        print(f"forced rerun (all rows already curated): {res2.metrics['duration_seconds']}s")
    r = insert_benchmark()
    print("insert micro-benchmark (rows/s):", {k: round(v) for k, v in r.items()},
          f"-> {r['executemany_single_txn'] / r['row_by_row_autocommit']:.0f}x")


if __name__ == "__main__":
    main()
