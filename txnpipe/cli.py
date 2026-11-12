"""Command line: python -m txnpipe {generate,run,report}"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime

from .generate import generate
from .pipeline import PipelineConfig, run_pipeline
from .report import export_outputs


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="txnpipe", description="Double-entry transaction validation pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="write synthetic accounts.csv, legs.csv and expected_labels.csv")
    g.add_argument("--out-dir", required=True)
    g.add_argument("--txns", type=int, default=40)
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--defect-rate", type=float, default=0.12)
    g.add_argument("--customers", type=int, default=20)

    r = sub.add_parser("run", help="ingest + validate + publish one legs file")
    r.add_argument("--legs", required=True)
    r.add_argument("--accounts")
    r.add_argument("--db", default="out/curated.db")
    r.add_argument("--out-dir", default="out")
    r.add_argument("--as-of", help="ISO-8601 reference time for the future-timestamp rule (default: now)")
    r.add_argument("--force", action="store_true", help="reprocess even if identical input already succeeded")

    e = sub.add_parser("report", help="reconciliation + CSV/JSON/MD exports")
    e.add_argument("--db", default="out/curated.db")
    e.add_argument("--out-dir", default="out")
    e.add_argument("--labels", help="expected_labels.csv from the generator (optional)")

    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if args.cmd == "generate":
        res = generate(args.out_dir, args.txns, args.seed, args.defect_rate, args.customers)
        print(json.dumps(res["stats"], indent=2))
        return 0
    if args.cmd == "run":
        as_of = datetime.fromisoformat(args.as_of.replace("Z", "+00:00")) if args.as_of else None
        res = run_pipeline(PipelineConfig(args.legs, args.db, args.accounts, args.out_dir, as_of, force=args.force))
        print(json.dumps({k: v for k, v in res.metrics.items() if k != "timings"}, indent=2))
        return 0 if res.status in ("success", "skipped") else 2
    rep = export_outputs(args.db, args.out_dir, args.labels)
    for c in rep["checks"]:
        print(("PASS " if c["passed"] else "FAIL ") + c["check"])
    if "label_evaluation" in rep:
        print("labels:", rep["label_evaluation"]["rows_labelled"], "rows,", rep["label_evaluation"]["mismatches"], "mismatches")
    return 0 if rep["all_passed"] else 3


if __name__ == "__main__":
    sys.exit(main())
