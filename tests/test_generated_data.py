"""Ground-truth validation: pipeline outcomes must equal the generator's separate label file."""
import filecmp
from pathlib import Path

import pytest

from txnpipe.generate import generate
from txnpipe.pipeline import PipelineConfig, run_pipeline
from txnpipe.report import evaluate_labels, reconcile

SAMPLE = Path(__file__).resolve().parent.parent / "data" / "sample"


def test_generator_is_deterministic(tmp_path):
    generate(str(tmp_path / "a"), 200, seed=5)
    generate(str(tmp_path / "b"), 200, seed=5)
    generate(str(tmp_path / "c"), 200, seed=6)
    for f in ("legs.csv", "accounts.csv", "expected_labels.csv"):
        assert filecmp.cmp(tmp_path / "a" / f, tmp_path / "b" / f, shallow=False)
    assert not filecmp.cmp(tmp_path / "a" / "legs.csv", tmp_path / "c" / "legs.csv", shallow=False)


def test_labels_are_not_in_input(tmp_path):
    generate(str(tmp_path), 20)
    assert "expected" not in (tmp_path / "legs.csv").read_text().splitlines()[0]


@pytest.mark.parametrize("seed,n,rate", [(1, 400, 0.15), (2, 1500, 0.30), (3, 600, 0.0)])
def test_generated_batches_match_labels_and_reconcile(tmp_path, seed, n, rate):
    g = generate(str(tmp_path / "d"), n, seed=seed, defect_rate=rate)
    d = tmp_path / "d"
    db = str(tmp_path / "x.db")
    res = run_pipeline(PipelineConfig(str(d / "legs.csv"), db, str(d / "accounts.csv")))
    assert res.status == "success"
    assert res.metrics["rows_read"] == g["stats"]["rows"]
    assert res.metrics["rows_loaded"] == g["stats"]["expected_loaded"]
    ev = evaluate_labels(db, str(d / "expected_labels.csv"))
    assert ev["mismatches"] == 0, ev["examples"]
    failed = [c for c in reconcile(db) if not c["passed"]]
    assert not failed, failed
    if rate == 0.0:
        assert res.metrics["rows_rejected"] == 0


def test_committed_sample_matches_labels(tmp_path):
    db = str(tmp_path / "s.db")
    res = run_pipeline(PipelineConfig(str(SAMPLE / "legs.csv"), db, str(SAMPLE / "accounts.csv")))
    assert res.status == "success" and res.metrics["rows_rejected"] > 0
    assert evaluate_labels(db, str(SAMPLE / "expected_labels.csv"))["mismatches"] == 0
