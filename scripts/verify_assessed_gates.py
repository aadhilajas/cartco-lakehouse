"""Exercise real reconciliation and GE failures without replacing good output."""
import argparse
from copy import deepcopy
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.assessed_release import publish
from pipeline.output_quality import OutputQualityError
from pipeline.dashboard_snapshot import export


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--published", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    candidate = json.loads(args.candidate.read_text(encoding="utf-8"))
    pointer = (args.published / "CURRENT.json").read_bytes()
    bad = deepcopy(candidate)
    bad["marts"]["daily_revenue"][0]["total_revenue"] = "999.99"
    path = args.output / "bad-reconciliation.json"
    path.write_text(json.dumps(bad), encoding="utf-8")
    try:
        publish(path, args.published)
        raise AssertionError("Reconciliation failure was published")
    except ValueError as exc:
        if "independent reference" not in str(exc): raise
    assert (args.published / "CURRENT.json").read_bytes() == pointer
    # Financial rows remain unchanged; this reaches and fails the real GE suite.
    bad = deepcopy(candidate)
    bad["silver_rows"][0]["source_updated_at"] = None
    path = args.output / "bad-ge-output.json"
    path.write_text(json.dumps(bad), encoding="utf-8")
    try:
        publish(path, args.published)
        raise AssertionError("GE failure was published")
    except OutputQualityError as exc:
        assert not exc.report["success"] and not exc.report["suites"]["silver_state"]["success"]
        (args.output / "actual-ge-failure.json").write_text(json.dumps(exc.report, indent=2), encoding="utf-8")
    assert (args.published / "CURRENT.json").read_bytes() == pointer
    destination = args.output / "public-snapshot.json"
    export(args.published, destination)
    before = destination.read_bytes()
    # A corrupt source pointer is rejected before the exporter opens its target.
    corrupt_root = args.output / "bad-pointer"
    corrupt_root.mkdir(exist_ok=True)
    (corrupt_root / "CURRENT.json").write_text('{"release_id":"../escape"}')
    try:
        export(corrupt_root, destination)
        raise AssertionError("Invalid pointer exported")
    except ValueError:
        pass
    assert destination.read_bytes() == before
    report = {"reconciliation_rejected": True, "actual_ge_failure_rejected": True, "prior_release_pointer_preserved": True, "bad_export_kept_valid_snapshot": True}
    (args.output / "gate-verification.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report))


if __name__ == "__main__": main()
