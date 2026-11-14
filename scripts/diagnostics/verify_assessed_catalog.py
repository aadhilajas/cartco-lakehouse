"""Read-only proof that a published release still resolves to its exact tables."""

# Support direct CLI execution and imports from any working directory.
import sys as _sys
from pathlib import Path as _Path
_SCRIPT_PATH = _Path(__file__).resolve()
_REPOSITORY_ROOT = next(
    (parent for parent in _SCRIPT_PATH.parents
     if (parent / "pipeline/source_identity.py").is_file()
     and (parent / "scripts/layout.json").is_file()),
    _SCRIPT_PATH.parent,  # Individually copied stdlib tools do not need a repo.
)
if str(_REPOSITORY_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_REPOSITORY_ROOT))
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(_REPOSITORY_ROOT))
from pipeline.published_release import read_release
from pipeline.spark_lakehouse import session, table_identity


def canonical(rows):
    return sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)


def verify(spark, candidate):
    identities = {key: value for key, value in candidate["catalog"].items() if key != "gold"}
    identities.update(candidate["catalog"]["gold"])
    for name, expected in identities.items():
        actual = table_identity(spark, expected["table"])
        if actual != expected:
            raise ValueError("Published table identity changed: " + name)
    for name, expected in candidate["marts"].items():
        actual = [row.asDict(recursive=True) for row in spark.table(identities[name]["table"]).collect()]
        if canonical(actual) != canonical(expected):
            raise ValueError("Published Gold rows changed: " + name)
    actual = [row.asDict(recursive=True) for row in spark.table(identities["silver"]["table"]).collect()]
    if canonical(actual) != canonical(candidate["silver_rows"]):
        raise ValueError("Silver state differs from the accepted release")
    return {"release_id": candidate["release_id"], "identities": identities,
            "exact_gold_rows": True, "exact_silver_rows": True}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/opt/airflow/data/assessed/published"))
    args = parser.parse_args()
    _, candidate = read_release(args.root)
    spark = session()
    try:
        print(json.dumps(verify(spark, candidate), sort_keys=True, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
