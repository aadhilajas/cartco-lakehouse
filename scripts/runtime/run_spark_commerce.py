"""Build a catalogued Spark candidate; independent quality gate publishes it."""

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
from pathlib import Path
import sys

sys.path.insert(0, str(_REPOSITORY_ROOT))
from pipeline.spark_lakehouse import session, run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--namespace", default="cartco_assessed")
    args = parser.parse_args()
    spark = session()
    try:
        print(run(spark, args.manifest, namespace=args.namespace, candidate_path=args.candidate))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
