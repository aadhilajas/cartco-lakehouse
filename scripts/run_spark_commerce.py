"""Build a catalogued Spark candidate; independent quality gate publishes it."""
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
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
