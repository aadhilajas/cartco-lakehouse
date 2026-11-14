"""Compatibility for the shared Spark JAR checksum helper."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from infra.shared.spark import fetch_jars as _implementation
if __name__ == "__main__":
    _implementation.fetch(Path(sys.argv[1]), Path(sys.argv[2]))
else:
    sys.modules[__name__] = _implementation
