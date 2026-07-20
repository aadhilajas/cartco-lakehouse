"""Repo-root pytest bootstrap.

Ensures the repository root is on ``sys.path`` regardless of pytest's
rootdir/import-mode detection, so ``import transforms.silver_shopify_orders``
etc. resolve the same way here as they do for Airflow tasks (see the same
``sys.path.insert`` pattern in ``dags/shopify_bronze_dag.py``).
"""

import pathlib
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
