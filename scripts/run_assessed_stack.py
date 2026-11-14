"""Compatibility entrypoint; implementation: scripts/acceptance/run_assessed_stack.py."""
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parent))
from _compat import dispatch as _dispatch
_dispatch("acceptance/run_assessed_stack.py", globals())
