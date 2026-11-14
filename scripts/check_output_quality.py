"""Compatibility entrypoint; implementation: scripts/diagnostics/check_output_quality.py."""
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parent))
from _compat import dispatch as _dispatch
_dispatch("diagnostics/check_output_quality.py", globals())
