"""Compatibility for reviewed, repository-local script entrypoints."""
import importlib
from pathlib import Path
import runpy
import sys


def dispatch(relative, namespace):
    scripts = Path(__file__).resolve().parent
    target = (scripts / relative).resolve()
    if not target.is_relative_to(scripts) or not target.is_file():
        raise ValueError("Invalid script compatibility target")
    root = scripts.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    name = namespace["__name__"]
    if name == "__main__":
        runpy.run_path(str(target), run_name="__main__")
    else:
        module = importlib.import_module("scripts." + relative.removesuffix(".py").replace("/", "."))
        # Alias regular imports; retain exports for spec_from_file_location callers.
        namespace.update({key: value for key, value in vars(module).items() if not key.startswith("__")})
        sys.modules[name] = module
