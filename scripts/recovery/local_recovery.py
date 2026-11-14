"""Verify/create local recovery bundles or restore absent original paths. Never overwrite."""

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
from pipeline.recovery import create_backup, restore_backup, verify_backup


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    backup = commands.add_parser("backup")
    restore = commands.add_parser("restore")
    for command in (backup, restore):
        command.add_argument("--catalog-db", required=True, type=Path)
        command.add_argument("--warehouse", required=True, type=Path)
        command.add_argument("--runtime-root", required=True, type=Path)
        command.add_argument("--writers-stopped", action="store_true", help="Explicit acknowledgement: all writers must actually be stopped")
    backup.add_argument("--output", required=True, type=Path, help="New directory outside all source targets")
    backup.add_argument("--catalog-name", required=True, help="Exact populated SQLite catalog name")
    restore.add_argument("--backup", required=True, type=Path)
    verify = commands.add_parser("verify")
    verify.add_argument("--backup", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "backup":
        manifest = create_backup(args.catalog_db, args.warehouse, args.runtime_root, args.output,
                                 catalog_name=args.catalog_name, writers_stopped=args.writers_stopped)
        result = dict(files=len(manifest["files"]), audit=manifest["audit"], backup=str(args.output.resolve()))
    elif args.command == "restore":
        result = restore_backup(args.backup, args.catalog_db, args.warehouse, args.runtime_root, writers_stopped=args.writers_stopped)
    else:
        manifest = verify_backup(args.backup)
        result = dict(verified=True, files=len(manifest["files"]), original_paths=manifest["original_paths"], audit=manifest["audit"])
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
