"""Remove test, coverage and smoke output: an explicit list of repository paths.

    python scripts/clean.py [--pycache] PATH [PATH ...]

Each PATH is relative to the repository root, whatever the working
directory. Every path is checked before anything is deleted, and the run
fails without deleting anything if a path resolves outside the repository
(through .., an absolute path or a symbolic link) or is, or is inside, a
protected path: the root itself, .git, .venv, node_modules or .env. Paths
that do not exist are skipped.

--pycache also removes every __pycache__ directory under the root, without
descending into .git, .venv or node_modules.

Docker volumes are never touched: they are not files of the repository.

Exit status: 0 when every path was removed or absent, 1 when a path was
refused (nothing is deleted then) or could not be removed.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PROTECTED = (".git", ".venv", "node_modules", ".env")
NOT_WALKED = {".git", ".venv", "node_modules"}


class RefusedPath(ValueError):
    pass


def check(root: Path, relative: str) -> Path:
    """The resolved target of relative, or RefusedPath if it must not be deleted."""
    root = root.resolve()
    if Path(relative).is_absolute():
        raise RefusedPath(f"{relative}: absolute path; give a path relative to the repository")
    target = (root / relative).resolve()
    if target != root and root not in target.parents:
        raise RefusedPath(f"{relative}: resolves outside the repository ({target})")
    if target == root:
        raise RefusedPath(f"{relative}: is the repository root")
    for name in PROTECTED:
        guarded = root / name
        if target == guarded or guarded in target.parents:
            raise RefusedPath(f"{relative}: protected ({name})")
    return target


def pycache_dirs(root: Path) -> list[Path]:
    found = []
    for current, dirs, _ in os.walk(root):
        dirs[:] = [d for d in dirs if d not in NOT_WALKED]
        if "__pycache__" in dirs:
            found.append(Path(current) / "__pycache__")
            dirs.remove("__pycache__")
    return sorted(found)


def remove(target: Path) -> None:
    if target.is_dir() and not target.is_symlink():
        shutil.rmtree(target)
    else:
        target.unlink()


def clean(root: Path, paths: list[str], pycache: bool = False) -> tuple[list[Path], list[str]]:
    """Check every path, then delete. Returns (removed, absent); raises RefusedPath before deleting anything."""
    root = root.resolve()
    relatives = list(paths)
    if pycache:
        relatives += [p.relative_to(root).as_posix() for p in pycache_dirs(root)]
    targets = [(relative, check(root, relative)) for relative in relatives]
    removed, absent = [], []
    for relative, target in targets:
        if not os.path.lexists(target):
            absent.append(relative)
            continue
        remove(target)
        removed.append(target)
    return removed, absent


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Remove test, coverage and smoke output from the repository.")
    p.add_argument("paths", nargs="+", metavar="PATH", help="path relative to the repository root")
    p.add_argument("--pycache", action="store_true", help="also remove every __pycache__ directory")
    args = p.parse_args(argv)
    try:
        removed, absent = clean(REPO_ROOT, args.paths, args.pycache)
    except RefusedPath as e:
        print(f"clean: refused, nothing deleted: {e}", file=sys.stderr)
        return 1
    except OSError as e:
        print(f"clean: could not remove {e.filename}: {e.strerror}", file=sys.stderr)
        return 1
    for target in removed:
        print(f"clean: removed {target.relative_to(REPO_ROOT).as_posix()}")
    print(f"clean: {len(removed)} removed, {len(absent)} absent")
    return 0


if __name__ == "__main__":
    sys.exit(main())
