"""Scan files about to be committed for secrets and machine-specific paths.

Run before every commit in this phase (``make check-secrets``). Exits non-zero
on any hit, which blocks the commit.

By default the script scans the staged content of every added, copied,
modified or renamed file, read from the git index rather than the working
tree, so it checks exactly what the commit will contain. ``--tracked`` scans
every file in the index instead; after a commit that is the content of HEAD.

The patterns are written so that this file does not match itself.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass

# (name, compiled pattern). Case-insensitive throughout.
PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("firebase-rtdb-host", re.compile(r"firebaseio[.]com", re.I)),
    ("firebase-rtdb-host", re.compile(r"firebasedatabase[.]app", re.I)),
    ("url-auth-parameter", re.compile(r"[?&]auth=", re.I)),
    ("private-key-block", re.compile(r"-{5}BEGIN", re.I)),
    # Assignments with a non-empty value. Empty assignments such as
    # "FIREBASE_DB_SECRET=" in .env.example are allowed.
    ("password-assignment", re.compile(r"password\s*=\s*['\"]?[^\s'\"#]+", re.I)),
    ("secret-assignment", re.compile(r"secret\s*=\s*['\"]?[^\s'\"#]+", re.I)),
    ("long-token-assignment", re.compile(r"token\s*=\s*['\"]?[A-Za-z0-9_\-.:/+]{20,}", re.I)),
    # Absolute paths into a user profile: drive-letter Windows paths, MSYS
    # drive paths, macOS Users and Linux home directories. The lookbehind
    # skips URL paths on a host name.
    ("user-home-path", re.compile(r"\b[A-Za-z]:[\\/]+Users[\\/]+\w", re.I)),
    ("user-home-path", re.compile(r"(?<![\w.])/(?:[A-Za-z]/)?(?:Users|home)/\w", re.I)),
]


@dataclass(frozen=True)
class Hit:
    path: str
    line: int
    rule: str


def git(*args: str) -> bytes:
    return subprocess.run(["git", *args], check=True, capture_output=True).stdout


def staged_paths() -> list[str]:
    out = git("diff", "--cached", "--name-only", "--diff-filter=ACMR", "-z")
    return [p for p in out.decode("utf-8").split("\0") if p]


def tracked_paths() -> list[str]:
    out = git("ls-files", "-z")
    return [p for p in out.decode("utf-8").split("\0") if p]


def read_blob(path: str) -> bytes:
    return git("show", f":{path}")


def scan(path: str, data: bytes) -> list[Hit]:
    if b"\0" in data[:8192]:  # binary
        return []
    text = data.decode("utf-8", errors="replace")
    hits = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        for rule, pattern in PATTERNS:
            if pattern.search(line):
                hits.append(Hit(path, lineno, rule))
    return hits


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tracked", action="store_true", help="scan every file in the index instead of the staged set")
    args = parser.parse_args()

    paths = tracked_paths() if args.tracked else staged_paths()
    hits = [h for p in paths for h in scan(p, read_blob(p))]

    scope = "tracked" if args.tracked else "staged"
    for h in hits:
        # Report location and rule only; never echo the matched value.
        print(f"FAIL {h.path}:{h.line} [{h.rule}]")
    print(f"check-secrets: {len(paths)} {scope} file(s) scanned, {len(hits)} hit(s)")
    return 1 if hits else 0


if __name__ == "__main__":
    sys.exit(main())
