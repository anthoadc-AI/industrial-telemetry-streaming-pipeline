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
import fnmatch
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
    # Any other absolute drive-letter path with at least one directory, such
    # as a working tree outside a user profile; doubled backslashes cover the
    # JSON-escaped form a traceback takes in a log. Profile paths are left to
    # user-home-path. Added after Task 3.6 STEP 0 found two the rules above
    # missed (owner decision, DETENTE 3.6.a, 2026-10-15).
    ("drive-path", re.compile(r"(?<![\w])[A-Za-z]:(?:\\{1,2}|/)(?!Users\b)[\w.\-]+(?:\\{1,2}|/)", re.I)),
]


@dataclass(frozen=True)
class AllowEntry:
    """A named, reviewed exception: one rule, suppressed only under one path glob."""

    name: str
    rule: str
    path_glob: str
    reason: str


# Every entry is an owner decision. Allowed hits are still reported, as ALLOW
# lines naming the entry, so an exception never passes silently.
ALLOW_LIST: list[AllowEntry] = [
    AllowEntry(
        name="internal-docs-describe-firebase-host-patterns",
        rule="firebase-rtdb-host",
        path_glob="docs/internal/**",
        # The Phase 0 diagnostic report and the Phase 1 briefing describe the
        # patterns the secret scan looks for, so they name the Firebase host
        # domains as text. They contain no database URL. Owner decision,
        # after DETENTE 1, 2026-09-30.
        reason="internal docs name the host domains the scan looks for",
    ),
    # The same documents list the remaining scan patterns by name: the URL
    # auth parameter, the PEM header marker, and empty password/secret
    # assignments closed by a Markdown backtick, which the value class reads
    # as a value. None carries a real value. Owner decision during Task 1.2,
    # 2026-09-30: allow-list, scoped to docs/internal only; patterns unchanged.
    *(
        AllowEntry(
            name="internal-docs-describe-scan-patterns",
            rule=rule,
            path_glob="docs/internal/**",
            reason="internal docs name the patterns the scan looks for",
        )
        for rule in (
            "url-auth-parameter",
            "private-key-block",
            "password-assignment",
            "secret-assignment",
        )
    ),
]


@dataclass(frozen=True)
class Hit:
    path: str
    line: int
    rule: str


def allowed_by(hit: Hit) -> AllowEntry | None:
    for entry in ALLOW_LIST:
        if entry.rule == hit.rule and fnmatch.fnmatchcase(hit.path, entry.path_glob):
            return entry
    return None


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
    failures = 0
    for h in hits:
        # Report location and rule only; never echo the matched value.
        entry = allowed_by(h)
        if entry:
            print(f"ALLOW {h.path}:{h.line} [{h.rule}] by {entry.name}")
        else:
            failures += 1
            print(f"FAIL {h.path}:{h.line} [{h.rule}]")
    allowed = len(hits) - failures
    print(f"check-secrets: {len(paths)} {scope} file(s) scanned, {failures} hit(s), {allowed} allowed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
