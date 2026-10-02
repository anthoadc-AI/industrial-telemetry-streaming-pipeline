"""scripts/check_secrets.py: every pattern, the allow-list scoping, and the git plumbing (faked).

The secret-like strings below are built from pieces at run time, so this test
file does not itself trip the scan it tests.
"""

from __future__ import annotations

import importlib.util
import sys
import types

import pytest

from conftest import REPO_ROOT

spec = importlib.util.spec_from_file_location("check_secrets", REPO_ROOT / "scripts" / "check_secrets.py")
cs = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = cs
spec.loader.exec_module(cs)

J = "".join
HITS = {
    "firebase-rtdb-host": J(["https://demo-default-rtdb.", "firebase", "io.com/x.json"]),
    "url-auth-parameter": J(["https://h/x.json", "?", "auth=abc"]),
    "private-key-block": J(["-----", "BEGIN PRIVATE KEY-----"]),
    "password-assignment": J(["pass", "word = hunter2"]),
    "secret-assignment": J(["FIREBASE_DB_", "SEC", "RET=abcdef"]),
    "long-token-assignment": J(["TB_ACCESS_", "TOKEN=", "A" * 24]),
    "user-home-path": J(["C:", "\\", "Users", "\\", "someone\\.mytb-data"]),
}
CLEAN = [
    J(["FIREBASE_DB_", "SEC", "RET="]),
    J(["TB_ACCESS_", "TOKEN=DHT11_DEMO_TOKEN"]),
    "see https://api.example.com/users/42 and ~/.mytb-data",
    "%USERPROFILE%\\.mytb-data",
]


@pytest.mark.parametrize("rule, text", HITS.items())
def test_each_pattern_hits(rule, text):
    assert [h.rule for h in cs.scan("f.txt", text.encode())] == [rule]


@pytest.mark.parametrize("text", CLEAN)
def test_clean_lines_pass(text):
    assert cs.scan("f.txt", text.encode()) == []


@pytest.mark.parametrize("text", [J(["/c/", "Users/someone/x"]), J(["/", "home/someone/x"])])
def test_other_home_path_forms(text):
    assert [h.rule for h in cs.scan("f.txt", text.encode())] == ["user-home-path"]


def test_binary_files_are_skipped():
    assert cs.scan("img.png", b"\x89PNG\x00\x00" + HITS["private-key-block"].encode()) == []


def test_line_numbers():
    hits = cs.scan("f.txt", ("ok\n" + HITS["password-assignment"]).encode())
    assert (hits[0].path, hits[0].line) == ("f.txt", 2)


def test_allow_list_is_scoped_to_docs_internal():
    inside = cs.Hit("docs/internal/report.md", 1, "url-auth-parameter")
    outside = cs.Hit("docs/report.md", 1, "url-auth-parameter")
    never = cs.Hit("docs/internal/report.md", 1, "user-home-path")
    assert cs.allowed_by(inside).name == "internal-docs-describe-scan-patterns"
    assert cs.allowed_by(outside) is None and cs.allowed_by(never) is None


def fake_git(monkeypatch, files):
    def run(cmd, check, capture_output):
        args = cmd[1:]
        if args[:2] == ["diff", "--cached"] or args[0] == "ls-files":
            return types.SimpleNamespace(stdout=b"\0".join(p.encode() for p in files) + b"\0")
        if args[0] == "show":
            return types.SimpleNamespace(stdout=files[args[1][1:]])
        raise AssertionError(args)
    monkeypatch.setattr(cs.subprocess, "run", run)


def test_main_fails_on_a_hit_and_reports_allowed_ones(monkeypatch, capsys):
    fake_git(monkeypatch, {"app.py": HITS["password-assignment"].encode(),
                           "docs/internal/r.md": HITS["firebase-rtdb-host"].encode(),
                           "ok.py": b"print('hi')"})
    monkeypatch.setattr(sys, "argv", ["check_secrets.py"])
    assert cs.main() == 1
    out = capsys.readouterr().out
    assert "FAIL app.py:1 [password-assignment]" in out
    assert "ALLOW docs/internal/r.md:1 [firebase-rtdb-host] by internal-docs-describe-firebase-host-patterns" in out
    assert "3 staged file(s) scanned, 1 hit(s), 1 allowed" in out
    assert "hunter2" not in out  # the matched value is never echoed


def test_main_passes_on_clean_tracked_files(monkeypatch, capsys):
    fake_git(monkeypatch, {"ok.py": b"x = 1"})
    monkeypatch.setattr(sys, "argv", ["check_secrets.py", "--tracked"])
    assert cs.main() == 0
    assert "1 tracked file(s) scanned, 0 hit(s), 0 allowed" in capsys.readouterr().out


def test_git_helper_runs_git(monkeypatch):
    seen = {}

    def run(cmd, check, capture_output):
        seen.update(cmd=cmd, check=check)
        return types.SimpleNamespace(stdout=b"out")
    monkeypatch.setattr(cs.subprocess, "run", run)
    assert cs.git("status") == b"out" and seen == {"cmd": ["git", "status"], "check": True}
