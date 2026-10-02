"""scripts/clean.py: the path checks that run before anything is deleted, and the deletion itself.

Every test works in a temporary directory standing in for the repository;
none touches the real one.
"""

from __future__ import annotations

import importlib.util
import os
import sys

import pytest

from conftest import REPO_ROOT

spec = importlib.util.spec_from_file_location("clean", REPO_ROOT / "scripts" / "clean.py")
cl = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = cl
spec.loader.exec_module(cl)


@pytest.fixture
def repo(tmp_path):
    root = tmp_path / "repo"
    for d in (".git", ".venv/lib/__pycache__", "node_modules/x/__pycache__", "coverage", "pkg/__pycache__",
              "pkg/sub/__pycache__", ".pytest_cache"):
        (root / d).mkdir(parents=True)
    for f in (".env", ".coverage", "coverage/lcov.info", "pkg/mod.py", "pkg/__pycache__/mod.pyc"):
        (root / f).write_text("x", encoding="utf-8")
    (tmp_path / "outside.txt").write_text("keep", encoding="utf-8")
    return root


@pytest.mark.parametrize("path", ["..", "../outside.txt", "coverage/../../outside.txt"])
def test_paths_outside_the_repository_are_refused(repo, path):
    with pytest.raises(cl.RefusedPath, match="outside the repository"):
        cl.check(repo, path)


def test_absolute_path_is_refused(repo, tmp_path):
    with pytest.raises(cl.RefusedPath, match="absolute"):
        cl.check(repo, str(tmp_path / "outside.txt"))


@pytest.mark.parametrize("path", [".", "pkg/.."])
def test_the_root_is_refused(repo, path):
    with pytest.raises(cl.RefusedPath, match="repository root"):
        cl.check(repo, path)


@pytest.mark.parametrize("path", [".git", ".venv", ".venv/lib", "node_modules/x", ".env"])
def test_protected_paths_are_refused(repo, path):
    with pytest.raises(cl.RefusedPath, match="protected"):
        cl.check(repo, path)


def test_symbolic_link_leading_outside_is_refused(repo, tmp_path):
    link = repo / "escape"
    try:
        os.symlink(tmp_path, link, target_is_directory=True)
    except OSError:
        pytest.skip("this account cannot create symbolic links")
    with pytest.raises(cl.RefusedPath, match="outside the repository"):
        cl.check(repo, "escape")


def test_one_refused_path_deletes_nothing(repo, tmp_path):
    with pytest.raises(cl.RefusedPath):
        cl.clean(repo, [".coverage", "coverage", "../outside.txt"], pycache=True)
    assert (repo / ".coverage").exists() and (repo / "coverage" / "lcov.info").exists()
    assert (repo / "pkg" / "__pycache__").exists() and (tmp_path / "outside.txt").exists()


def test_clean_removes_files_and_trees_and_skips_absent(repo):
    removed, absent = cl.clean(repo, [".coverage", "coverage", ".pytest_cache", "coverage.xml"])
    assert sorted(p.name for p in removed) == [".coverage", ".pytest_cache", "coverage"]
    assert absent == ["coverage.xml"]
    assert not (repo / "coverage").exists() and (repo / "pkg" / "mod.py").exists()


def test_pycache_skips_venv_node_modules_and_git(repo):
    removed, _ = cl.clean(repo, [], pycache=True)
    assert sorted(p.relative_to(repo.resolve()).as_posix() for p in removed) == \
        ["pkg/__pycache__", "pkg/sub/__pycache__"]
    assert (repo / ".venv/lib/__pycache__").exists() and (repo / "node_modules/x/__pycache__").exists()
    assert (repo / ".env").exists() and (repo / "pkg" / "mod.py").exists()


def test_main_reports_and_exits(repo, monkeypatch, capsys):
    monkeypatch.setattr(cl, "REPO_ROOT", repo.resolve())
    assert cl.main([".coverage", "missing"]) == 0
    assert capsys.readouterr().out.splitlines() == ["clean: removed .coverage", "clean: 1 removed, 1 absent"]
    assert cl.main([".env"]) == 1
    assert "refused, nothing deleted" in capsys.readouterr().err


def test_main_reports_a_failed_removal(repo, monkeypatch, capsys):
    monkeypatch.setattr(cl, "REPO_ROOT", repo.resolve())

    def fail(target):
        raise PermissionError(13, "Permission denied", str(target))
    monkeypatch.setattr(cl, "remove", fail)
    assert cl.main([".coverage"]) == 1
    assert "could not remove" in capsys.readouterr().err
