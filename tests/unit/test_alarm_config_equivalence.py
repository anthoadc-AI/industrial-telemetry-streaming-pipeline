"""processing/alarm_consumer/config_loader.js must agree with config/loader.py.

1. On the real config/, both loaders resolve the same alarm rules: the same
   (asset_id, channel) pairs, in the same order, with the same direction,
   thresholds, hysteresis and persistence. They also agree on the Kafka topic
   names and topic settings the consumer uses.
2. On broken copies of config/, both reject the configuration with the same
   message. For YAML that does not parse, the parsers' own wording differs,
   so only the common prefix "<file>: invalid YAML (" is compared.

Runs under pytest, or directly: python tests/unit/test_alarm_config_equivalence.py
Requires node on PATH and the npm dependencies installed (npm ci).
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
from config.loader import ConfigError, load_config  # noqa: E402

NODE_SCRIPT = r"""
const { loadConfig, ConfigError } = require(process.argv[1]);
try {
  const c = loadConfig({ configDir: process.argv[2], env: {} });
  const topics = Object.fromEntries(Object.entries(c.topics).map(([k, v]) => [k, { name: v.name, configs: v.configs }]));
  process.stdout.write(JSON.stringify({ ok: true, rules: c.rules, topics }));
} catch (e) {
  if (!(e instanceof ConfigError)) throw e;
  process.stdout.write(JSON.stringify({ ok: false, error: e.message }));
}
"""

# (name, file, text to replace, replacement)
BROKEN = [
    ("invalid YAML", "alarms.yaml", "channels:\n", "channels: [unclosed\n"),
    ("override for an asset that does not exist", "alarms.yaml", "  motor-01:\n", "  motor-99:\n"),
    ("critical not above warning", "alarms.yaml", "critical: 95.0", "critical: 70.0"),
    ("hysteresis wider than the band", "alarms.yaml", "hysteresis: 3.0", "hysteresis: 20.0"),
    ("unknown key in a rule", "alarms.yaml", "    persistence_samples: 3\n", "    persistence_samples: 3\n    persistance: 4\n"),
    ("persistence not a positive integer", "alarms.yaml", "persistence_samples: 3", "persistence_samples: 0"),
    (
        "missing per-asset current thresholds",
        "alarms.yaml",
        "  pump-02:\n    motor_current_a: {warning: 31.5, critical: 36.0}\n",
        "",
    ),
    (
        "alarm on an unknown channel",
        "alarms.yaml",
        "  vibration_rms_mm_s:\n    direction: high",
        "  oil_pressure_bar:\n    direction: high",
    ),
    ("negative hysteresis", "alarms.yaml", "hysteresis: 0.3", "hysteresis: -0.3"),
    # Found by the Task 2.4 unit tests: Python accepted an empty list here,
    # Node.js rejected it.
    # "re:" marks a regular expression: the whole overrides block, to the end
    # of the file, becomes an empty list.
    ("asset_overrides as an empty list", "alarms.yaml", r"re:asset_overrides:\n.*\Z", "asset_overrides: []\n"),
]


def node_load(config_dir: Path) -> dict:
    node = shutil.which("node")
    assert node, "node is not on PATH"
    result = subprocess.run(
        [node, "-e", NODE_SCRIPT, str(REPO_ROOT / "processing" / "alarm_consumer" / "config_loader.js"), str(config_dir)],
        capture_output=True, check=True,
    )
    return json.loads(result.stdout.decode("utf-8"))


def python_rules(config_dir: Path) -> list[dict]:
    cfg = load_config(config_dir, env={})
    return [
        {"asset_id": asset, "channel": channel, "direction": r.direction, "warning": r.warning, "critical": r.critical,
         "hysteresis": r.hysteresis, "persistence_samples": r.persistence_samples}
        for (asset, channel), r in cfg.alarms.rules.items()
    ]


def broken_copy(workdir: Path, file: str, old: str, new: str) -> Path:
    target = workdir / "config"
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(REPO_ROOT / "config", target, ignore=shutil.ignore_patterns("__pycache__", "*.py"))
    path = target / file
    text = path.read_text(encoding="utf-8")
    if old.startswith("re:"):
        changed, n = re.subn(old[3:], lambda m: new, text, count=1, flags=re.DOTALL)
        assert n == 1, f"test fixture out of date: {old!r} matches nothing in {file}"
    else:
        assert old in text, f"test fixture out of date: {old!r} not in {file}"
        changed = text.replace(old, new, 1)
    path.write_text(changed, encoding="utf-8")
    return target


def test_same_rules_and_topics():
    node = node_load(REPO_ROOT / "config")
    assert node["ok"], node
    assert node["rules"] == python_rules(REPO_ROOT / "config")
    cfg = load_config(REPO_ROOT / "config", env={})
    assert node["topics"] == {k: {"name": t.name, "configs": dict(t.configs)} for k, t in cfg.topics.kafka.items()}


def rejections() -> list[tuple[str, str, str]]:
    out = []
    with tempfile.TemporaryDirectory() as tmp:
        for name, file, old, new in BROKEN:
            config_dir = broken_copy(Path(tmp), file, old, new)
            try:
                load_config(config_dir, env={})
                py = "ACCEPTED"
            except ConfigError as e:
                py = str(e)
            node = node_load(config_dir)
            out.append((name, py, "ACCEPTED" if node["ok"] else node["error"]))
    return out


def test_same_rejections():
    for name, py, js in rejections():
        assert py != "ACCEPTED" and js != "ACCEPTED", f"{name}: python {py!r}, node {js!r}"
        if name == "invalid YAML":
            prefix = "alarms.yaml: invalid YAML ("
            assert py.startswith(prefix) and js.startswith(prefix), f"{name}: python {py!r}, node {js!r}"
        else:
            assert py == js, f"{name}:\n  python: {py}\n  node:   {js}"


if __name__ == "__main__":
    test_same_rules_and_topics()
    print(f"rules: {len(python_rules(REPO_ROOT / 'config'))} identical in both loaders; topics identical")
    for name, py, js in rejections():
        print(f"[{name}]\n  python: {py}\n  node:   {js}")
    test_same_rejections()
    print("PASS")
