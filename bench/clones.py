"""A generated copy of config/ for one benchmark run (owner decisions D-A and D2, Task 3.3).

The copy is derived from config/, never edited by hand, and mounted read-only over the containers' config/:

- clones (D-A): every real asset of telemetry.yaml cloned clones_per_asset times, ids <asset>-c<NN>, each with
  the original's model and its alarms.yaml asset_overrides (motor_current_a has no default thresholds, so a
  clone without its override would not load). Every message then meets the alarm state machine, which an
  unknown asset would skip (verified at STEP 1, 1.1.1);
- linger (D2): bridge.yaml's producer gains linger.ms when benchmark.yaml sets bridge.linger_ms.

The result is checked with load_config before it is used.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import yaml

from config.loader import CONFIG_DIR, Config, load_config


def clone_id(asset_id: str, k: int) -> str:
    return f"{asset_id}-c{k:02d}"


def generate(target: Path, clones_per_asset: int = 0, linger_ms: float | None = None, source: Path = CONFIG_DIR) -> Config:
    """Write the run's config/ into target (replaced) and return it loaded; 0 clones keeps the real assets."""
    if clones_per_asset < 0:
        raise ValueError("clones_per_asset must be >= 0")
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(source, target, ignore=shutil.ignore_patterns("__pycache__", "*.py"))

    if clones_per_asset:
        telemetry = yaml.safe_load((target / "telemetry.yaml").read_text(encoding="utf-8"))
        alarms = yaml.safe_load((target / "alarms.yaml").read_text(encoding="utf-8"))
        overrides = alarms.get("asset_overrides") or {}
        assets, cloned_overrides = [], {}
        for asset in telemetry["assets"]:
            for k in range(clones_per_asset):
                cid = clone_id(asset["id"], k)
                assets.append({**asset, "id": cid, "description": f"Benchmark clone {k} of {asset['id']}"})
                if asset["id"] in overrides:
                    cloned_overrides[cid] = overrides[asset["id"]]
        telemetry["assets"] = assets
        telemetry["fault_schedule"] = []
        alarms["asset_overrides"] = cloned_overrides
        _write(target / "telemetry.yaml", telemetry, "telemetry.yaml with cloned assets")
        _write(target / "alarms.yaml", alarms, "alarms.yaml with the clones' asset_overrides")

    if linger_ms is not None:
        bridge = yaml.safe_load((target / "bridge.yaml").read_text(encoding="utf-8"))
        bridge["producer"]["linger.ms"] = linger_ms
        _write(target / "bridge.yaml", bridge, f"bridge.yaml with linger.ms {linger_ms:g}")

    return load_config(target, env={})


def _write(path: Path, data: dict, what: str) -> None:
    header = f"# GENERATED for one benchmark run by bench/clones.py from config/ ({what}). Do not edit.\n"
    path.write_text(header + yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
