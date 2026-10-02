"""edge/simulator/model.py: the physical models, fault offsets, drift, noise and reproducibility."""

from __future__ import annotations

import dataclasses
import math
import random

import pytest

from config.loader import ScheduledFault
from edge.simulator.model import ActiveFault, AssetModel


def quiet(asset):
    """The asset with every noise source set to zero, so models can be checked exactly."""
    return dataclasses.replace(asset, noise_std={c: 0.0 for c in asset.channels},
                               load=dataclasses.replace(asset.load, noise_std=0.0))


def build(cfg, asset_id, schedule=(), noise=False, seed=1, **asset_changes):
    asset = cfg.telemetry.asset(asset_id)
    asset = asset if noise else quiet(asset)
    asset = dataclasses.replace(asset, **asset_changes)
    return AssetModel.build(asset, cfg.telemetry.ambient_temperature_c, dict(cfg.telemetry.faults), list(schedule),
                            random.Random(seed))


def test_steady_state_start_and_channel_models(cfg):
    m = build(cfg, "motor-01")
    a = m.asset
    assert m.temperature_c == pytest.approx(25.0 + 40.0 * 0.75)
    s = m.step(0.0, 1.0)  # sin(0) = 0: load is exactly the base
    assert s.values["speed_rpm"] == pytest.approx(1500 * (1 - 0.02 * 0.75))
    assert s.values["motor_current_a"] == pytest.approx(55.0 * (0.35 + 0.65 * 0.75))
    assert s.values["vibration_rms_mm_s"] == pytest.approx(1.2 + 0.8 * 0.75)
    assert s.values["bearing_temperature_c"] == pytest.approx(55.0)
    assert s.active_faults == ()
    assert a.channels == tuple(s.values)


def test_load_follows_the_duty_cycle_and_is_clamped(cfg):
    m = build(cfg, "motor-01")
    assert m.load(225.0) == pytest.approx(0.75 + 0.10)  # quarter of a 900 s period
    base_load = cfg.telemetry.asset("motor-01").load
    high = build(cfg, "motor-01", load=dataclasses.replace(base_load, base=1.2, amplitude=0.5, noise_std=0.0))
    assert high.load(225.0) == 1.2
    low = build(cfg, "motor-01", load=dataclasses.replace(base_load, base=0.0, amplitude=0.5, noise_std=0.0))
    assert low.load(675.0) == 0.0


def test_temperature_is_a_first_order_lag(cfg):
    full_load = dataclasses.replace(cfg.telemetry.asset("motor-01").load, base=1.0, amplitude=0.0, noise_std=0.0)
    m = build(cfg, "motor-01", load=full_load)
    start, target = m.temperature_c, 25.0 + 40.0
    m.step(0.0, 600.0)  # one time constant
    assert m.temperature_c == pytest.approx(target - (target - start) * math.exp(-1))


def test_step_fault_offsets_from_its_start(cfg):
    fault = ActiveFault(cfg.telemetry.faults["imbalance"], 10.0)
    assert (fault.offset(9.9), fault.offset(10.0), fault.offset(500.0)) == (0.0, 4.0, 4.0)


def test_ramp_fault_rises_then_holds(cfg):
    fault = ActiveFault(cfg.telemetry.faults["bearing_overheating"], 10.0)
    assert fault.offset(10.0) == 0.0
    assert fault.offset(100.0) == pytest.approx(45.0 * 90 / 180)
    assert fault.offset(190.0) == fault.offset(1000.0) == 45.0


def test_fault_reaches_only_its_asset_and_channel(cfg):
    schedule = [ScheduledFault("pump-01", "imbalance", 0.0)]
    faulty, healthy = build(cfg, "pump-01", schedule), build(cfg, "pump-02", schedule)
    s_f, s_h = faulty.step(0.0, 1.0), healthy.step(0.0, 1.0)
    assert s_f.values["vibration_rms_mm_s"] == pytest.approx(1.8 + 1.0 * 0.8 + 4.0)
    assert s_f.values["bearing_temperature_c"] == pytest.approx(s_h.values["bearing_temperature_c"])
    assert s_f.active_faults == ("imbalance",) and s_h.active_faults == ()


def test_degradation_drifts_per_hour(cfg):
    m = build(cfg, "pump-02", load=dataclasses.replace(cfg.telemetry.asset("pump-02").load, amplitude=0.0, noise_std=0.0))
    base = m.step(0.0, 1.0).values["vibration_rms_mm_s"]
    assert m.step(3600.0, 1.0).values["vibration_rms_mm_s"] - base == pytest.approx(0.6)


def test_non_negative_channels_are_clamped(cfg):
    asset = cfg.telemetry.asset("motor-01")
    m = build(cfg, "motor-01", noise=True, noise_std={**asset.noise_std, "vibration_rms_mm_s": 1000.0})
    assert all(m.step(float(t), 1.0).values["vibration_rms_mm_s"] >= 0.0 for t in range(200))


def test_same_seed_same_values(cfg):
    a = [build(cfg, "compressor-01", noise=True, seed=7).step(float(t), 1.0).values for t in range(5)]
    b = [build(cfg, "compressor-01", noise=True, seed=7).step(float(t), 1.0).values for t in range(5)]
    c = [build(cfg, "compressor-01", noise=True, seed=8).step(float(t), 1.0).values for t in range(5)]
    assert a == b and a != c
