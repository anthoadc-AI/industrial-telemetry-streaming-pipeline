"""Physical models for one simulated rotating asset.

Every value produced here is synthetic. The models are deliberately simple:
plausible to a plant engineer, and no more elaborate than the pipeline needs.
All parameters come from config/telemetry.yaml; none is hard-coded here.

The asset is driven by a per-unit load signal, and each channel follows from
the load:

load (per unit of rated load)
    ``base + amplitude * sin(2 pi t / period_s) + N(0, noise_std)``, clamped
    to [0, 1.2]. A slow periodic duty cycle with small random disturbance.

speed_rpm
    ``synchronous_speed_rpm * (1 - rated_slip * load)``. An induction motor
    runs below synchronous speed by a slip roughly proportional to load.

motor_current_a
    ``rated_current_a * (no_load_fraction + (1 - no_load_fraction) * load)``.
    Current rises linearly with load above the magnetising (no-load) current.

bearing_temperature_c
    First-order lag toward the load-dependent steady state
    ``ambient + rise_at_full_load_c * load`` with time constant
    ``time_constant_s``, integrated exactly per step:
    ``T += (T_ss - T) * (1 - exp(-dt / tau))``. The state starts at the
    steady state for the base load, so a run begins without a warm-up
    transient.

vibration_rms_mm_s
    ``baseline_mm_s + load_coefficient_mm_s * load``. Casing vibration grows
    with load around a machine-specific baseline.

On top of the model value, each channel receives, in this order:

- degradation drift, ``rate_per_hour * t / 3600``, where configured;
- injected fault offsets: a ``step`` adds its magnitude from start_s on; a
  ``ramp`` rises linearly to its magnitude over ramp_s, then holds. A fault
  acts on the measured channel directly, standing for a local effect (for
  example frictional heating at a failing bearing) that is faster than the
  machine's own thermal time constant;
- Gaussian measurement noise, ``N(0, noise_std[channel])``.

Current, speed and vibration are clamped at zero after noise.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass

from config.loader import Asset, FaultMode, ScheduledFault

NON_NEGATIVE = ("vibration_rms_mm_s", "motor_current_a", "speed_rpm")


@dataclass(frozen=True)
class ActiveFault:
    mode: FaultMode
    start_s: float

    def offset(self, t: float) -> float:
        if t < self.start_s:
            return 0.0
        if self.mode.kind == "step":
            return self.mode.magnitude
        return self.mode.magnitude * min(1.0, (t - self.start_s) / self.mode.ramp_s)


@dataclass(frozen=True)
class Sample:
    values: dict[str, float]
    active_faults: tuple[str, ...]


class AssetModel:
    """State and per-step evaluation for one asset."""

    def __init__(self, asset: Asset, ambient_c: float, faults: list[ActiveFault], rng: random.Random):
        self.asset = asset
        self.ambient_c = ambient_c
        self.faults = faults
        self.rng = rng
        self.temperature_c = self._steady_state_temperature(asset.load.base)

    @classmethod
    def build(cls, asset: Asset, ambient_c: float, fault_modes: dict[str, FaultMode],
              schedule: list[ScheduledFault], rng: random.Random) -> AssetModel:
        faults = [ActiveFault(fault_modes[s.mode], s.start_s) for s in schedule if s.asset == asset.id]
        return cls(asset, ambient_c, faults, rng)

    def _steady_state_temperature(self, load: float) -> float:
        return self.ambient_c + self.asset.thermal.rise_at_full_load_c * load

    def load(self, t: float) -> float:
        ld = self.asset.load
        value = ld.base + ld.amplitude * math.sin(2 * math.pi * t / ld.period_s) + self.rng.gauss(0.0, ld.noise_std)
        return min(1.2, max(0.0, value))

    def step(self, t: float, dt: float) -> Sample:
        """Advance the thermal state by dt seconds and return the sample at time t."""
        a = self.asset
        load = self.load(t)

        target = self._steady_state_temperature(load)
        self.temperature_c += (target - self.temperature_c) * (1.0 - math.exp(-dt / a.thermal.time_constant_s))

        model = {
            "bearing_temperature_c": self.temperature_c,
            "vibration_rms_mm_s": a.vibration.baseline_mm_s + a.vibration.load_coefficient_mm_s * load,
            "motor_current_a": a.electrical.rated_current_a
            * (a.electrical.no_load_fraction + (1.0 - a.electrical.no_load_fraction) * load),
            "speed_rpm": a.electrical.synchronous_speed_rpm * (1.0 - a.electrical.rated_slip * load),
        }

        values = {}
        for channel in a.channels:
            v = model[channel]
            v += a.degradation.get(channel, 0.0) * t / 3600.0
            v += sum(f.offset(t) for f in self.faults if f.mode.channel == channel)
            v += self.rng.gauss(0.0, a.noise_std[channel])
            if channel in NON_NEGATIVE:
                v = max(0.0, v)
            values[channel] = v

        active = tuple(sorted({f.mode.name for f in self.faults if t >= f.start_s}))
        return Sample(values, active)
