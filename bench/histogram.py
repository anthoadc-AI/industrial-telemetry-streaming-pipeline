"""Fixed-resolution latency histogram (owner decision D-H, Task 3.3).

The buckets are the same in every run and every metric, so histograms can be versioned, merged and re-read
years later, and any percentile or threshold fraction recomputed without the raw samples:

    0.1 ms wide from 0 to 10 ms        (100 buckets)
    1 ms wide from 10 ms to 1 s        (990 buckets)
    10 ms wide from 1 s to 60 s        (5,900 buckets)
    one overflow bucket above 60 s

A value is counted in the bucket whose lower edge is the largest one not above it, and a percentile is reported
as the upper edge of the bucket it falls in, so it never understates: the error is under one bucket width
(0.1, 1 or 10 ms). Negative values (possible when two clocks are compared) go to an underflow count and are
reported, not hidden. The 1 s threshold of D8 is a bucket edge, so the fraction below it is exact.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

FORMAT = "fixed-log-linear-v1"
# (start_ms, end_ms, width_ms) of each region, contiguous.
REGIONS = ((0.0, 10.0, 0.1), (10.0, 1000.0, 1.0), (1000.0, 60000.0, 10.0))
MAX_MS = REGIONS[-1][1]


def _region_sizes() -> list[int]:
    return [round((end - start) / width) for start, end, width in REGIONS]


SIZES = _region_sizes()
N_BUCKETS = sum(SIZES)  # 6,990, plus the overflow bucket


def bucket_index(value_ms: float) -> int:
    """Index of the bucket holding value_ms (0 <= value_ms); N_BUCKETS is the overflow bucket."""
    if value_ms >= MAX_MS:
        return N_BUCKETS
    offset = 0
    for (start, end, width), size in zip(REGIONS, SIZES):
        if value_ms < end:
            # The small epsilon keeps a value exactly on an edge (0.3, 1.0, ...) in the bucket that starts there
            # despite binary floating point.
            return offset + min(size - 1, int(math.floor((value_ms - start) / width + 1e-9)))
        offset += size
    raise AssertionError("unreachable")  # pragma: no cover


def bucket_bounds(index: int) -> tuple[float, float]:
    """(lower, upper) edge of a bucket in ms; the overflow bucket's upper edge is infinity."""
    if index >= N_BUCKETS:
        return MAX_MS, math.inf
    offset = 0
    for (start, _end, width), size in zip(REGIONS, SIZES):
        if index < offset + size:
            lower = start + (index - offset) * width
            return round(lower, 6), round(lower + width, 6)
        offset += size
    raise AssertionError("unreachable")  # pragma: no cover


@dataclass
class Histogram:
    counts: dict[int, int] = field(default_factory=dict)  # sparse: bucket index -> count
    underflow: int = 0  # negative values
    total: int = 0
    min_ms: float = math.inf
    max_ms: float = -math.inf

    def add(self, value_ms: float) -> None:
        if math.isnan(value_ms):
            raise ValueError("NaN latency")
        self.total += 1
        self.min_ms = min(self.min_ms, value_ms)
        self.max_ms = max(self.max_ms, value_ms)
        if value_ms < 0:
            self.underflow += 1
            return
        i = bucket_index(value_ms)
        self.counts[i] = self.counts.get(i, 0) + 1

    def merge(self, other: Histogram) -> Histogram:
        out = Histogram(dict(self.counts), self.underflow + other.underflow, self.total + other.total,
                        min(self.min_ms, other.min_ms), max(self.max_ms, other.max_ms))
        for i, c in other.counts.items():
            out.counts[i] = out.counts.get(i, 0) + c
        return out

    def percentile(self, q: float) -> float | None:
        """Nearest-rank percentile (0 < q <= 100), as the upper edge of its bucket; None when empty.

        An underflowed rank is reported as 0.0 (the value was below zero, see the module docstring).
        """
        if not 0 < q <= 100:
            raise ValueError("q must be in (0, 100]")
        if self.total == 0:
            return None
        rank = math.ceil(q / 100 * self.total)
        seen = self.underflow
        if rank <= seen:
            return 0.0
        for i in sorted(self.counts):
            seen += self.counts[i]
            if seen >= rank:
                # The upper edge never understates; capped at the largest value seen (and the overflow bucket,
                # whose upper edge is infinite, reports that value).
                return min(bucket_bounds(i)[1], self.max_ms)
        raise AssertionError("unreachable")  # pragma: no cover

    def fraction_below(self, threshold_ms: float) -> float | None:
        """Share of samples strictly below threshold_ms, exact when the threshold is a bucket edge."""
        if self.total == 0:
            return None
        below = self.underflow + sum(c for i, c in self.counts.items() if bucket_bounds(i)[1] <= threshold_ms)
        return below / self.total

    def to_json(self) -> dict:
        return {"format": FORMAT, "total": self.total, "underflow": self.underflow,
                "min_ms": None if self.total == 0 else self.min_ms, "max_ms": None if self.total == 0 else self.max_ms,
                "counts": {str(i): c for i, c in sorted(self.counts.items())}}

    @classmethod
    def from_json(cls, data: dict) -> Histogram:
        if data.get("format") != FORMAT:
            raise ValueError(f"unknown histogram format {data.get('format')!r}")
        h = cls({int(i): c for i, c in data["counts"].items()}, data["underflow"], data["total"])
        if h.total:
            h.min_ms, h.max_ms = data["min_ms"], data["max_ms"]
        return h

    def summary(self) -> dict:
        """n, p50, p95, p99 (None below 10,000 samples, protocol rule 5.1.6), min, max."""
        return {"n": self.total, "p50": self.percentile(50), "p95": self.percentile(95),
                "p99": self.percentile(99) if self.total >= 10_000 else None,
                "min": None if self.total == 0 else self.min_ms, "max": None if self.total == 0 else self.max_ms,
                "underflow": self.underflow}
