# Diagnostic passes (Task 3.3 STEP 0)

Raw data of the produce_to_ack_ms matrix (`run_matrix.py`). Diagnostics for
designing the benchmark protocol; none of these figures is publishable.
Every pass keeps its cells as they ran; `cells.jsonl` records the power source
before (and, from pass 2, after) every cell.

| Pass | Directory | Power | Cells | Outcome |
|---|---|---|---|---|
| 1 | `run1-battery-then-ac-interrupted` | battery for the first 4 cells, AC from `host-singles10-base` | 7 of 15 | The lid was closed at 22:50:29 (suspend), then hibernation 23:00:31 to the next day 21:03. `host-iso-nagleoff` and `host-iso-linger0` are **invalid**: they measure the suspend (a message timed out, the next left 600 s late). No validity check existed yet. |
| 2 | `run2-ac-then-battery-stopped` | AC, unplugged during cell 3 (21:11:48) | 3 of 15 | Stopped by the validity check (AC required) on `host-burst-base`, which ran partly on battery and is invalid; the first 2 cells are valid on AC. |
| 3 | `run3-battery-stopped` | battery throughout | 5 of 15 | Validity criterion changed to "same power source for the whole pass" so the matrix could continue on battery; stopped by the owner, who restored "AC for the whole pass". All 5 cells valid under the battery criterion. |
| 4 | `run4-ac` | AC required for every cell | 15 of 15 | The reference pass; all cells valid. |

Also here:

- `bimodal-ac`: `bimodal.py`, 8 rounds of 5 burst variants (110 messages per cell), AC, all 40 cells valid.
- `clock-ac`: `clock_offset.py`, host vs container clocks at t0 and t0 + 10 min, AC.

Per cell: `<cell>.csv` (one row per message) and `<cell>.stats.csv` (librdkafka statistics per window, from `reduce_stats.py`); the full `*.stats.jsonl` dumps (about 20 MB) are kept locally only (`.gitignore`).

The validity criterion of pass 3 (constant power source, battery allowed) is a
deviation from the owner's criterion (AC for the whole pass), reported at
DETENTE 3.3.a.
