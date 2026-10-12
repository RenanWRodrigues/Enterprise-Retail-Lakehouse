# IoT detection — methodology and limits

Results: [REPORT.md](REPORT.md) and [metrics.json](metrics.json), both written by
`python -m iot_lakehouse.evaluation --output docs/iot/evaluation` from
`projects/industrial-iot-lakehouse/config/evaluation.toml`.

**Reproducibility, verified 2026-10-09:** two runs with the same configuration produced byte
for byte the same `metrics.json` (277 s and 309 s on the local machine).

Code: `projects/industrial-iot-lakehouse/src/iot_lakehouse/evaluation/`:

- `detector.py`: the detector;
- `matching.py`: matching and metrics;
- `run.py`: the pipeline;
- `report.py`: the report.

## 1. What the generator simulates (inspection)

| Category | Behaviour | Ground truth | Treated by |
|---|---|---|---|
| **Quality error** (the record is wrong) | physically impossible value, resend, late or out-of-order arrival, communication loss | `is_impossible`, `is_duplicate`, `is_late`, `is_out_of_order`, `offline_periods.csv` | Silver quality rules (quarantine and flags). Not an anomaly |
| **Operational anomaly** (the record is right; the behaviour is unusual) | spike (+6 to 12 σ on one reading), calibration drift (5–15%, linear over the episode) | `is_spike`, `drift_episode_id`, `drift_episodes.csv` | not targeted by this detector; drift is its main confounder |
| **Real failure** | quadratic degradation over 2–7 days on the sensors of the failure mode (+15% to +250% at the failure), then the stop | `failure_degradation.csv` (start of degradation, failure instant, sensor, effect), `failure_id` on readings | **the detector's target** |

Before this work there was **no detector**, only quality rules.

## 2. Detector (baseline, deterministic)

1. daily mean per sensor;
2. baseline = median of the sensor's previous 7 daily means within the same uninterrupted run (a
   day with no readings restarts the history);
3. alert when |mean − baseline| / |baseline| ≥ threshold, with at least 3 days of history;
4. an equipment alerts on a day when any of its sensors does; consecutive alert days form one
   alert event.

**Why this baseline:** it is explainable in one sentence, uses only the readings, and matches
what the data offers (slow multi-day degradation). Daily means remove the load cycle and most of
the noise. No model was introduced: there is no evidence that something more complex is needed.

## 3. Matching and metrics

- **Detection window** of a failure: from the start of its degradation (ground truth) to 24 h
  after the failure, on the same equipment.
- **One-to-one, greedy in time:** failures in order of occurrence, each taking the earliest
  unmatched alert event that intersects its window. One alert cannot count for two failures, and
  two alerts cannot both count for one.
- **Event level:**
  - TP = failures detected; FN = failures missed; FP = alert events matching no failure;
  - TN is undefined (there is no "negative event").
- **Equipment-day level:**
  - a day is positive when it intersects a detection window;
  - all four quadrants are computed, and the false positive rate is FP / (FP + TN);
  - only days with a detector decision (with readings) count.
- **Latency:** hours between the first day of the matching alert and the failure (median and
  minimum).
- **Zero denominator:** reported as *undefined*, never 0 or 1.

## 4. Leakage prevention

- The detector receives only `sensor_id`, `event_timestamp` and `reading_value` from the
  **Silver standardization** (`build_silver`), into which the generated readings enter without
  any label column.
- `refuse_labels` raises `LeakageError` if a ground-truth column reaches the detector, and a
  static test checks that `detector.py` imports nothing from the generator or the matching.
- **Calibration and evaluation on different equipment:** alternating within each equipment type,
  by id (8 + 8). The threshold is chosen on the calibration half (best event F1; a tie goes to
  the higher, more conservative threshold). The numbers reported for evaluation come only from
  the other half.

## 5. Results of the executed run (2026-10-09)

The numbers come from [REPORT.md](REPORT.md) (held-out equipment, threshold 0.2):

| Level | Result |
|---|---|
| Event | 10 failures, 8 detected, 2 missed, 0 false alarms: precision 1.000, recall 0.800, F1 0.889 |
| Equipment-day | TP 11, FP 0, TN 183, FN 38: precision 1.000, recall 0.224, FPR 0.000 |
| Latency | median warning 22.6 h before the failure; shortest 8.1 h |

How to read them:

- **The day-level recall is low by construction.** Degradation grows quadratically, so most of
  its first days differ from normal by less than the threshold. The detector sees the last day
  or so, which is the ~22 h of median warning.
- **The calibration sweep is flat from 0.05 to 0.20** (same F1). In this synthetic data the
  failure signatures are large next to the noise of a daily mean, so the threshold barely
  matters. That says more about the data than about the detector.
- **No false alarm came from the 6 drift episodes** at these thresholds: a slow linear drift is
  absorbed by the 7-day trailing baseline.

## 6. Limitations

- **Synthetic data with clean signatures.** These metrics measure the pipeline and the method on
  the generator's world. They are **not** an estimate of performance on real equipment, where
  degradations are subtler and noisier.
- **Small sample:** 23 failures, 10 of them in evaluation. One more or one fewer detection moves
  recall by 0.1. No confidence interval is reported.
- **Daily granularity:** alerts mark whole days; the latency resolution is one day.
- **Spikes are not detected** (one-reading transients vanish in a daily mean). That is a
  different detector (per reading, against the sensor's recent dispersion).
- **The detector uses the generator's readings through the Silver standardization, not Delta
  tables** (no Bronze/Silver persistence in the evaluation run). The transformations are the
  production ones.
- **The dataset is identified by a checksum of the generated readings and their labels**, not
  by a stored file version. The generator is deterministic for the same configuration.
