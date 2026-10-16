# Model card: SkyPulse arrival-delay model

| | |
|---|---|
| **Registered model** | `skypulse-delay` (MLflow), served alias `champion` |
| **Type** | Two LightGBM binary classifiers in one MLflow pyfunc (with its preprocessing) |
| **Predicts** | Probability that a flight arrives at least **15 minutes** late, and at least **180 minutes** late (the EU261 threshold) |
| **Prediction time** | Two hours before the scheduled departure |
| **Feature set** | `v1` (`src/skypulse/ml/feature_spec.py`) |
| **Owner** | Data platform |
| **Code** | `skypulse.ml` (features, training, registry, serving); weekly DAG `model_retraining` |

## Intended use

Ranking flights by delay risk ahead of departure, for operational awareness and to
estimate EU261 exposure before it materialises. It is not a basis for decisions about
individual passengers, and its probabilities feed analytics, not compensation decisions
(those come from the rules engine on actual outcomes).

## Data

> **The training data is synthetic.** Live scheduled-versus-actual times require a paid
> provider (TD-003). The history is a seeded simulation (`skypulse.ml.history`) of 120 days,
> 60 flights a day on a fixed timetable across 10 European airports, with hourly METAR.
> It is loaded through the same bronze → silver path and contracts as live data, so the
> pipeline is real; the numbers below describe how well the model recovers the simulated
> causes, not real-world accuracy.

Delay causes built into the simulation: knock-on delays through the day, airline and route
effects, busier Fridays and Sundays, storms (with a long tail), fog and gusts at the origin,
storms at the destination, and rare random disruptions that no feature can predict.

## Features

| Group | Features | Point-in-time rule |
|-------|----------|--------------------|
| Identity | origin, destination, airline (categorical) | Known from the schedule |
| Calendar | departure hour (UTC), day of week | Known from the schedule |
| Route | great-circle distance | Static reference data |
| Weather | visibility, wind, gust, thunderstorm and precipitation flags at origin and destination | Latest METAR reported **before** prediction time, at most 3 hours old |
| History | route mean arrival delay, route 15-minute delay rate, route flight count, airline 15-minute delay rate | Only flights that **had landed before** prediction time, over the previous 30 days |

Labels exist only for landed flights; cancelled flights are excluded (a different problem).

## Training and evaluation

- **Split by time:** the newest 20% of flights validate; nothing from the validation period
  is seen in training. Early stopping uses the last 10% of the training period.
- **Baseline:** the route's own recent delay statistics used directly as the score.
- **Promotion gate:** a candidate becomes `champion` only if, on the same validation flights,
  it beats the current champion's 15-minute PR-AUC by at least 0.002 and loses no more than
  0.01 on the 180-minute PR-AUC.

Validation results (synthetic history, 1,442 validation flights):

| Label | Positive rate | ROC-AUC | PR-AUC | Baseline ROC-AUC | Baseline PR-AUC |
|-------|--------------:|--------:|-------:|-----------------:|----------------:|
| ≥ 15 min | 47.7% | 0.78 | 0.78 | 0.66 | 0.63 |
| ≥ 180 min | 3.2% | 0.68 | 0.34 | 0.53 | 0.04 |

The 180-minute model started with no signal (ROC-AUC 0.47): its positives came mostly from
random disruptions and the default leaf size could not isolate the few storm days. Giving
storms a long tail and the rare label smaller leaves produced the numbers above.

## Leakage considerations

- Weather after prediction time is excluded, including reports between prediction and
  departure; destination weather is the latest report before prediction time, not the
  conditions at arrival (a forecast, TAF, would be the right input and is not ingested).
- History counts only flights already landed at prediction time; a flight never contributes
  to its own features (tests in `tests/unit/ml/test_features.py`).
- Online features are produced by `skypulse.ml.online`, the serving twin of the feature job;
  a test builds one flight both ways and requires identical values.

## Limitations

- Synthetic data: real delays have causes the simulation lacks (air traffic control
  restrictions, crew and aircraft rotations, strikes) and correlations it doesn't model.
- 180-minute delays remain hard: about 3% positives, and part of them is unpredictable by
  construction. Use the probabilities for ranking and aggregate exposure, not as calibrated
  per-flight estimates.
- Departure hour is in UTC, so the same local hour shifts with daylight saving time.
- Unseen airports or airlines become missing categories; predictions still return but rely on
  the remaining features.

## Monitoring

The API exports request counts, latency and the distribution of predicted probabilities
(`skypulse_api_*`), so a shift in the score distribution is visible in Grafana. Retraining
runs weekly; rejected candidates stay registered with the reason as a tag.
