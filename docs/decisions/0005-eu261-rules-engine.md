# 0005. EU261 rules engine as a pure library

- Status: Accepted
- Date: 2026-10-14

## Context

The EU261 assessment is the core business logic of SkyPulse: whether a disrupted flight
is covered, eligible, and for how much. It needs to run in several places: in a Lambda
for each disruption, in batch over history (dbt and Spark backfills), and in notebooks for
analysis. Legal rules also change through court rulings, so the logic has to be easy to
read, test and change.

## Decision

1. **`skypulse.eu261` is pure and standard-library only.** No I/O, no clock, no AWS, no
   Spark. Inputs are small immutable value objects (`Airport`, `Carrier`, `Route`,
   `FlightCase`, `WeatherObservation`); the output is an `Assessment`.
2. **Callers gather the facts.** The detector enriches each disruption with airport
   coordinates and countries, the carrier's country and the METAR reports near departure and
   arrival, and publishes all of it in the `flight_disruption` event. The calculator Lambda
   only turns the event into a `FlightCase`, calls `assess` and writes the result.
3. **The Lambda zip contains just `skypulse.eu261` and `skypulse.lambdas`.** A unit test
   imports the handlers with `python -I -S` to prove nothing else is needed.
4. **Rules implemented (simplified, documented in `rules.py`):**
   - Covered: departure from an EU/EEA/UK/CH airport, or arrival there on a carrier based in
     that area.
   - Eligible: arrival delay of 3 hours or more, or cancellation notified less than 14 days
     before departure (notice is measured from when SkyPulse first saw the cancellation).
   - Amount by great-circle distance: EUR 250 up to 1,500 km; EUR 400 for intra-EU flights
     above 1,500 km and other flights up to 3,500 km; EUR 600 beyond.
   - Rerouting that arrives within 2/3/4 hours (by band) halves the amount. Modelled as an
     optional input, off by default because the feeds do not report rerouting.
5. **Weather is a flag, not a verdict.** `extraordinary_circumstances_likely` is set when a
   METAR within an hour of the scheduled time shows a thunderstorm, freezing precipitation,
   visibility below 800 m or a crosswind above 35 kt (thresholds in `WeatherPolicy`). The
   amount is still computed; the claim records both, because whether an exemption applies is
   decided case by case. Without runway headings the crosswind is the full peak wind, the
   worst case.
6. **Property-based tests (Hypothesis)** pin the invariants: the exact 180-minute boundary,
   amounts always in the regulation's set, bands consistent with distance and scope, amounts
   never decreasing with distance, and the distance function being a metric.

## Consequences

- The same function scores live disruptions and history; a rule change is one pull request
  with its tests, and a backfill can recompute every claim.
- Simplifications (connecting flights, denied boarding, the full Art. 5(1)(c) rerouting
  rules, airline-reported causes) are out of scope and listed in the module docstring.
- Reference data quality bounds the result. Unknown airports make a disruption
  non-assessable; the calculator logs and skips it instead of guessing.
