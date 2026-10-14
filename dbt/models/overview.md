{% docs __overview__ %}
# SkyPulse gold layer

Query-oriented models over the SkyPulse lakehouse: flights, routes, airports and the
EU261 compensation each disruption could cost.

**Lineage:** OpenSky, AeroDataBox and aviationweather.gov are ingested to Kafka, land in
the bronze Delta table, are typed and deduplicated in silver by Spark Structured
Streaming, and modelled here with dbt. EU261 assessments come from the same pure Python
rules engine the claims Lambda uses (`skypulse.eu261`).

| Layer | Models |
|-------|--------|
| Staging | `stg_*`: thin views over the silver Delta tables |
| Dimensions | `dim_airport`, `dim_airline`, `dim_route`, `dim_date` |
| Facts | `fct_flight` (one row per flight), `fct_eu261_claim` (one row per assessed disruption) |
| Aggregates | `agg_airport_daily_delays`, `agg_route_otp`, `agg_eu261_exposure_by_airline` |

The data in this published build comes from the replay fixtures: real OpenSky positions and
METAR reports recorded on 2026-10-14, and a synthetic, seeded schedule day. See the
[repository](https://github.com/digomes87/SkyPulse) for the code and the decision records.
{% enddocs %}

{% docs physical_layout %}
**Physical layout.** Locally these are DuckDB tables. On Databricks the larger tables use
liquid clustering instead of Hive-style partitions: `fct_flight` on (`flight_date`,
`origin_icao`), the columns nearly every query filters on, and `agg_airport_daily_delays`
on `flight_date`. Data volumes per day are far below the size where partitioning pays off
(roughly 1 GB per partition), and clustering keeps file skipping without the small-file
cost. Silver tables are Z-ordered by the batch maintenance job on their join keys.
{% enddocs %}
