"""Validates the DAG folder inside the Airflow image: ``make airflow-validate``.

Fails on import errors, on missing or unexpected DAGs, on the wrong task order for the
lakehouse refresh, and on DAGs without retries, tags or a schedule.
"""

import sys

from airflow.dag_processing.dagbag import DagBag

DAGS_FOLDER = "/opt/airflow/dags"

EXPECTED_DAGS = {
    "reference_data",
    "metar_refresh",
    "lake_to_gold",
    "lake_maintenance",
    "model_retraining",
}
LAKE_TO_GOLD_UPSTREAM = {
    "bronze": set(),
    "silver": {"bronze"},
    "backfill_silver": {"silver"},
    "quality_silver": {"backfill_silver"},
    "detect_disruptions": {"quality_silver"},
    "recompute_assessments": {"detect_disruptions"},
    "dbt_build": {"recompute_assessments"},
    "quality_gold": {"dbt_build"},
    "quality_reconciliation": {"dbt_build"},
    "quality_statistics": {"dbt_build"},
}


def problems(bag: DagBag) -> list[str]:
    found = [f"import error in {path}: {error}" for path, error in bag.import_errors.items()]
    if set(bag.dag_ids) != EXPECTED_DAGS:
        found.append(f"expected DAGs {sorted(EXPECTED_DAGS)}, found {sorted(bag.dag_ids)}")
    for dag_id, dag in bag.dags.items():
        if not dag.tags:
            found.append(f"{dag_id} has no tags")
        if type(dag.timetable).__name__ == "NullTimetable":
            found.append(f"{dag_id} has no schedule")
        found.extend(
            f"{dag_id}.{task.task_id} has no retries" for task in dag.tasks if not task.retries
        )
    retraining = bag.dags.get("model_retraining")
    if retraining is not None and retraining.get_task("train_and_promote").upstream_task_ids != {
        "build_features"
    }:
        found.append("model_retraining must build features before training")
    lake = bag.dags.get("lake_to_gold")
    if lake is not None:
        upstream = {task.task_id: set(task.upstream_task_ids) for task in lake.tasks}
        if upstream != LAKE_TO_GOLD_UPSTREAM:
            found.append(f"lake_to_gold dependencies are {upstream}")
    found.extend(
        f"{dag_id} has no failure alert"
        for dag_id, dag in bag.dags.items()
        if any(task.on_failure_callback is None for task in dag.tasks)
    )
    return found


def main() -> int:
    # The DAG processor puts the DAGs folder on sys.path; do the same here.
    sys.path.insert(0, DAGS_FOLDER)
    bag = DagBag(dag_folder=DAGS_FOLDER)
    found = problems(bag)
    for problem in found:
        print(f"FAIL {problem}")
    if not found:
        print(f"OK {len(bag.dag_ids)} DAGs: {', '.join(sorted(bag.dag_ids))}")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
