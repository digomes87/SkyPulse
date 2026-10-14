"""Builds DockerOperator tasks that run the SkyPulse images.

Airflow only orchestrates: every task is a short-lived container from the same images
`make demo` uses, on the stack's network, with the SkyPulse settings passed through from
the scheduler's environment. In AWS the same tasks become ECS or EMR Serverless jobs.
"""

import os
from datetime import timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

SPARK_IMAGE = "skypulse-spark:local"
PRODUCER_IMAGE = "skypulse-producer:local"
DBT_IMAGE = "skypulse-dbt:local"
GOLD_VOLUME = "skypulse_gold"
PASSED_THROUGH = ("SKYPULSE_", "AWS_")
SPARK_MASTER = ["--master", "local[2]"]

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}


def task_environment() -> dict[str, str]:
    return {
        name: value
        for name, value in os.environ.items()
        if name.startswith(PASSED_THROUGH) and not name.startswith("SKYPULSE_DOCKER_")
    }


def container_task(
    task_id: str, image: str, command: list[str], mounts: list[Mount] | None = None
) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        environment=task_environment(),
        docker_url=os.environ.get("SKYPULSE_DOCKER_URL", "unix://var/run/docker.sock"),
        network_mode=os.environ.get("SKYPULSE_DOCKER_NETWORK", "bridge"),
        mounts=mounts or [],
        auto_remove="success",
        mount_tmp_dir=False,
        tty=False,
    )


def spark_task(task_id: str, *arguments: str) -> DockerOperator:
    return container_task(task_id, SPARK_IMAGE, [*arguments, *SPARK_MASTER])


def batch_task(task_id: str, job: str) -> DockerOperator:
    command = ["-m", "skypulse.batch", job, *SPARK_MASTER]
    operator = container_task(task_id, SPARK_IMAGE, command)
    operator.entrypoint = ["python"]
    return operator


def producer_task(task_id: str, feed: str) -> DockerOperator:
    return container_task(task_id, PRODUCER_IMAGE, [feed, "--cycles", "1"])


def dbt_task(task_id: str, *arguments: str) -> DockerOperator:
    gold = Mount(target="/gold", source=GOLD_VOLUME, type="volume")
    return container_task(task_id, DBT_IMAGE, list(arguments), mounts=[gold])
