"""SparkSession with Delta Lake, and Kafka/S3A connectors when a job needs them."""

from dataclasses import dataclass

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from skypulse.common.config import RunMode, Settings

SPARK_SCALA_VERSION = "2.13"
SPARK_VERSION = "4.0.1"
HADOOP_VERSION = "3.4.1"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_{SPARK_SCALA_VERSION}:{SPARK_VERSION}"
S3A_PACKAGE = f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}"


@dataclass(frozen=True)
class Connectors:
    kafka: bool = False
    s3a: bool = False

    def packages(self) -> list[str]:
        wanted = {KAFKA_PACKAGE: self.kafka, S3A_PACKAGE: self.s3a}
        return [package for package, enabled in wanted.items() if enabled]


def _s3a_options(settings: Settings) -> dict[str, str]:
    if settings.run_mode is RunMode.AWS:
        return {}
    endpoint = settings.aws_endpoint_url or ""
    return {
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(endpoint.startswith("https")).lower(),
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        "spark.hadoop.fs.s3a.access.key": "test",
        "spark.hadoop.fs.s3a.secret.key": "test",
    }


def build_session(
    app_name: str, settings: Settings, connectors: Connectors, master: str | None = None
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", settings.spark.driver_memory)
        .config("spark.ui.showConsoleProgress", "false")
        # Silver tables feed the disruption detector through their change data feed.
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    )
    if master is not None:
        builder = builder.master(master)
    if connectors.s3a:
        for key, value in _s3a_options(settings).items():
            builder = builder.config(key, value)
    configured = configure_spark_with_delta_pip(builder, extra_packages=connectors.packages())
    return configured.getOrCreate()
