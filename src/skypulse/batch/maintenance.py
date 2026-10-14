"""Delta maintenance: compact small files, cluster on query keys, drop old files.

Streaming micro-batches leave many small files. OPTIMIZE with Z-ordering on the columns
the gold models filter and join on keeps reads fast; VACUUM keeps a week of history for
time travel and debugging, then removes unreferenced files.
"""

from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from skypulse.common.config import LakeLayer, Settings
from skypulse.common.logging import get_logger
from skypulse.streaming.bronze import bronze_path

RETENTION_HOURS = 168

_log = get_logger(__name__)


@dataclass(frozen=True)
class MaintainedTable:
    layer: LakeLayer
    name: str
    zorder_by: tuple[str, ...] = ()


MAINTAINED_TABLES = (
    MaintainedTable(LakeLayer.SILVER, "aircraft_states", ("icao24",)),
    MaintainedTable(
        LakeLayer.SILVER, "flight_schedules", ("origin_icao", "scheduled_departure_utc")
    ),
    MaintainedTable(LakeLayer.SILVER, "metar_observations", ("station_icao", "observed_at")),
    MaintainedTable(LakeLayer.SILVER, "observed_flights", ("icao24",)),
    MaintainedTable(LakeLayer.SILVER, "flight_disruptions", ("flight_number",)),
    MaintainedTable(LakeLayer.SILVER, "quarantine"),
)


def table_path(settings: Settings, table: MaintainedTable) -> str:
    return f"{settings.lake.uri_for(table.layer)}/{table.name}"


def maintain(spark: SparkSession, path: str, zorder_by: tuple[str, ...]) -> bool:
    if not DeltaTable.isDeltaTable(spark, path):
        return False
    table = DeltaTable.forPath(spark, path)
    optimizer = table.optimize()
    if zorder_by:
        optimizer.executeZOrderBy(*zorder_by)
    else:
        optimizer.executeCompaction()
    table.vacuum(RETENTION_HOURS)
    return True


def run_maintenance(spark: SparkSession, settings: Settings) -> list[str]:
    targets = [(table_path(settings, table), table.zorder_by) for table in MAINTAINED_TABLES]
    targets.append((bronze_path(settings), ()))
    done = [path for path, zorder in targets if maintain(spark, path, zorder)]
    _log.info("maintenance_completed", tables=done)
    return done
