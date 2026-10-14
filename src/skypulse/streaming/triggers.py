"""How often a streaming query runs: continuously on an interval, or drain-and-stop."""

from dataclasses import dataclass

from pyspark.sql.streaming.readwriter import DataStreamWriter


@dataclass(frozen=True)
class Trigger:
    """``available_now`` processes everything pending and stops (batch-style runs and tests)."""

    available_now: bool = False
    interval: str = "30 seconds"

    def apply(self, writer: DataStreamWriter) -> DataStreamWriter:
        if self.available_now:
            return writer.trigger(availableNow=True)
        return writer.trigger(processingTime=self.interval)
