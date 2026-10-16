"""Silver table specifications, free of Spark so lightweight processes can use them.

Each table names its event type, event-time column and watermark, natural key and update
rule. The streaming jobs (``skypulse.streaming.silver``) turn these into queries.
"""

from dataclasses import dataclass

from skypulse.contracts import EventType
from skypulse.ingestion.publisher import DEFAULT_ROUTES


@dataclass(frozen=True)
class EventTiming:
    """Which column is event time and how late an event may arrive."""

    expression: str
    watermark: str


@dataclass(frozen=True)
class MergePolicy:
    """Natural key, and the column that decides whether an update is newer (None: insert-only)."""

    keys: tuple[str, ...]
    newer_by: str | None = None

    def condition(self) -> str:
        return " AND ".join(f"target.{key} <=> source.{key}" for key in self.keys)

    def update_condition(self) -> str | None:
        if self.newer_by is None:
            return None
        return f"source.{self.newer_by} > target.{self.newer_by}"


@dataclass(frozen=True)
class SilverTable:
    name: str
    event_type: EventType
    timing: EventTiming
    policy: MergePolicy

    def topic(self) -> str:
        return str(DEFAULT_ROUTES.topic_for(self.event_type))


SILVER_TABLES = (
    SilverTable(
        "aircraft_states",
        EventType.AIRCRAFT_STATE,
        EventTiming("timestamp_seconds(last_contact)", "10 minutes"),
        MergePolicy(("icao24", "last_contact")),
    ),
    SilverTable(
        "observed_flights",
        EventType.OBSERVED_FLIGHT,
        EventTiming("first_seen", "2 days"),
        MergePolicy(("icao24", "callsign", "first_seen"), newer_by="last_seen"),
    ),
    SilverTable(
        "flight_schedules",
        EventType.FLIGHT_SCHEDULE,
        EventTiming("observed_at", "1 hour"),
        MergePolicy(("flight_number", "scheduled_departure_utc"), newer_by="observed_at"),
    ),
    SilverTable(
        "metar_observations",
        EventType.METAR_OBSERVATION,
        EventTiming("observed_at", "3 hours"),
        MergePolicy(("station_icao", "observed_at", "report_type")),
    ),
)
