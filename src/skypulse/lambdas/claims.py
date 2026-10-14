"""EU261 claim records and where they are kept (DynamoDB)."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol

from skypulse.eu261.rules import Assessment
from skypulse.lambdas.disruption import Disruption


@dataclass(frozen=True)
class Claim:
    item: Mapping[str, Any]

    @classmethod
    def from_assessment(
        cls,
        disruption: Disruption,
        assessment: Assessment,
        assessed_at: datetime,
        correlation_id: str | None,
    ) -> "Claim":
        data = disruption.data
        case = disruption.flight_case()
        exemption_likely = assessment.extraordinary_circumstances_likely()
        return cls(
            {
                "claim_id": disruption.claim_id(),
                "flight_number": data["flight_number"],
                "flight_date": data["scheduled_departure_utc"][:10],
                "airline_icao": data["airline_icao"],
                "origin": data["origin"]["icao"],
                "destination": data["destination"]["icao"],
                "kind": data["kind"],
                "arrival_delay_minutes": data["arrival_delay_minutes"],
                "verdict": str(assessment.verdict),
                "distance_band": assessment.band.name.lower(),
                "distance_km": Decimal(str(round(case.route.distance_km(), 1))),
                "amount_eur": Decimal(str(assessment.compensation.amount_eur)),
                "reduced": assessment.compensation.reduced,
                "extraordinary_circumstances_likely": exemption_likely,
                "severe_weather": sorted(str(condition) for condition in assessment.severe_weather),
                "source_event_id": disruption.event_id,
                "detected_at": data["detected_at"],
                "assessed_at": assessed_at.isoformat(),
                "correlation_id": correlation_id,
            }
        )


class ClaimRepository(Protocol):
    def save_if_newer(self, claim: Claim) -> bool:
        """Store the claim unless a version detected at the same time or later exists."""
        ...


class DynamoDbClaims:
    def __init__(self, table: Any) -> None:
        self._table = table

    def save_if_newer(self, claim: Claim) -> bool:
        try:
            self._table.put_item(
                Item=dict(claim.item),
                ConditionExpression="attribute_not_exists(claim_id) OR detected_at < :detected_at",
                ExpressionAttributeValues={":detected_at": claim.item["detected_at"]},
            )
        except self._table.meta.client.exceptions.ConditionalCheckFailedException:
            return False
        return True
