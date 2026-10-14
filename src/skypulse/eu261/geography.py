"""Where the regulation applies and how far a flight is."""

import math
import re
from dataclasses import dataclass

EARTH_RADIUS_KM = 6371.0
_ISO_COUNTRY = re.compile(r"^[A-Z]{2}$")


@dataclass(frozen=True)
class CountryCode:
    """ISO 3166-1 alpha-2."""

    value: str

    def __post_init__(self) -> None:
        if not _ISO_COUNTRY.fullmatch(self.value):
            raise ValueError(f"not an ISO 3166-1 alpha-2 code: {self.value!r}")


@dataclass(frozen=True)
class Coordinates:
    latitude: float
    longitude: float

    def __post_init__(self) -> None:
        if not -90 <= self.latitude <= 90:
            raise ValueError(f"latitude out of range: {self.latitude}")
        if not -180 <= self.longitude <= 180:
            raise ValueError(f"longitude out of range: {self.longitude}")


def great_circle_km(origin: Coordinates, destination: Coordinates) -> float:
    """Haversine distance, the method the regulation prescribes (Art. 7(4))."""
    lat1, lat2 = math.radians(origin.latitude), math.radians(destination.latitude)
    delta_lat = lat2 - lat1
    delta_lon = math.radians(destination.longitude - origin.longitude)
    inner = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(inner)))


# EU member states, their outermost regions with own ISO codes, EEA (IS, LI, NO),
# Switzerland (bilateral agreement) and the United Kingdom (retained as UK261).
_EU_MEMBERS = (
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE",
    "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE",
)  # fmt: skip
_OUTERMOST_REGIONS = ("GP", "GF", "MQ", "RE", "YT", "MF")
_EEA_SWITZERLAND_UK = ("IS", "LI", "NO", "CH", "GB")

EU261_COUNTRIES = frozenset(
    CountryCode(code) for code in (*_EU_MEMBERS, *_OUTERMOST_REGIONS, *_EEA_SWITZERLAND_UK)
)
