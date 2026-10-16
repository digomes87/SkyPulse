from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from skypulse.ml.feature_spec import FEATURE_SET_VERSION, NUMERIC_FEATURES
from skypulse.ml.online import Statistics
from skypulse.ml.serving import LoadedModel, Service, create_app
from skypulse.ml.training import train

REQUEST = {
    "flight_number": "FR1234",
    "airline_icao": "RYR",
    "origin_icao": "EIDW",
    "destination_icao": "LEMD",
    "scheduled_departure_utc": "2026-10-14T09:15:00Z",
    "origin_weather": {"visibility_m": 300, "wind_speed_kt": 4, "present_weather": "FG"},
}


@pytest.fixture(scope="module")
def model() -> LoadedModel:
    rng = np.random.default_rng(7)
    size = 600
    frame = pd.DataFrame({name: rng.normal(size=size) for name in NUMERIC_FEATURES})
    frame["origin_icao"] = rng.choice(["EIDW", "LEMD"], size)
    frame["destination_icao"] = rng.choice(["LEMD", "EGLL"], size)
    frame["airline_icao"] = rng.choice(["RYR", "EIN"], size)
    frame["delayed_15"] = (
        frame["origin_thunderstorm"] + rng.normal(scale=0.5, size=size) > 0.5
    ).astype(int)
    frame["delayed_180"] = (frame["origin_thunderstorm"] > 1.2).astype(int)
    frame["scheduled_departure_utc"] = [
        datetime(2026, 7, 1, tzinfo=UTC) + timedelta(hours=i) for i in range(size)
    ]
    frame["feature_set_version"] = FEATURE_SET_VERSION
    return LoadedModel(train(frame).model, "7")


def client(model: LoadedModel | None) -> TestClient:
    return TestClient(create_app(Service(lambda: model, Statistics.empty)))


def test_health_reports_the_model_version(model: LoadedModel) -> None:
    with client(model) as api:
        response = api.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "model_version": "7",
        "feature_set_version": FEATURE_SET_VERSION,
    }


def test_prediction_returns_both_probabilities(model: LoadedModel) -> None:
    with client(model) as api:
        body = api.post("/predict", json=REQUEST).json()

    assert body["flight_number"] == "FR1234"
    assert 0 <= body["probability_delayed_15"] <= 1
    assert 0 <= body["probability_delayed_180"] <= 1
    assert body["model_version"] == "7"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("origin_icao", "dublin"),
        ("scheduled_departure_utc", "2026-10-14T09:15:00"),
        ("flight_number", "x"),
    ],
)
def test_invalid_requests_are_rejected(model: LoadedModel, field: str, value: str) -> None:
    with client(model) as api:
        response = api.post("/predict", json={**REQUEST, field: value})

    assert response.status_code == 422


def test_without_a_model_the_service_is_unhealthy_but_up() -> None:
    with client(None) as api:
        assert api.get("/health").status_code == 503
        assert api.post("/predict", json=REQUEST).status_code == 503


def test_metrics_are_exposed(model: LoadedModel) -> None:
    with client(model) as api:
        api.post("/predict", json=REQUEST)
        metrics = api.get("/metrics/").text

    assert "skypulse_api_requests_total" in metrics
    assert "skypulse_api_predicted_probability" in metrics
