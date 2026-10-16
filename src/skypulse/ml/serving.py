"""Delay prediction API: ``python -m skypulse.ml.serving`` (port 8000).

On startup it loads the ``champion`` version of the registered model and the latest
route/airline statistics. If either is unavailable the service still starts and reports
itself unhealthy, so an orchestrator can retry instead of crash-looping.
"""

import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime

import mlflow
import pandas as pd
import uvicorn
from deltalake import DeltaTable
from fastapi import FastAPI, HTTPException
from prometheus_client import Counter, Histogram, make_asgi_app
from pydantic import AwareDatetime, BaseModel, Field

from skypulse.common.config import LakeLayer, Settings
from skypulse.common.logging import configure_logging, get_logger
from skypulse.ml.feature_spec import FEATURE_SET_VERSION
from skypulse.ml.model import OUTPUT_COLUMNS, DelayModel
from skypulse.ml.online import FlightQuery, Statistics, WeatherReport
from skypulse.ml.registry import CHAMPION
from skypulse.observability.lake import storage_options

ICAO_AIRPORT = r"^[A-Z0-9]{4}$"
ICAO_AIRLINE = r"^[A-Z]{3}$"
PORT = 8000

REQUESTS = Counter("skypulse_api_requests", "Prediction API requests", ["endpoint", "status"])
LATENCY = Histogram(
    "skypulse_api_latency_seconds",
    "Prediction latency",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1),
)
PREDICTED = Histogram(
    "skypulse_api_predicted_probability",
    "Predicted delay probabilities",
    ["label"],
    buckets=(0.05, 0.1, 0.2, 0.3, 0.5, 0.7, 0.9),
)

_log = get_logger(__name__)


class Weather(BaseModel):
    visibility_m: int | None = Field(default=None, ge=0, le=9999)
    wind_speed_kt: int | None = Field(default=None, ge=0, le=200)
    wind_gust_kt: int | None = Field(default=None, ge=0, le=250)
    present_weather: str | None = Field(default=None, max_length=40)

    def report(self) -> WeatherReport:
        return WeatherReport(
            self.visibility_m, self.wind_speed_kt, self.wind_gust_kt, self.present_weather
        )


class PredictionRequest(BaseModel):
    flight_number: str = Field(pattern=r"^[A-Z0-9]{2,3}[0-9]{1,4}[A-Z]?$")
    airline_icao: str = Field(pattern=ICAO_AIRLINE)
    origin_icao: str = Field(pattern=ICAO_AIRPORT)
    destination_icao: str = Field(pattern=ICAO_AIRPORT)
    scheduled_departure_utc: AwareDatetime
    origin_weather: Weather | None = None
    destination_weather: Weather | None = None

    def query(self) -> FlightQuery:
        return FlightQuery(
            self.origin_icao,
            self.destination_icao,
            self.airline_icao,
            self.scheduled_departure_utc,
            self.origin_weather.report() if self.origin_weather else None,
            self.destination_weather.report() if self.destination_weather else None,
        )


class PredictionResponse(BaseModel):
    flight_number: str
    probability_delayed_15: float
    probability_delayed_180: float
    model_version: str
    feature_set_version: str
    predicted_at: datetime


@dataclass(frozen=True)
class LoadedModel:
    model: DelayModel
    version: str


ModelLoader = Callable[[], LoadedModel | None]
StatisticsLoader = Callable[[], Statistics]


def champion_loader(settings: Settings) -> ModelLoader:
    def load() -> LoadedModel | None:
        try:
            client = mlflow.MlflowClient(settings.ml.tracking_uri)
            version = client.get_model_version_by_alias(settings.ml.model_name, CHAMPION).version
            mlflow.set_tracking_uri(settings.ml.tracking_uri)
            loaded = mlflow.pyfunc.load_model(f"models:/{settings.ml.model_name}@{CHAMPION}")
        except Exception:  # registry down or no champion yet: unhealthy, not dead
            _log.exception("model_load_failed")
            return None
        return LoadedModel(loaded.unwrap_python_model(), str(version))

    return load


def statistics_loader(settings: Settings) -> StatisticsLoader:
    def load() -> Statistics:
        uri = f"{settings.lake.uri_for(LakeLayer.GOLD)}/flight_feature_stats".replace(
            "s3a://", "s3://", 1
        )
        try:
            frame = DeltaTable(uri, storage_options=storage_options(settings)).to_pandas()
        except Exception:  # serving without history still works; features become missing
            _log.exception("statistics_load_failed")
            return Statistics.empty()
        return Statistics.from_frame(frame)

    return load


class Service:
    def __init__(self, load_model: ModelLoader, load_statistics: StatisticsLoader) -> None:
        self._load_model = load_model
        self._load_statistics = load_statistics
        self.model: LoadedModel | None = None
        self.statistics = Statistics.empty()

    def start(self) -> None:
        self.model = self._load_model()
        self.statistics = self._load_statistics()

    def predict(self, request: PredictionRequest) -> PredictionResponse:
        if self.model is None:
            raise HTTPException(status_code=503, detail="no model loaded")
        features = request.query().features(self.statistics)
        probabilities = self.model.model.predict_frame(features).iloc[0]
        for label, column in OUTPUT_COLUMNS.items():
            PREDICTED.labels(label).observe(float(probabilities[column]))
        return PredictionResponse(
            flight_number=request.flight_number,
            probability_delayed_15=round(float(probabilities[OUTPUT_COLUMNS["delayed_15"]]), 4),
            probability_delayed_180=round(float(probabilities[OUTPUT_COLUMNS["delayed_180"]]), 4),
            model_version=self.model.version,
            feature_set_version=FEATURE_SET_VERSION,
            predicted_at=pd.Timestamp.now(tz="UTC").to_pydatetime(),
        )


def create_app(service: Service) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        service.start()
        yield

    app = FastAPI(title="SkyPulse delay prediction", version="1.0.0", lifespan=lifespan)
    app.mount("/metrics", make_asgi_app())

    @app.get("/health")
    def health() -> dict[str, str]:
        if service.model is None:
            REQUESTS.labels("health", "503").inc()
            raise HTTPException(status_code=503, detail="no model loaded")
        REQUESTS.labels("health", "200").inc()
        return {
            "status": "ok",
            "model_version": service.model.version,
            "feature_set_version": FEATURE_SET_VERSION,
        }

    @app.post("/predict")
    def predict(request: PredictionRequest) -> PredictionResponse:
        started = time.perf_counter()
        try:
            response = service.predict(request)
        except HTTPException as error:
            REQUESTS.labels("predict", str(error.status_code)).inc()
            raise
        LATENCY.observe(time.perf_counter() - started)
        REQUESTS.labels("predict", "200").inc()
        return response

    return app


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    service = Service(champion_loader(settings), statistics_loader(settings))
    uvicorn.run(create_app(service), host="0.0.0.0", port=PORT)  # noqa: S104 - container port


if __name__ == "__main__":
    main()
