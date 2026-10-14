# syntax=docker/dockerfile:1.7
# Two images from one file:
#   producer: Python only, runs the ingestion feeds
#   spark:    adds a JRE and pre-fetched Spark packages, runs the streaming jobs

FROM python:3.12-slim-bookworm AS base
COPY --from=ghcr.io/astral-sh/uv:0.8.8 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    SKYPULSE_INGESTION__FIXTURES_DIR=/app/data/fixtures
WORKDIR /app
RUN useradd --create-home --uid 10001 skypulse

FROM base AS producer
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY contracts ./contracts
COPY src ./src
RUN uv sync --frozen --no-dev
COPY data/fixtures ./data/fixtures
USER skypulse
ENTRYPOINT ["python", "-m", "skypulse.ingestion"]

FROM base AS spark
RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --extra spark --no-install-project
COPY contracts ./contracts
COPY src ./src
RUN uv sync --frozen --no-dev --extra spark
USER skypulse
# Resolve Delta, Kafka and S3A packages at build time so containers start offline.
RUN python -c "from skypulse.common.config import Settings; \
from skypulse.streaming.session import Connectors, build_session; \
build_session('warmup', Settings(), Connectors(kafka=True, s3a=True), master='local[1]').stop()"
ENTRYPOINT ["python", "-m", "skypulse.streaming"]
