# syntax=docker/dockerfile:1.7
# Two images from one file:
#   producer: Python only, runs the ingestion feeds
#   spark:    adds a JRE and pre-fetched Spark packages, runs the streaming jobs

FROM python:3.12-slim-bookworm AS base
COPY --from=ghcr.io/astral-sh/uv:0.8.8 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1 \
    MLFLOW_DISABLE_AGENT_HINT=1 \
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
RUN uv sync --frozen --no-dev --extra spark --group quality --no-install-project
COPY contracts ./contracts
COPY src ./src
RUN uv sync --frozen --no-dev --extra spark --group quality
USER skypulse
# Resolve Delta, Kafka and S3A packages at build time so containers start offline.
RUN python -c "from skypulse.common.config import Settings; \
from skypulse.streaming.session import Connectors, build_session; \
build_session('warmup', Settings(), Connectors(kafka=True, s3a=True), master='local[1]').stop()"
# Replay fixtures (reference CSVs) for replay mode; last so code changes keep the package layer.
COPY data/fixtures ./data/fixtures
ENTRYPOINT ["python", "-m", "skypulse.streaming"]

FROM base AS dbt
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --group dbt --no-install-project
RUN mkdir /gold && chown skypulse /gold
COPY --chown=skypulse dbt ./dbt
WORKDIR /app/dbt
USER skypulse
RUN dbt deps
# Gold tables live in a DuckDB file on a mounted volume (see docker-compose).
ENV SKYPULSE_DUCKDB_PATH=/gold/skypulse.duckdb
ENTRYPOINT ["dbt"]

FROM base AS ml
# LightGBM needs the OpenMP runtime.
RUN apt-get update && apt-get install --yes --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --group ml --no-install-project
COPY contracts ./contracts
COPY src ./src
RUN uv sync --frozen --no-dev --group ml
USER skypulse
EXPOSE 8000
ENTRYPOINT ["python", "-m", "skypulse.ml.serving"]
