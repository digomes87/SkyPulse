"""Structured JSON logging shared by producers, stream jobs, Lambdas and services."""

import logging

import structlog
from structlog.typing import EventDict, WrappedLogger

from skypulse.common.config import LogLevel
from skypulse.common.correlation import current_correlation_id


def _add_correlation_id(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    correlation_id = current_correlation_id()
    if correlation_id is None:
        return event_dict
    return {**event_dict, "correlation_id": str(correlation_id)}


def configure_logging(level: LogLevel) -> None:
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            _add_correlation_id,
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelNamesMapping()[level]),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str) -> structlog.typing.FilteringBoundLogger:
    """Lazy logger: safe to create at import time, binds to the configuration on first use."""
    logger: structlog.typing.FilteringBoundLogger = structlog.get_logger(component=name)
    return logger
