"""Correlation ids that follow one event from producer to stream job to Lambda.

The id travels in message headers between processes and lives in a context variable
inside a process, so every log line written while handling an event carries it.
"""

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass


@dataclass(frozen=True)
class CorrelationId:
    value: str

    def __post_init__(self) -> None:
        if not self.value.strip():
            raise ValueError("correlation id must not be blank")

    @classmethod
    def new(cls) -> "CorrelationId":
        return cls(uuid.uuid4().hex)

    def __str__(self) -> str:
        return self.value


_current: ContextVar[CorrelationId | None] = ContextVar("correlation_id", default=None)


def current_correlation_id() -> CorrelationId | None:
    return _current.get()


@contextmanager
def correlation_scope(correlation_id: CorrelationId | None = None) -> Iterator[CorrelationId]:
    """Bind a correlation id for the duration of the block, generating one if absent."""
    bound = correlation_id or CorrelationId.new()
    token = _current.set(bound)
    try:
        yield bound
    finally:
        _current.reset(token)
