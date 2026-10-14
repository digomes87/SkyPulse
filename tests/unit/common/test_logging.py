import json

import pytest
import structlog

from skypulse.common.correlation import CorrelationId, correlation_scope, current_correlation_id
from skypulse.common.logging import configure_logging, get_logger


def last_json_line(output: str) -> dict[str, object]:
    lines = [line for line in output.splitlines() if line.strip()]
    parsed: dict[str, object] = json.loads(lines[-1])
    return parsed


def test_correlation_id_is_unset_outside_a_scope() -> None:
    assert current_correlation_id() is None


def test_scope_binds_and_restores_the_correlation_id() -> None:
    outer = CorrelationId.new()
    inner = CorrelationId("inner-id")

    with correlation_scope(outer):
        with correlation_scope(inner):
            assert current_correlation_id() == inner
        assert current_correlation_id() == outer

    assert current_correlation_id() is None


def test_scope_without_argument_generates_a_fresh_id() -> None:
    with correlation_scope() as correlation_id:
        assert current_correlation_id() == correlation_id
        assert len(str(correlation_id)) == 32


def test_correlation_id_rejects_blank_values() -> None:
    with pytest.raises(ValueError, match="blank"):
        CorrelationId("  ")


def test_log_lines_are_json_with_level_logger_and_event(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO")

    get_logger("skypulse.test").info("flight_detected", icao24="4ca7b5")

    line = last_json_line(capsys.readouterr().out)
    assert line["event"] == "flight_detected"
    assert line["level"] == "info"
    assert line["component"] == "skypulse.test"
    assert line["icao24"] == "4ca7b5"
    assert "timestamp" in line


def test_log_lines_carry_the_active_correlation_id(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")

    with correlation_scope(CorrelationId("abc123")):
        get_logger("skypulse.test").info("inside_scope")

    assert last_json_line(capsys.readouterr().out)["correlation_id"] == "abc123"


def test_log_lines_below_the_configured_level_are_dropped(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("WARNING")

    get_logger("skypulse.test").info("too_chatty")

    assert capsys.readouterr().out == ""


def test_loggers_created_before_configuration_still_emit_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    structlog.reset_defaults()
    early = get_logger("skypulse.early")

    configure_logging("INFO")
    early.info("module_level_logger")

    assert last_json_line(capsys.readouterr().out)["component"] == "skypulse.early"
