import pytest

from skypulse.ingestion.opensky.access import CreditsExhaustedError
from skypulse.ingestion.publisher import PublishReport
from skypulse.ingestion.runner import CREDITS_BACKOFF_SECONDS, Schedule, run_periodically


class ScriptedPoller:
    def __init__(self, *outcomes: PublishReport | Exception) -> None:
        self._outcomes = list(outcomes)

    async def run_once(self) -> PublishReport:
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class RecordingSleep:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


async def test_runs_the_requested_number_of_cycles_with_the_interval() -> None:
    sleep = RecordingSleep()
    poller = ScriptedPoller(PublishReport(1, 0), PublishReport(2, 0), PublishReport(0, 0))

    completed = await run_periodically("test", poller, Schedule(30, cycles=3), sleep)

    assert completed == 3
    assert sleep.delays == [30, 30]


async def test_a_failing_cycle_does_not_stop_the_loop() -> None:
    sleep = RecordingSleep()
    poller = ScriptedPoller(RuntimeError("upstream blew up"), PublishReport(1, 0))

    completed = await run_periodically("test", poller, Schedule(5, cycles=2), sleep)

    assert completed == 2


async def test_running_out_of_credits_backs_off_longer() -> None:
    sleep = RecordingSleep()
    poller = ScriptedPoller(CreditsExhaustedError("0 left"), PublishReport(1, 0))

    await run_periodically("test", poller, Schedule(5, cycles=2), sleep)

    assert sleep.delays == [CREDITS_BACKOFF_SECONDS]


def test_schedule_without_cycle_limit_runs_forever() -> None:
    assert Schedule(1).allows(10_000)


@pytest.mark.parametrize("completed", [0, 1])
def test_schedule_with_limit(completed: int) -> None:
    assert Schedule(1, cycles=2).allows(completed)
