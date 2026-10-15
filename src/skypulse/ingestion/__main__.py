"""Producer entrypoint: ``python -m skypulse.ingestion <feed> [--cycles N]``."""

import argparse
import asyncio

from skypulse.common.config import Settings
from skypulse.common.logging import configure_logging
from skypulse.ingestion.runner import Schedule, run_periodically
from skypulse.ingestion.wiring import Feed, Wiring
from skypulse.observability.metrics import start_metrics_server


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="skypulse.ingestion", description=__doc__)
    parser.add_argument("feed", type=Feed, choices=list(Feed))
    parser.add_argument("--cycles", type=int, default=None, help="stop after N cycles")
    parser.add_argument("--interval", type=float, default=None, help="seconds between cycles")
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> int:
    arguments = parse_arguments(argv)
    settings = Settings()
    configure_logging(settings.log_level)
    start_metrics_server(settings)
    wiring = Wiring.from_settings(settings)
    feed: Feed = arguments.feed
    schedule = Schedule(arguments.interval or feed.interval(settings), arguments.cycles)
    await run_periodically(str(feed), wiring.poller(feed), schedule)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
