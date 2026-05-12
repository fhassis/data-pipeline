"""
workers/main.py
===============
Application entry point. Wires all workers together and runs them
concurrently in the same asyncio event loop.

Pipeline order (logical, not sequential — all run concurrently):

    ProducerWorker A/B  →  RawStoreWorker  →  ProcessorWorker
                        →  StoreWorker     →  NotificationWorker

For learning purposes all workers run in a single process. In production
each worker type would be its own container/deployment, allowing independent
scaling and failure isolation.

Environment variables
---------------------
NATS_URL      NATS server address. Default: nats://nats:4222
DATABASE_URL  asyncpg DSN.        Default: postgresql://postgres:postgres@postgres:5432/postgres
LOG_LEVEL     Minimum log level.  Default: INFO

Running
-------
    # Pretty-printed logs (requires kelora)
    uv run --package workers python -u -m workers.main | kelora

    # Raw JSON
    uv run --package workers python -m workers.main
"""

import asyncio
import os
import signal

import structlog
import uvloop
from shared.logging import configure_logging

from workers.core import BaseWorker
from workers.db.database import Database
from workers.notification_worker import NotificationWorker
from workers.processor_worker import ProcessorWorker
from workers.producer_worker import ProducerWorker
from workers.raw_store_worker import RawStoreWorker
from workers.store_worker import StoreWorker

logger = structlog.get_logger(__name__)


async def main() -> None:
    nats_url = os.environ.get("NATS_URL", "nats://nats:4222")
    db_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@postgres:5432/postgres",
    )

    logger.info("pipeline.starting", nats_url=nats_url)

    db = Database(dsn=db_url)
    await db.start()

    workers: list[BaseWorker] = [
        ProducerWorker(nats_url, sensor_id="A"),
        ProducerWorker(nats_url, sensor_id="B"),
        RawStoreWorker(nats_url, db),
        ProcessorWorker(nats_url),
        StoreWorker(nats_url, db),
        NotificationWorker(nats_url),
    ]

    loop = asyncio.get_running_loop()
    tasks: list[asyncio.Task] = []

    def _shutdown(sig: signal.Signals) -> None:
        logger.info("pipeline.shutdown_requested", signal=sig.name)
        for task in tasks:
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    tasks = [asyncio.create_task(w.start(), name=type(w).__name__) for w in workers]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("pipeline.cancelled")
    finally:
        logger.info("pipeline.stopping_workers")
        for w in workers:
            await w.stop()
        await db.stop()
        logger.info("pipeline.stopped")


if __name__ == "__main__":
    configure_logging()
    uvloop.run(main())
