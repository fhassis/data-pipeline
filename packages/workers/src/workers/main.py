"""
workers/main.py
===============
Application entry point.

The `data-pipeline` command is a script installed into the virtualenv by
`uv sync` during the Docker build stage. The runner image needs no uv —
the script is a plain Python shim that calls run() directly.

    CMD ["data-pipeline"]

    env:
      - name: WORKER_TYPE
        value: raw_store
      - name: NATS_URL
        valueFrom:
          secretKeyRef: ...
      - name: DATABASE_URL
        valueFrom:
          secretKeyRef: ...

Environment variables
---------------------
WORKER_TYPE   Which worker to run. Use "all" to run every worker in one
              process (development). Required.
SENSOR_ID     Sensor identifier for producer workers (e.g. "A", "B").
              Required when WORKER_TYPE=producer.
NATS_URL      NATS server address. Default: nats://nats:4222
DATABASE_URL  asyncpg DSN. Default: postgresql://postgres:postgres@postgres:5432/postgres
LOG_LEVEL     Minimum log level. Default: INFO
"""

import asyncio
import os
import signal

import structlog
import uvloop
from shared.logging import configure_logging

from workers.db.database import Database
from workers.notification_worker import NotificationWorker
from workers.processor_worker import ProcessorWorker
from workers.producer_worker import ProducerWorker
from workers.raw_store_worker import RawStoreWorker
from workers.store_worker import StoreWorker

logger = structlog.get_logger("main")


async def main() -> None:
    """
    Main application entry point.
    """
    nats_url = os.environ.get("NATS_URL", "nats://nats:4222")
    db_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@postgres:5432/postgres",
    )

    # create the database repository and start the connection pool
    db = Database(db_url)
    await db.start()

    # get the worker type from the environment
    worker_type = os.environ.get("WORKER_TYPE")
    if not worker_type:
        raise ValueError("WORKER_TYPE environment variable is required.")

    # create the applicable workers
    workers = []
    match worker_type:
        case "producer":
            sensor_id = os.environ.get("SENSOR_ID")
            if not sensor_id:
                raise ValueError(
                    "SENSOR_ID environment variable is required for WORKER_TYPE=producer"
                )
            workers.append(ProducerWorker(nats_url, sensor_id=sensor_id))

        case "raw_store":
            workers.append(RawStoreWorker(nats_url, db))

        case "processor":
            workers.append(ProcessorWorker(nats_url))

        case "store":
            workers.append(StoreWorker(nats_url, db))

        case "notifier":
            workers.append(NotificationWorker(nats_url))

        case "all":
            workers.extend(
                [
                    ProducerWorker(nats_url, sensor_id="A"),
                    ProducerWorker(nats_url, sensor_id="B"),
                    RawStoreWorker(nats_url, db),
                    ProcessorWorker(nats_url),
                    StoreWorker(nats_url, db),
                    NotificationWorker(nats_url),
                ]
            )

        case _:
            raise ValueError(
                f"Unknown worker {worker_type!r}. "
                f"Valid values: producer, raw_store, processor, store, notifier, all."
            )

    # create the event loop and register tasks
    loop = asyncio.get_running_loop()
    tasks = [asyncio.create_task(w.start(), name=w.name) for w in workers]

    def _shutdown(sig: signal.Signals) -> None:
        """Signal handler for graceful shutdown on SIGINT / SIGTERM."""
        logger.info("pipeline.shutdown_requested", signal=sig.name)
        for task in tasks:
            task.cancel()

    # register signal handlers for graceful shutdown
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    # wait for tasks to complete or be cancelled
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("pipeline.cancelled")
    finally:
        # stop workers and database connection pool
        for worker in workers:
            await worker.stop()
        await db.stop()
        logger.info("pipeline.stopped")


def run() -> None:
    """
    Synchronous entry point for the data-pipeline script.

    Called by the [project.scripts] entry point installed into the virtualenv
    by uv sync. The runner image invokes this via:
        CMD ["data-pipeline"]
    """
    configure_logging()
    uvloop.run(main())


# executes when running the module directly (e.g. for development)
if __name__ == "__main__":
    run()
