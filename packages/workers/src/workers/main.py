"""
workers/main.py
===============
Application entry point.

In development, all workers run in a single process for convenience:

    uv run --package workers python -m workers.main | kelora

In production, each container runs a single worker selected by the WORKER
environment variable. The same image is used for all workers — only the
env vars differ per Kubernetes deployment:

    CMD ["data-pipeline"]

    env:
      - name: WORKER
        value: raw_store          # one of the values in _build_worker()
      - name: NATS_URL
        valueFrom:
          secretKeyRef: ...
      - name: DATABASE_URL
        valueFrom:
          secretKeyRef: ...

The `data-pipeline` command is a script installed into the virtualenv by
`uv sync` during the Docker build stage. The runner image needs no uv —
the script is a plain Python shim that calls run() directly.

Environment variables
---------------------
WORKER        Which worker to run. Required in production; omit to run all
              workers locally (development mode).
SENSOR_ID     Sensor identifier for producer workers (e.g. "A", "B").
              Required when WORKER=producer.
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

from workers.core import BaseWorker
from workers.db.database import Database
from workers.notification_worker import NotificationWorker
from workers.processor_worker import ProcessorWorker
from workers.producer_worker import ProducerWorker
from workers.raw_store_worker import RawStoreWorker
from workers.store_worker import StoreWorker

logger = structlog.get_logger(__name__)


def _build_worker(name: str, nats_url: str, db_url: str) -> BaseWorker:
    """
    Instantiate the worker identified by name.

    Parameters
    ----------
    name:
        Worker identifier. Must match one of the cases below.
    nats_url:
        NATS server address passed to every worker.
    db_url:
        asyncpg DSN passed to workers that require database access.
        Each process instantiates its own Database — no pool sharing
        across containers.

    Returns
    -------
    BaseWorker
        The fully constructed worker, ready to call start() on.

    Raises
    ------
    ValueError
        If name does not match any known worker, or if a required
        environment variable (e.g. SENSOR_ID) is missing.
    """
    match name:
        case "producer":
            sensor_id = os.environ.get("SENSOR_ID")
            if not sensor_id:
                raise ValueError(
                    "SENSOR_ID environment variable is required for WORKER=producer"
                )
            return ProducerWorker(nats_url, sensor_id=sensor_id)

        case "raw_store":
            return RawStoreWorker(nats_url, Database(db_url))

        case "processor":
            return ProcessorWorker(nats_url)

        case "store":
            return StoreWorker(nats_url, Database(db_url))

        case "notifier":
            return NotificationWorker(nats_url)

        case _:
            raise ValueError(
                f"Unknown worker {name!r}. "
                f"Valid values: producer, raw_store, processor, store, notifier."
            )


async def _run_one(worker: BaseWorker) -> None:
    """
    Run a single worker with graceful shutdown on SIGINT / SIGTERM.

    Parameters
    ----------
    worker:
        The worker instance to start. Blocks until cancelled.
    """
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(worker.start(), name=type(worker).__name__)

    def _shutdown(sig: signal.Signals) -> None:
        logger.info("shutdown.requested", signal=sig.name)
        task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    try:
        await task
    except asyncio.CancelledError:
        logger.info("worker.cancelled")
    finally:
        await worker.stop()


async def _run_all(nats_url: str, db_url: str) -> None:
    """
    Run all workers concurrently in a single process.

    Used in development only. Not suitable for production — in production
    each worker runs in its own container via _run_one().

    Parameters
    ----------
    nats_url:
        NATS server address.
    db_url:
        asyncpg DSN. A single Database instance is shared between workers
        that need DB access within this process.
    """
    db = Database(db_url)
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
    tasks = [asyncio.create_task(w.start(), name=type(w).__name__) for w in workers]

    def _shutdown(sig: signal.Signals) -> None:
        logger.info("pipeline.shutdown_requested", signal=sig.name)
        for t in tasks:
            t.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("pipeline.cancelled")
    finally:
        for w in workers:
            await w.stop()
        await db.stop()
        logger.info("pipeline.stopped")


async def main() -> None:
    """
    Entry coroutine. Dispatches to _run_one() or _run_all() based on the
    WORKER environment variable.

    WORKER set   → production mode: single worker, one container
    WORKER unset → development mode: all workers in one process
    """
    nats_url = os.environ.get("NATS_URL", "nats://nats:4222")
    db_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@postgres:5432/postgres",
    )
    worker_name = os.environ.get("WORKER")

    if worker_name:
        logger.info("pipeline.starting", mode="single", worker=worker_name)
        worker = _build_worker(worker_name, nats_url, db_url)
        await _run_one(worker)
    else:
        logger.info("pipeline.starting", mode="all")
        await _run_all(nats_url, db_url)


def run() -> None:
    """
    Synchronous entry point for the data-pipeline script.

    Called by the [project.scripts] entry point installed into the virtualenv
    by uv sync. The runner image invokes this via:
        CMD ["data-pipeline"]
    """
    configure_logging()
    uvloop.run(main())


if __name__ == "__main__":
    run()
