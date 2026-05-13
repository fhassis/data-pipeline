"""
workers/core/base.py
====================
Single base class for all pipeline workers.

Design
------
One class handles both consumer and producer patterns through two optional
mechanisms:

  Pull loop (consumer)
    Set STREAM and CONSUMER as class variables. The default run() activates
    the pull loop automatically and delegates each message to on_message().

  Publish helper (all workers)
    publish(subject, payload, stream) is available to every worker regardless
    of whether it also consumes. A worker that watches one channel and
    publishes to another — like RawStoreWorker — uses both mechanisms from
    the same base.

  Producer (no consumption)
    Leave STREAM and CONSUMER as None and override run() with a custom loop.
    publish() is still available.

Health files
------------
Each worker instance gets its own health file derived from its class name:
    /tmp/worker_health_rawstoreworker
    /tmp/worker_health_processorworker

The heartbeat loop only touches the file when WorkerHealth.is_healthy is True.
A disconnected or stalled worker stops touching its file, which causes the
Kubernetes liveness probe to fail and trigger a pod restart.

A /health HTTP endpoint backed by WorkerHealth.as_dict() can be added later
without changing the health state logic.
"""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar

import nats
import nats.errors
import structlog
from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import PubAck

from workers.core.health import WorkerHealth
from workers.core.utils import to_snake_case


class BaseWorker:
    """
    Foundation for all NATS workers.

    Consumer usage (set STREAM + CONSUMER, implement on_message):
    ---------------------------------------------------------------
        class MyConsumer(BaseWorker):
            STREAM   = "MY_STREAM"
            CONSUMER = "my-consumer"

            async def on_message(self, msg: Msg) -> None:
                data = json_decode(msg.data, type=MyStruct)
                await msg.ack()

    Producer usage (override run, call publish):
    --------------------------------------------
        class MyProducer(BaseWorker):
            async def run(self) -> None:
                while True:
                    await self.publish("my.subject", payload, stream="MY_STREAM")
                    await asyncio.sleep(2)

    Hybrid usage (consume and publish — like RawStoreWorker):
    ----------------------------------------------------------
        class MyHybrid(BaseWorker):
            STREAM   = "INBOUND"
            CONSUMER = "my-consumer"

            async def on_message(self, msg: Msg) -> None:
                await self.publish("outbound.subject", result, stream="OUTBOUND")
                await msg.ack()
    """

    # Consumer configuration — leave as None for pure producers.
    # Both must be set together; setting only one raises ValueError at __init__.
    STREAM: ClassVar[str | None] = None
    CONSUMER: ClassVar[str | None] = None

    # Pull loop tuning — override per worker class if needed.
    FETCH_BATCH: ClassVar[int] = 10
    FETCH_TIMEOUT: ClassVar[float] = 5.0
    NAK_DELAY: ClassVar[float] = 5.0
    RECONNECT_DELAY: ClassVar[float] = 1.0

    # How often the heartbeat loop touches the health file (seconds).
    # The Kubernetes liveness probe window should be 2-3x this value.
    HEARTBEAT_INTERVAL: ClassVar[int] = 30

    def __init__(self, nats_url: str) -> None:
        """
        Initialise the worker and validate consumer configuration.

        Parameters
        ----------
        nats_url:
            NATS server address (e.g. "nats://nats:4222").

        Raises
        ------
        ValueError
            If exactly one of STREAM or CONSUMER is set. Both must be set
            together to activate the pull loop, or both left as None for
            a producer that overrides run().
        """
        # forms the worker name based on the class name in snake_case
        self.name = to_snake_case(type(self).__name__)

        # Validate that STREAM and CONSUMER are either both set or both None.
        if bool(self.STREAM) ^ bool(self.CONSUMER):
            raise ValueError(
                f"{self.name} has only one of STREAM or CONSUMER set "
                f"(STREAM={self.STREAM!r}, CONSUMER={self.CONSUMER!r}). "
                f"Either set both to activate the pull loop, or set neither "
                f"and override run()."
            )

        # initializes instance variables
        self._nats_url = nats_url
        self._health_file = Path(f"/tmp/{self.name.lower()}_health")
        self._nc: NATSClient | None = None
        self._js: JetStreamContext | None = None
        self.health = WorkerHealth(worker_name=self.name)
        self.logger = structlog.get_logger(self.name)

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """
        Entry point for the worker. Controls the full startup lifecycle.

        Sequence:
            1. Establish NATS connection and JetStream context.
            2. Start the heartbeat background task.
            3. Call on_start() for optional subclass-specific setup.
            4. Call run() — blocks until the worker is stopped or cancelled.

        Never override this method. Use on_start() for extra setup logic.
        """
        # connects to NATS and initializes JetStream context
        await self._connect()

        # starts the heartbeat loop in the background to update health state
        asyncio.create_task(self._heartbeat_loop(), name=f"{self.name}_heartbeat_loop")

        # calls the on_start() hook for optional extra setup
        await self.on_start()

        # enters the main loop
        self.logger.info("worker.started")
        await self.run()

    async def stop(self) -> None:
        """
        Graceful shutdown. Drains in-flight messages before closing.

        nats-py drain() waits for all delivered-but-unacked messages to be
        processed before closing the connection, preserving at-least-once
        delivery guarantees.
        """
        if self._nc:
            await self._nc.drain()
        self.logger.info("worker.stopped")

    async def publish(
        self,
        subject: str,
        payload: bytes,
        stream: str | None = None,
    ) -> PubAck | None:
        """
        Publish a payload to a JetStream subject.

        Available to all workers regardless of whether they also consume.
        Errors are logged but not re-raised — the caller checks the return
        value and decides whether to retry or nack the source message.

        Parameters
        ----------
        subject:
            The NATS subject to publish to (e.g. "parsed.sensor.A").
            JetStream routes the message to the correct stream based on
            subject pattern matching configured on the server.
        payload:
            Serialised message bytes, typically from msgspec.json.encode().
        stream:
            Optional stream name for publish confirmation. When provided,
            nats-py verifies the message was accepted by this specific stream
            and raises if not — useful for catching subject/stream routing
            misconfigurations early in development.

        Returns
        -------
        PubAck | None
            The server acknowledgement on success, containing ack.seq (the
            stream sequence number, useful for debugging). None if the publish
            failed — the error is already logged.
        """
        try:
            ack: PubAck = await self._js.publish(subject, payload, stream=stream)
            self.logger.debug("publish.ok", subject=subject, seq=ack.seq)
            return ack
        except nats.errors.TimeoutError:
            self.logger.warning("publish.timeout", subject=subject)
            return None
        except Exception as e:
            self.logger.error("publish.error", subject=subject, error=str(e))
            return None

    # -------------------------------------------------------------------------
    # Hooks — override in subclasses
    # -------------------------------------------------------------------------

    async def on_start(self) -> None:
        """
        Optional extra setup after the NATS connection is ready.

        Called by start() before run(). Override to load runtime config,
        warm caches, initialise database repositories, etc.
        No need to call super() — base implementation is a deliberate no-op.
        """

    async def run(self) -> None:
        """
        Main worker loop.

        Default behaviour:
          - If both STREAM and CONSUMER are set: activates the pull loop,
            which delivers messages to on_message().
          - Otherwise: raises NotImplementedError, signalling that the
            subclass must override run() with a producer or custom loop.

        Override this method for producer workers or any worker with custom
        loop logic that does not fit the standard pull pattern.
        """
        if self.STREAM and self.CONSUMER:
            await self._pull_loop()
        else:
            raise NotImplementedError(
                f"{type(self).__name__} must either set STREAM and CONSUMER "
                f"(to activate the pull loop) or override run() "
                f"(for producer or custom loop behavior)."
            )

    async def on_message(self, msg: Msg) -> None:
        """
        Process a single message delivered by the pull loop.

        Override this method when STREAM and CONSUMER are set. The
        implementation is responsible for:
          - Decoding msg.data (typically with msgspec.json.decode).
          - Calling await msg.ack() on success.
          - Calling await msg.nak(delay=N) on recoverable errors so the
            message is redelivered after N seconds.
          - Calling await msg.ack() on unrecoverable errors (bad payload,
            schema mismatch) to prevent infinite redelivery.

        Parameters
        ----------
        msg:
            The NATS message. msg.subject contains the full concrete subject
            even when the consumer uses a wildcard filter (e.g. "raw.sensor.A"
            not "raw.sensor.>"). Use msg.subject.split(".")[-1] to extract
            routing metadata such as sensor_id.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement on_message() when STREAM and CONSUMER are set."
        )

    # -------------------------------------------------------------------------
    # NATS callbacks — override to extend, no need to call super()
    # -------------------------------------------------------------------------

    async def _on_error(self, e: Exception) -> None:
        """
        Called by nats-py on client errors (protocol errors, slow consumers).

        Updates health state and logs the error. Override to add custom
        error handling such as metrics or alerting.
        """
        self.health.last_error = str(e)
        self.logger.error("nats.error", error=str(e))

    async def _on_disconnected(self) -> None:
        """
        Called by nats-py when the TCP connection is lost.

        Marks the worker as disconnected in health state. nats-py will
        attempt reconnection automatically in the background.
        """
        self.health.connected = False
        self.logger.warning("nats.disconnected")

    async def _on_reconnected(self) -> None:
        """
        Called by nats-py after a successful TCP reconnection.

        Restores the connected flag and clears the last error so health
        state reflects the current reality.
        """
        self.health.connected = True
        self.health.last_error = None
        self.logger.info("nats.reconnected")

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    async def _connect(self) -> None:
        """
        Establish the NATS connection and JetStream context.

        Uses infinite reconnect (max_reconnect_attempts=-1) because the
        default of 60 attempts is unsuitable for long-running workers — a
        worker that loses connectivity for ~2 minutes would give up permanently.
        """
        self._nc = await nats.connect(
            self._nats_url,
            max_reconnect_attempts=-1,
            reconnect_time_wait=2,
            error_cb=self._on_error,
            disconnected_cb=self._on_disconnected,
            reconnected_cb=self._on_reconnected,
        )
        self._js = self._nc.jetstream()
        self.health.connected = True
        self.logger.info("nats.connected", url=self._nats_url)

    async def _pull_loop(self) -> None:
        """
        Pull consumer loop. Activated by run() when STREAM and CONSUMER are set.

        Binds to the pre-existing durable consumer via pull_subscribe_bind(),
        which raises immediately if the consumer does not exist on the server —
        enforcing the infrastructure-first principle (nats_setup.sh must be
        run before starting workers).

        Each received message updates health.last_message_at before being
        passed to on_message(), so the heartbeat loop has an accurate view
        of message activity.
        """
        sub = await self._js.pull_subscribe_bind(self.CONSUMER, stream=self.STREAM)
        self.logger.info("consumer.subscribed", stream=self.STREAM, consumer=self.CONSUMER)

        while True:
            try:
                # fetches a batch of messages
                msgs = await sub.fetch(
                    batch=self.FETCH_BATCH,
                    timeout=self.FETCH_TIMEOUT,
                )
            except nats.errors.TimeoutError:
                # Normal idle condition — no messages in the fetch window
                continue
            except nats.errors.ConnectionClosedError:
                # TCP dropped — nats-py is reconnecting in the background.
                # Sleep briefly to avoid a tight spin loop before retrying.
                self.logger.warning("consumer.connection_closed")
                await asyncio.sleep(self.RECONNECT_DELAY)
                continue

            # iterates over received messages
            for msg in msgs:
                # uddate health with the timestamp of the last received message
                self.health.last_message_at = datetime.now(timezone.utc)

                try:
                    # delivers the message to the worker's on_message() implementation
                    await self.on_message(msg)
                except Exception as e:
                    # logs the error
                    self.logger.error("on_message.exception", subject=msg.subject, error=str(e))

                    # nacks the message for redelivery after a delay
                    await msg.nak(delay=self.NAK_DELAY)

    async def send_to_dlq(self, msg: Msg) -> None:
        """
        Route an unrecoverable message to the DLQ stream and ack the original.

        Call this from on_message() for errors that will never resolve on retry
        (bad payload, schema mismatch). Do NOT call for transient failures
        (DB down, network error) — nack those so NATS retries indefinitely.

        The DLQ subject mirrors the original: dlq.raw.sensor.A, etc.
        The original payload is forwarded as-is for inspection and replay.
        """
        # forms a subject like "dlq.raw.sensor.A" from "raw.sensor.A"
        dlq_subject = f"dlq.{msg.subject}"

        # publishes to the DLQ stream
        ack = await self.publish(dlq_subject, msg.data, stream="DLQ")

        # logs the ack result
        self.logger.error(
            "message.dlqed" if ack else "message.dlq_publish_failed",
            original_subject=msg.subject,
            num_delivered=msg.metadata.num_delivered,
        )

        # acks the original message to prevent redelivery
        await msg.ack()

    async def _heartbeat_loop(self) -> None:
        """
        Touch the per-worker health file every HEARTBEAT_INTERVAL seconds,
        but only when the worker is healthy.

        The file's modification timestamp is the liveness signal — no content
        is written. The Kubernetes liveness probe checks it with:
            find /tmp/worker_health_<classname_lower> -mmin -1

        When the worker is unhealthy (disconnected or stale), the file is not
        touched. The probe detects the stale mtime and triggers a pod restart.
        This wires WorkerHealth.is_healthy directly to the liveness signal.

        A future /health HTTP endpoint can expose WorkerHealth.as_dict() for
        richer diagnostics without changing this logic.
        """
        while True:
            # update liveliness file if healthy
            if self.health.is_healthy:
                self._health_file.touch()

            # sleep until the next heartbeat
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
