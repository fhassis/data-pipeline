"""
workers/raw_store_worker.py
============================
Stores the original producer message to the database and republishes with
the generated raw_id so the rest of the pipeline can resolve the FK.

Pipeline position:

    raw.sensor.> ──> [RawStoreWorker] ──> raw_sensor_data (postgres)
                                     ──> raw_stored.sensor.> [RawStoredEvent]

Hybrid worker — consumes from RAW via the pull loop (STREAM + CONSUMER set)
and publishes to RAW_STORED via publish(). Both mechanisms come from BaseWorker.
"""

import msgspec
from msgspec.json import decode as json_decode
from msgspec.json import encode as json_encode
from nats.aio.msg import Msg
from shared.models import RawSensorData, RawStoredEvent

from workers.core import BaseWorker
from workers.db.database import Database
from workers.db.repository import SensorRepository


class RawStoreWorker(BaseWorker):
    STREAM = "RAW"
    CONSUMER = "raw_store"

    def __init__(self, nats_url: str, db_url: str) -> None:
        super().__init__(nats_url)
        self._db = Database(db_url)
        self._repo = SensorRepository(self._db)

    async def on_start(self) -> None:
        # starts the database connection pool
        await self._db.start()

    async def on_stop(self) -> None:
        # closes the database connection pool
        await self._db.stop()

    async def on_message(self, msg: Msg) -> None:
        """
        Store raw bytes without parsing, then republish with raw_id.

        Acks only after both the DB insert and the republish succeed.
        On DB failure: nack — message redelivered.
        On publish failure after a successful DB insert: nack — retry produces
        a duplicate raw_sensor_data row (orphan), which is acceptable at this
        scale. Add deduplication if needed.
        """
        sensor_id = msg.subject.split(".")[-1]
        self.logger.info("message.received", subject=msg.subject, sensor_id=sensor_id)

        # --- Persist raw bytes — no parsing -----------------------------------
        try:
            raw_id = await self._repo.insert_raw_sensor(
                subject=msg.subject,
                payload=msg.data.decode(),
            )
            self.logger.info("message.stored", raw_id=raw_id, subject=msg.subject)

        except Exception as e:
            self.logger.error("message.store_failed", subject=msg.subject, error=str(e))
            await msg.nak(delay=self.NAK_DELAY)
            return

        # --- Republish with raw_id -------------------------------------------
        try:
            event = RawStoredEvent(
                raw_id=raw_id,
                subject=msg.subject,
                payload=json_decode(msg.data, type=RawSensorData),
            )
        except msgspec.DecodeError as e:
            self.logger.error("message.decode_error", subject=msg.subject, error=str(e))
            await self.send_to_dlq(msg)
            return

        # publish the event with the generated raw_id so downstream workers can resolve the FK
        out_subject = f"raw_stored.sensor.{sensor_id}"
        ack = await self.publish(out_subject, json_encode(event), stream="RAW_STORED")
        if ack:
            self.logger.info(
                "message.published",
                subject=out_subject,
                raw_id=raw_id,
                seq=ack.seq,
            )
            await msg.ack()
        else:
            await msg.nak(delay=self.NAK_DELAY)
