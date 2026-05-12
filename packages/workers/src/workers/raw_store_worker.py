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

from msgspec.json import encode as json_encode
from nats.aio.msg import Msg
from shared.models import RawStoredEvent

from workers.core import BaseWorker
from workers.db.database import Database
from workers.db.repository import SensorRepository


class RawStoreWorker(BaseWorker):
    STREAM = "RAW"
    CONSUMER = "raw_store"

    def __init__(self, nats_url: str, db: Database) -> None:
        super().__init__(nats_url)
        self._db = db
        self._repo = SensorRepository()

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
        self.logger.info("raw_store.received", subject=msg.subject, sensor_id=sensor_id)

        # --- Persist raw bytes — no parsing -----------------------------------
        try:
            async with self._db.acquire() as conn:
                raw_id = await self._repo.insert_raw(
                    conn,
                    subject=msg.subject,
                    payload=msg.data.decode(),
                )
            self.logger.info("raw_store.persisted", raw_id=raw_id, subject=msg.subject)

        except Exception as e:
            self.logger.error("raw_store.db_error", subject=msg.subject, error=str(e))
            await msg.nak(delay=5)
            return

        # --- Republish with raw_id -------------------------------------------
        event = RawStoredEvent(
            raw_id=raw_id,
            subject=msg.subject,
            payload=msg.data.decode(),
        )
        out_subject = f"raw_stored.sensor.{sensor_id}"
        ack = await self.publish(out_subject, json_encode(event), stream="RAW_STORED")
        if ack:
            self.logger.info(
                "raw_store.published",
                subject=out_subject,
                raw_id=raw_id,
                seq=ack.seq,
            )
            await msg.ack()
        else:
            await msg.nak(delay=5)
