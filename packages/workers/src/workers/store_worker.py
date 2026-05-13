"""
workers/store_worker.py
=======================
Persists parsed sensor data to PostgreSQL and publishes a notification.

Pipeline position:

    parsed.sensor.> ──> [StoreWorker] ──> sensor_data (postgres)
                                     ──> notifications.sensor.>

Hybrid worker — consumes from PARSED (STREAM + CONSUMER set) and publishes
to NOTIFICATIONS via publish(). Both mechanisms come from BaseWorker.
"""

import msgspec
from msgspec.json import decode as json_decode
from msgspec.json import encode as json_encode
from nats.aio.msg import Msg
from shared.models import Notification, SensorData

from workers.core import BaseWorker
from workers.db.database import Database
from workers.db.repository import SensorRepository


class StoreWorker(BaseWorker):
    STREAM = "PARSED"
    CONSUMER = "store"

    def __init__(self, nats_url: str, db: Database) -> None:
        super().__init__(nats_url)
        self._db = db
        self._repo = SensorRepository()

    async def on_message(self, msg: Msg) -> None:
        """
        Store the SensorData in PostgreSQL and publish a Notification of the outcome.
        """
        sensor_id = msg.subject.split(".")[-1]
        self.logger.info("store.received", subject=msg.subject, sensor_id=sensor_id)

        # --- Deserialise -------------------------------------------------------
        try:
            reading: SensorData = json_decode(msg.data, type=SensorData)
        except msgspec.DecodeError as e:
            self.logger.error(
                "store.decode_error",
                subject=msg.subject,
                error=str(e),
                raw_payload=msg.data.decode(errors="replace"),
            )
            await msg.ack()  # unrecoverable — ack to avoid infinite redelivery
            return

        # --- Persist -----------------------------------------------------------
        notification: Notification
        try:
            async with self._db.acquire() as conn:
                row_id = await self._repo.insert(conn, reading)

            notification = Notification(
                sensor_id=sensor_id,
                success=True,
                message=f"stored id={row_id}",
            )
            self.logger.info(
                "store.persisted",
                sensor_id=sensor_id,
                row_id=row_id,
                raw_id=reading.raw_id,
                value=reading.value,
            )
            await msg.ack()

        except Exception as e:
            notification = Notification(
                sensor_id=sensor_id,
                success=False,
                message=str(e),
            )
            self.logger.error("store.db_error", sensor_id=sensor_id, error=str(e))
            await msg.ack()  # ack even on DB error — Notification carries the failure

        # --- Publish notification ----------------------------------------------
        notif_subject = f"notifications.sensor.{sensor_id}"
        await self.publish(notif_subject, json_encode(notification), stream="NOTIFICATIONS")
        self.logger.info(
            "store.notification_published",
            subject=notif_subject,
            success=notification.success,
        )
