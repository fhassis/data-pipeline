"""
workers/producer_worker.py
==========================
Simulates a temperature sensor publishing raw readings every 2 seconds.

Pipeline position:

    [ProducerWorker A] ──┐
                          ├──> raw.sensor.> ──> [RawStoreWorker]
    [ProducerWorker B] ──┘

Pure producer — STREAM and CONSUMER are not set. run() is overridden with
a timer loop. publish() is inherited from BaseWorker.
"""

import asyncio
import random
import time

from msgspec.json import encode as json_encode
from shared.models import RawSensorData

from workers.core import BaseWorker


class ProducerWorker(BaseWorker):
    def __init__(
        self,
        nats_url: str,
        sensor_id: str,
        publish_interval: float = 2.0,
    ) -> None:
        super().__init__(nats_url)
        self._sensor_id = sensor_id
        self._publish_interval = publish_interval
        self._subject = f"raw.sensor.{sensor_id}"

    async def run(self) -> None:
        self.logger.info(
            "producer.started", sensor_id=self._sensor_id, subject=self._subject
        )

        while True:
            reading = RawSensorData(
                timestamp=int(time.time()),
                value=round(random.uniform(0.0, 50.0), 2),
            )

            ack = await self.publish(self._subject, json_encode(reading), stream="RAW")
            if ack:
                self.logger.info(
                    "producer.published",
                    sensor_id=self._sensor_id,
                    value=reading.value,
                    seq=ack.seq,
                )

            await asyncio.sleep(self._publish_interval)
