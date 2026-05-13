"""
workers/producer_worker.py
==========================
Simulates a temperature sensor publishing raw readings every 2 seconds.

Pipeline position:

    [ProducerWorker] ──> raw.sensor.<sensor_id> ──> [RawStoreWorker]

Pure producer — STREAM and CONSUMER are left as None, run() is overridden
with a timer loop. publish() is inherited from BaseWorker.

In production, sensor_id comes from the SENSOR_ID environment variable set
in the Kubernetes deployment manifest. The same WORKER=producer image is
deployed multiple times with different SENSOR_ID values — one deployment
per sensor source.

In development, main.py instantiates two instances directly:
    ProducerWorker(nats_url, sensor_id="A")
    ProducerWorker(nats_url, sensor_id="B")
"""

import asyncio
import random
import time

from msgspec.json import encode as json_encode
from shared.models import RawSensorData

from workers.core import BaseWorker


class ProducerWorker(BaseWorker):
    """
    Publishes a simulated temperature reading every 2 seconds.

    Parameters
    ----------
    nats_url:
        NATS server address.
    sensor_id:
        Identifier for this sensor (e.g. "A", "B"). Becomes part of the
        published subject: raw.sensor.<sensor_id>. In production, passed
        from the SENSOR_ID environment variable by main._build_worker().
    publish_interval:
        Seconds between publications. Default: 5.
    """

    def __init__(
        self,
        nats_url: str,
        sensor_id: str,
        publish_interval: float = 5.0,
    ) -> None:
        super().__init__(nats_url)
        self._sensor_id = sensor_id
        self._publish_interval = publish_interval
        self._subject = f"raw.sensor.{sensor_id}"

    async def run(self) -> None:
        """
        Publish loop. Runs forever, publishing one reading per interval.

        The reading value is randomised within [0.0, 50.0] to simulate a real sensor.
        """
        self.logger.info("producer.running", sensor_id=self._sensor_id, subject=self._subject)

        while True:
            # produces the sensor data
            reading = RawSensorData(
                timestamp=int(time.time()),
                value=round(random.uniform(0.0, 50.0), 2),
            )

            # publishes the sensor data to NATS
            ack = await self.publish(self._subject, json_encode(reading), stream="RAW")
            if ack:
                self.logger.info(
                    "producer.published",
                    sensor_id=self._sensor_id,
                    value=reading.value,
                    seq=ack.seq,
                )

            # wait for the next publish interval
            await asyncio.sleep(self._publish_interval)
