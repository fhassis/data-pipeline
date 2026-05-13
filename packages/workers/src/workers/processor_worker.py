"""
workers/processor_worker.py
============================
Normalises raw sensor data and publishes typed SensorData to the parsed channel.

Pipeline position:

    raw_stored.sensor.> ──> [ProcessorWorker] ──> parsed.sensor.>

Hybrid worker — consumes from RAW_STORED (STREAM + CONSUMER set) and publishes
to PARSED via publish(). Both mechanisms come from BaseWorker.
"""

from datetime import datetime, timezone

import msgspec
from msgspec.json import decode as json_decode
from msgspec.json import encode as json_encode
from nats.aio.msg import Msg
from shared.models import RawStoredEvent, SensorData

from workers.core import BaseWorker


class ProcessorWorker(BaseWorker):
    STREAM = "RAW_STORED"
    CONSUMER = "processor"

    async def on_message(self, msg: Msg) -> None:
        """
        Decode the RawStoredEvent envelope, normalise to SensorData, and republish.

        event.payload is already a RawSensorData struct (decoded by RawStoreWorker),
        so no second decode is needed here.

        sensor_id is extracted from event.subject (the original RAW subject
        "raw.sensor.A"), not msg.subject (the RAW_STORED subject
        "raw_stored.sensor.A").
        """
        self.logger.info("processor.received", subject=msg.subject)

        # --- Stage 1: decode the envelope -------------------------------------
        try:
            event: RawStoredEvent = json_decode(msg.data, type=RawStoredEvent)
        except msgspec.DecodeError as e:
            self.logger.error(
                "processor.decode_error.envelope",
                subject=msg.subject,
                raw_payload=msg.data.decode(errors="replace"),
                error=str(e),
            )
            await self.send_to_dlq(msg)
            return

        # --- Normalise --------------------------------------------------------
        sensor_id = event.subject.split(".")[-1]

        parsed = SensorData(
            raw_id=event.raw_id,
            sensor_id=sensor_id,
            timestamp=datetime.fromtimestamp(event.payload.timestamp, tz=timezone.utc),
            value=event.payload.value,
        )

        # --- Publish ----------------------------------------------------------
        out_subject = f"parsed.sensor.{sensor_id}"
        ack = await self.publish(out_subject, json_encode(parsed), stream="PARSED")
        if ack:
            self.logger.info(
                "processor.published",
                subject=out_subject,
                raw_id=parsed.raw_id,
                sensor_id=sensor_id,
                value=parsed.value,
                timestamp=parsed.timestamp.isoformat(),
                seq=ack.seq,
            )
            await msg.ack()
        else:
            await msg.nak(delay=self.NAK_DELAY)
