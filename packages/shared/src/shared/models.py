"""
shared/models.py
================
Msgspec structs that define the message contracts for every stage of the
pipeline.

Naming convention
-----------------
RawSensorData    — wire format as published by the producer (short field names)
RawStoredEvent   — published by RawStoreWorker after inserting into raw_sensor_data;
                   carries the generated raw_id and the original payload forward
SensorData       — normalised domain record published by ProcessorWorker
Notification     — outcome record published by StoreWorker

Pipeline flow
-------------
    Producer        → raw.sensor.>          [RawSensorData]
    RawStoreWorker  → raw_stored.sensor.>   [RawStoredEvent]
    ProcessorWorker → parsed.sensor.>       [SensorData]
    StoreWorker     → notifications.sensor.> [Notification]

Structs map to PostgreSQL tables:
    raw_sensor_data  ← written from RawStoredEvent fields (subject, payload)
    sensor_data      ← written from SensorData fields (raw_id ties the FK)

Timestamp handling
------------------
RawSensorData.timestamp  int      — exact wire format, no interpretation
SensorData.timestamp     datetime — UTC-aware, converted once in ProcessorWorker
Epoch ints never appear past the processor boundary.

Field renaming
--------------
RawSensorData uses a rename function so msgspec transparently maps the short
wire keys ("t", "d") to readable Python field names (timestamp, value).
"""

from datetime import datetime

import msgspec


def _raw_rename(name: str) -> str:
    """Maps readable Python field names to short JSON wire keys."""
    return {"timestamp": "t", "value": "d"}.get(name, name)


class RawSensorData(msgspec.Struct, frozen=True, rename=_raw_rename):
    """
    Wire format published by a sensor producer.

    Decoded only in ProcessorWorker, from RawStoredEvent.payload.

    Decodes from:
        {"t": 1718000000, "d": 23.47}

    The rename function maps:
        timestamp ← "t"
        value     ← "d"
    """

    timestamp: int  # Unix epoch seconds — raw, unconverted
    value: float  # Sensor measurement, expected in [0.0, 50.0]


class RawStoredEvent(msgspec.Struct, frozen=True):
    """
    Published by RawStoreWorker after inserting the raw message into
    raw_sensor_data. Carries the generated database ID and the original
    producer bytes forward so the rest of the pipeline can:
      - decode and process the original payload (ProcessorWorker)
      - resolve the FK when writing sensor_data (StoreWorker via SensorData)

    Example:
        {
          "raw_id": 42,
          "subject": "raw.sensor.A",
          "payload": "{\"t\":1718000000,\"d\":23.47}"
        }
    """

    raw_id: int  # PK from raw_sensor_data — used as FK in sensor_data
    subject: str  # Original NATS subject (e.g. "raw.sensor.A")
    payload: str  # Original producer JSON, byte-for-byte as published


class SensorData(msgspec.Struct, frozen=True):
    """
    Normalised domain record published by ProcessorWorker.

    raw_id is resolved before processing begins (by RawStoreWorker), so
    StoreWorker can write sensor_data with the FK already satisfied —
    no cross-worker transaction required.

    Example:
        {
          "raw_id": 42,
          "sensor_id": "A",
          "timestamp": "2024-06-10T12:00:00+00:00",
          "value": 23.47
        }
    """

    raw_id: int  # FK to raw_sensor_data.id
    sensor_id: str  # Extracted from NATS subject ("raw.sensor.A" → "A")
    timestamp: datetime  # UTC-aware, converted from RawSensorData.timestamp
    value: float  # Forwarded from RawSensorData.value


class Notification(msgspec.Struct, frozen=True):
    """
    Outcome record published by StoreWorker after a persistence attempt.

    Example (success):
        {"sensor_id": "A", "success": true, "message": "stored id=7"}

    Example (failure):
        {"sensor_id": "B", "success": false, "message": "...error detail..."}
    """

    sensor_id: str  # Which sensor this notification concerns
    success: bool  # True if the store succeeded
    message: str  # Human-readable detail for logging / alerting
