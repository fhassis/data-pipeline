"""
workers/db/repository.py
========================
Repository for raw_sensor_data and sensor_data tables.

Each method is used by a different worker:
    insert_raw  ← called by RawStoreWorker
    insert      ← called by StoreWorker

They are in the same repository class because they operate on the same
domain (sensor data) and share the same database. Workers that only need
one method simply call only that one — there is no forced coupling.

No transaction spans both methods. RawStoreWorker writes raw_sensor_data
first and carries the generated raw_id forward in the NATS message.
By the time StoreWorker calls insert(), the FK is already satisfied.
"""

import asyncpg
import structlog

from shared.models import SensorData

logger = structlog.get_logger(__name__)


class SensorRepository:
    """
    Wraps INSERT operations on raw_sensor_data and sensor_data.

    Methods receive a connection rather than acquiring one, so the caller
    controls the connection lifecycle.
    """

    async def insert_raw(
        self,
        conn: asyncpg.Connection,
        subject: str,
        payload: str,
    ) -> int:
        """
        Persist the original producer message to raw_sensor_data.

        subject: the original NATS subject (e.g. "raw.sensor.A")
        payload: the original JSON string as published by the producer

        asyncpg accepts a str for JSONB columns directly.
        Returns the generated id of the new row.
        """
        raw_id: int = await conn.fetchval(
            """
            INSERT INTO raw_sensor_data (subject, payload)
            VALUES ($1, $2)
            RETURNING id
            """,
            subject,
            payload,
        )
        logger.debug("db.insert_raw", id=raw_id, subject=subject)
        return raw_id

    async def insert(
        self,
        conn: asyncpg.Connection,
        reading: SensorData,
    ) -> int:
        """
        Persist the parsed sensor reading to sensor_data.

        reading.raw_id must reference an existing raw_sensor_data row —
        guaranteed by the pipeline order (RawStoreWorker runs first).

        asyncpg maps datetime(UTC) to TIMESTAMPTZ natively.
        Returns the generated id of the new row.
        """
        row_id: int = await conn.fetchval(
            """
            INSERT INTO sensor_data (raw_id, sensor_id, timestamp, value)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            """,
            reading.raw_id,
            reading.sensor_id,
            reading.timestamp,  # datetime(UTC) → TIMESTAMPTZ
            reading.value,
        )
        logger.debug(
            "db.insert",
            id=row_id,
            raw_id=reading.raw_id,
            sensor_id=reading.sensor_id,
            timestamp=reading.timestamp.isoformat(),
        )
        return row_id
