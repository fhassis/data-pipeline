"""
workers/db/database.py
======================
Thin wrapper around an asyncpg connection pool.

UTC enforcement
---------------
PostgreSQL's TIMESTAMPTZ type always stores values in UTC internally, but
displays them in the session timezone, which defaults to the server's setting.
We force UTC at the connection level via server_settings so all connections
in this pool always return UTC-aware datetime objects — regardless of the
PostgreSQL server's own timezone configuration.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

import asyncpg
import structlog

logger = structlog.get_logger("workers_db")


class Database:
    """Asyncpg pool wrapped with a start/stop lifecycle."""

    def __init__(self, dsn: str, min_size: int = 2, max_size: int = 10) -> None:
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._pool: asyncpg.Pool | None = None

    async def start(self) -> None:
        """Create the connection pool. Call once at application startup."""
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._min_size,
            max_size=self._max_size,
            server_settings={
                # Force UTC for every connection in the pool.
                # Ensures asyncpg returns UTC-aware datetimes and that
                # TIMESTAMPTZ values are displayed consistently in pgAdmin.
                "timezone": "UTC",
            },
        )
        logger.info("db.started", dsn=self._dsn)

    async def stop(self) -> None:
        """Close all pool connections. Call once at application shutdown."""
        if self._pool:
            await self._pool.close()
            logger.info("db.stopped")

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[asyncpg.Connection]:
        """
        Acquire a connection from the pool for the duration of the block.

            async with db.acquire() as conn:
                await conn.execute("INSERT INTO ...")
        """
        if self._pool is None:
            raise RuntimeError("Database.start() has not been called")
        async with self._pool.acquire() as conn:
            yield conn
