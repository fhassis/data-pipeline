"""
workers/core/health.py
======================
Runtime health state for a worker instance.

Populated by NATS connection callbacks and the pull loop. Read by the
heartbeat loop to decide whether to touch the liveness file, and later
by a /health HTTP endpoint backed by as_dict().
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class WorkerHealth:
    """
    Runtime health snapshot for a single worker instance.

    Updated by NATS connection callbacks and by the pull loop on each
    received message. Read by the heartbeat loop to decide whether to
    touch the liveness file, and later by an HTTP /health endpoint.
    """

    worker_name: str
    connected: bool = False
    last_message_at: datetime | None = None
    last_error: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def is_healthy(self) -> bool:
        """
        Return True if the worker is connected and not stale.

        A worker is considered stale if it has received at least one message
        but has not received a new one in the last 120 seconds. A worker that
        is connected but has never received a message is still considered
        healthy — it may simply be waiting for producers to start.
        """
        if not self.connected:
            return False
        if self.last_message_at is None:
            return True
        stale = (
            datetime.now(timezone.utc) - self.last_message_at
        ).total_seconds() > 120
        return not stale

    def as_dict(self) -> dict:
        """
        Serialise health state to a plain dict suitable for JSON responses.

        Intended for use by a future /health HTTP endpoint. All datetime
        fields are ISO 8601 strings so the dict is directly JSON-serialisable.
        """
        return {
            "healthy": self.is_healthy,
            "worker": self.worker_name,
            "connected": self.connected,
            "started_at": self.started_at.isoformat(),
            "last_message_at": self.last_message_at.isoformat()
            if self.last_message_at
            else None,
            "last_error": self.last_error,
        }
