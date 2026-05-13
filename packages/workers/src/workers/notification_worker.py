"""
workers/notification_worker.py
===============================
Consumes outcome notifications and simulates dispatching alerts.

Pipeline position:

    notifications.sensor.> ──> [NotificationWorker] ──> (Telegram / log)

Pure consumer — STREAM and CONSUMER are set, no publishing. The simplest
worker in the pipeline: decode, log, ack.

To add real Telegram notifications: replace the logger.info call with an
httpx POST to the Telegram Bot API. No other changes needed.
"""

import msgspec
from msgspec.json import decode as json_decode
from nats.aio.msg import Msg
from shared.models import Notification

from workers.core import BaseWorker


class NotificationWorker(BaseWorker):
    STREAM = "NOTIFICATIONS"
    CONSUMER = "notification"

    async def on_message(self, msg: Msg) -> None:
        """
        Notifies the user of important events, e.g. processing failures.
        """
        try:
            notif: Notification = json_decode(msg.data, type=Notification)
        except msgspec.DecodeError as e:
            self.logger.error("notifier.decode_error", subject=msg.subject, error=str(e))
            await msg.ack()
            return

        icon = "✅" if notif.success else "❌"
        self.logger.info(
            "notifier.alert",
            icon=icon,
            sensor_id=notif.sensor_id,
            success=notif.success,
            message=notif.message,
        )

        await msg.ack()
