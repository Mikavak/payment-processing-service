import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy import func, select

from app.models import Outbox

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(self, sessions, publisher):
        self.sessions = sessions
        self.publisher = publisher

    async def publish_one(self) -> bool:
        async with self.sessions() as session, session.begin():
            event = (
                await session.execute(
                    select(Outbox)
                    .where(Outbox.published_at.is_(None), Outbox.available_at <= func.now())
                    .order_by(Outbox.available_at, Outbox.id)
                    .with_for_update(skip_locked=True)
                    .limit(1)
                )
            ).scalar_one_or_none()
            if event is None:
                return False
            await self.publisher.publish(event)
            # A crash after confirm but before commit can publish twice. The
            # consumer uses payment state + attempt to safely ignore duplicates.
            event.published_at = datetime.now(UTC)
        return True

    async def run(self, poll_interval: float):
        while True:
            try:
                if await self.publish_one():
                    continue
            except Exception as exc:
                logger.warning("Outbox publish failed (%s); will retry", type(exc).__name__)
            await asyncio.sleep(poll_interval)
