import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event, select, update
from sqlalchemy.orm import Session

from app.models import Outbox
from app.outbox import OutboxRelay

pytestmark = pytest.mark.integration


async def test_failed_publish_is_not_marked_and_can_recover(sessions, payment_id):
    publisher = AsyncMock()
    publisher.publish.side_effect = ConnectionError()
    relay = OutboxRelay(sessions, publisher)
    with pytest.raises(ConnectionError):
        await relay.publish_one()
    async with sessions() as session:
        assert (await session.scalars(select(Outbox))).one().published_at is None
    publisher.publish.side_effect = None
    assert await relay.publish_one()
    assert not await relay.publish_one()
    async with sessions() as session:
        assert (await session.scalars(select(Outbox))).one().published_at is not None


async def test_not_due_events_wait(sessions, payment_id):
    async with sessions() as session, session.begin():
        await session.execute(
            update(Outbox).values(available_at=datetime.now(UTC) + timedelta(seconds=60))
        )
    publisher = AsyncMock()
    assert not await OutboxRelay(sessions, publisher).publish_one()
    publisher.publish.assert_not_awaited()


async def test_concurrent_relays_publish_once(sessions, payment_id):
    publisher = AsyncMock()
    relays = [OutboxRelay(sessions, publisher) for _ in range(10)]
    results = await asyncio.gather(*[relay.publish_one() for relay in relays])
    assert sum(results) == 1
    publisher.publish.assert_awaited_once()


async def test_crash_after_confirm_before_commit_republishes_same_event(sessions, payment_id):
    published = []

    async def publish(item):
        published.append(item.id)

    publisher = AsyncMock()
    publisher.publish.side_effect = publish
    relay = OutboxRelay(sessions, publisher)

    def fail_commit(session):
        raise RuntimeError("Crash after publish confirmation")

    event.listen(Session, "before_commit", fail_commit)
    try:
        with pytest.raises(RuntimeError, match="Crash after"):
            await relay.publish_one()
    finally:
        event.remove(Session, "before_commit", fail_commit)
    assert await relay.publish_one()
    assert len(published) == 2
    assert published[0] == published[1]
