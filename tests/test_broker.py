import asyncio
import json
import os
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import httpx
import pytest
from aio_pika.exceptions import DeliveryError
from faststream.exceptions import IncorrectState
from sqlalchemy import select, update

from app.consumer import register_handler
from app.messaging import (
    DLQ,
    NEW_QUEUE,
    PAYMENTS_EXCHANGE,
    EventPublisher,
    create_broker,
    declare_topology,
)
from app.models import Outbox, Payment
from app.outbox import OutboxRelay
from app.processing import PaymentProcessor

pytestmark = [pytest.mark.integration, pytest.mark.broker]


@pytest.fixture
async def broker():
    url = os.environ.get("TEST_RABBITMQ_URL")
    if not url:
        pytest.skip("Set TEST_RABBITMQ_URL for real RabbitMQ tests")
    instance = create_broker(url)
    await instance.connect()
    await declare_topology(instance)
    for definition in (NEW_QUEUE, DLQ):
        queue = await instance.declare_queue(definition)
        await queue.purge()
    try:
        yield instance
    finally:
        await instance.stop()


async def get_message(queue):
    async with asyncio.timeout(10):
        while True:
            message = await queue.get(fail=False)
            if message is not None:
                return message
            await asyncio.sleep(0.02)


async def wait_for_delivery(sessions, payment_id):
    async with asyncio.timeout(10):
        while True:
            async with sessions() as session:
                payment = await session.get(Payment, payment_id)
                if payment.webhook_delivered_at:
                    return payment
            await asyncio.sleep(0.02)


async def test_confirmed_persistent_publish_and_redelivery(broker, sessions, payment_id):
    relay = OutboxRelay(sessions, EventPublisher(broker))
    assert await relay.publish_one()
    queue = await broker.declare_queue(NEW_QUEUE)
    # Single-active-consumer queues require basic.consume, not basic.get.
    async with asyncio.timeout(10), queue.iterator() as messages:
        first = await anext(messages)
        assert first.delivery_mode.value == 2
        assert json.loads(first.body)["payment_id"] == str(payment_id)
        event_id = first.message_id
        await first.nack(requeue=True)
        second = await anext(messages)
        assert second.redelivered
        assert second.message_id == event_id
        await second.ack()
    assert not await relay.publish_one()


async def test_unroutable_publish_keeps_outbox_pending(broker, sessions, payment_id):
    queue = await broker.declare_queue(NEW_QUEUE)
    exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    await queue.unbind(exchange, routing_key="payments.new")
    relay = OutboxRelay(sessions, EventPublisher(broker))
    try:
        with pytest.raises(DeliveryError):
            await relay.publish_one()
        async with sessions() as session:
            assert (await session.scalars(select(Outbox))).one().published_at is None
    finally:
        await queue.bind(exchange, routing_key="payments.new")
    assert await relay.publish_one()


async def test_broker_unavailable_then_recovery(broker, sessions, payment_id):
    publisher = EventPublisher(broker)
    relay = OutboxRelay(sessions, publisher)
    await broker.stop()
    with pytest.raises(IncorrectState):
        await relay.publish_one()
    async with sessions() as session:
        assert (await session.scalars(select(Outbox))).one().published_at is None
    await broker.connect()
    await declare_topology(broker)
    assert await relay.publish_one()


async def test_real_consumer_duplicates_are_idempotent(broker, sessions, payment_id):
    gateway = AsyncMock()
    gateway.charge.return_value = "succeeded"
    callbacks = []

    def callback(request):
        callbacks.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(callback)) as client:
        processor = PaymentProcessor(sessions, gateway, client)
        handled = 0
        all_handled = asyncio.Event()
        original = processor.process

        async def counted(event):
            nonlocal handled
            await original(event)
            handled += 1
            if handled == 5:
                all_handled.set()

        processor.process = counted
        register_handler(broker, processor)
        await broker.start()
        for _ in range(5):
            await broker.publish(
                {"payment_id": str(payment_id), "attempt": 1},
                exchange=PAYMENTS_EXCHANGE,
                routing_key="payments.new",
                persist=True,
            )
        await asyncio.wait_for(all_handled.wait(), timeout=10)
        await wait_for_delivery(sessions, payment_id)
    assert len(callbacks) == 1
    gateway.charge.assert_awaited_once()


async def test_real_pipeline_retries_and_dlq(broker, sessions, payment_id):
    gateway = AsyncMock()
    gateway.charge.return_value = "succeeded"
    callbacks = []

    def callback(request):
        callbacks.append(request)
        return httpx.Response(503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(callback)) as client:
        register_handler(broker, PaymentProcessor(sessions, gateway, client, retry_base=0.05))
        await broker.start()
        relay = OutboxRelay(sessions, EventPublisher(broker))
        task = asyncio.create_task(relay.run(0.01))
        try:
            dlq = await broker.declare_queue(DLQ)
            message = await get_message(dlq)
            body = json.loads(message.body)
            assert body["payment_id"] == str(payment_id)
            assert body["attempt"] == 3
            assert body["error"] == "webhook:HTTPStatusError"
            await message.ack()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert len(callbacks) == 3
    gateway.charge.assert_awaited_once()
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == "succeeded"
        assert payment.delivery_failed_at
        events = (await session.scalars(select(Outbox))).all()
        assert len(events) == 4  # initial, two retries, final DLQ
        assert all(e.published_at for e in events)


@pytest.mark.parametrize("body", [b"not-json", b"[]", b'{"payment_id":"invalid"}', b"\xff"])
async def test_poison_messages_are_dead_lettered(broker, body):
    processor = AsyncMock()
    register_handler(broker, processor)
    await broker.start()
    await broker.publish(body, exchange=PAYMENTS_EXCHANGE, routing_key="payments.new", persist=True)
    queue = await broker.declare_queue(DLQ)
    message = await get_message(queue)
    assert message.body == body
    assert message.headers["x-death"][0]["reason"] == "rejected"
    await message.ack()
    processor.process.assert_not_awaited()


async def test_infrastructure_failure_is_requeued(broker):
    processor = AsyncMock()
    recovered = asyncio.Event()

    async def process(event):
        if processor.process.await_count == 1:
            raise ConnectionError("Database temporarily unavailable")
        recovered.set()

    processor.process.side_effect = process
    register_handler(broker, processor)
    await broker.start()
    await broker.publish(
        {"payment_id": "00000000-0000-0000-0000-000000000001", "attempt": 1},
        exchange=PAYMENTS_EXCHANGE,
        routing_key="payments.new",
        persist=True,
    )
    await asyncio.wait_for(recovered.wait(), timeout=10)
    assert processor.process.await_count == 2


async def test_relay_restart_ignores_already_delivered_payment(broker, sessions, payment_id):
    gateway = AsyncMock()
    gateway.charge.return_value = "succeeded"
    callbacks = []

    def callback(request):
        callbacks.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(callback)) as client:
        processor = PaymentProcessor(sessions, gateway, client)
        register_handler(broker, processor)
        await broker.start()
        relay = OutboxRelay(sessions, EventPublisher(broker))
        assert await relay.publish_one()
        await wait_for_delivery(sessions, payment_id)
        # Simulate confirmation received, but publication mark not committed.
        async with sessions() as session, session.begin():
            await session.execute(
                update(Outbox).values(published_at=None, available_at=datetime.now(UTC))
            )
        done = asyncio.Event()
        original = processor.process

        async def replay(event):
            await original(event)
            done.set()

        processor.process = replay
        assert await OutboxRelay(sessions, EventPublisher(broker)).publish_one()
        await asyncio.wait_for(done.wait(), timeout=10)
    assert len(callbacks) == 1
    gateway.charge.assert_awaited_once()
