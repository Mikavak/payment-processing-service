import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import select

from app.models import Outbox, Payment
from app.processing import PaymentProcessor
from app.schemas import PaymentEvent

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("result", ["succeeded", "failed"])
async def test_result_and_duplicate_delivery(sessions, payment_id, result):
    gateway = AsyncMock()
    gateway.charge.return_value = result
    requests = []

    def callback(request):
        requests.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(callback)) as client:
        processor = PaymentProcessor(sessions, gateway, client)
        event = PaymentEvent(payment_id=payment_id)
        await asyncio.gather(*[processor.process(event) for _ in range(10)])
        await processor.process(event)
    gateway.charge.assert_awaited_once_with(payment_id)
    assert len(requests) == 1
    assert json.loads(requests[0].content)["status"] == result
    assert requests[0].headers["Idempotency-Key"] == f"payment:{payment_id}:result"
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == result
        assert payment.processed_at is not None
        assert payment.webhook_delivered_at is not None
        assert payment.attempts == 1


async def test_webhook_retries_then_success_do_not_recharge(sessions, payment_id):
    gateway = AsyncMock()
    gateway.charge.return_value = "succeeded"
    requests = []

    def callback(request):
        requests.append(request)
        return httpx.Response(503 if len(requests) < 3 else 204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(callback)) as client:
        for attempt in range(1, 4):
            # Reconstruct the processor on every delivery to model process restarts.
            processor = PaymentProcessor(sessions, gateway, client, retry_base=1)
            before = datetime.now(UTC)
            await processor.process(PaymentEvent(payment_id=payment_id, attempt=attempt))
            if attempt < 3:
                async with sessions() as session:
                    events = (await session.scalars(select(Outbox))).all()
                    retry = next(e for e in events if e.payload["attempt"] == attempt + 1)
                    assert (retry.available_at - before).total_seconds() >= 2 ** (attempt - 1)
            # A duplicate of this attempt cannot bypass the scheduled retry.
            await processor.process(PaymentEvent(payment_id=payment_id, attempt=attempt))
    assert len(requests) == 3
    assert len({r.headers["Idempotency-Key"] for r in requests}) == 1
    assert len({r.content for r in requests}) == 1
    gateway.charge.assert_awaited_once()
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.attempts == 3
        assert payment.webhook_delivered_at is not None
        assert payment.delivery_failed_at is None


@pytest.mark.parametrize("failure", ["http", "timeout", "redirect"])
async def test_three_failures_create_one_dlq_event(sessions, payment_id, failure):
    gateway = AsyncMock()
    gateway.charge.return_value = "succeeded"
    requests = []

    def callback(request):
        requests.append(request)
        if failure == "timeout":
            raise httpx.ReadTimeout("timeout", request=request)
        return httpx.Response(302 if failure == "redirect" else 500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(callback)) as client:
        processor = PaymentProcessor(sessions, gateway, client)
        for attempt in range(1, 4):
            await processor.process(PaymentEvent(payment_id=payment_id, attempt=attempt))
        for attempt in range(1, 4):
            await processor.process(PaymentEvent(payment_id=payment_id, attempt=attempt))
    assert len(requests) == 3
    gateway.charge.assert_awaited_once()
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == "succeeded"
        assert payment.attempts == 3
        assert payment.delivery_failed_at is not None
        assert payment.webhook_delivered_at is None
        dlq = (await session.scalars(select(Outbox).where(Outbox.topic == "payments.dlq"))).one()
        assert dlq.payload["attempt"] == 3


async def test_gateway_exception_retries_then_dlq(sessions, payment_id):
    gateway = AsyncMock()
    gateway.charge.side_effect = TimeoutError()
    client = AsyncMock()
    processor = PaymentProcessor(sessions, gateway, client)
    for attempt in range(1, 4):
        await processor.process(PaymentEvent(payment_id=payment_id, attempt=attempt))
    assert gateway.charge.await_count == 3
    client.post.assert_not_awaited()
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == "pending"  # No known gateway outcome; do not invent one.
        assert payment.delivery_failed_at is not None


async def test_crash_before_webhook_reuses_committed_gateway_result(sessions, payment_id):
    gateway = AsyncMock()
    gateway.charge.return_value = "succeeded"
    client = AsyncMock()
    client.post.side_effect = asyncio.CancelledError()
    event = PaymentEvent(payment_id=payment_id)
    with pytest.raises(asyncio.CancelledError):
        await PaymentProcessor(sessions, gateway, client).process(event)
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == "succeeded"
        assert payment.attempts == 0
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200))
    ) as recovered:
        await PaymentProcessor(sessions, gateway, recovered).process(event)
    gateway.charge.assert_awaited_once()
