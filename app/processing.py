from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID

import httpx
from sqlalchemy import select

from app.models import Outbox, Payment
from app.schemas import PaymentEvent

MAX_ATTEMPTS = 3


class Gateway(Protocol):
    async def charge(self, payment_id: UUID) -> str: ...


class MissingPaymentError(Exception):
    pass


def retry_delay(attempt: int, base: float) -> float:
    return base * 2 ** (attempt - 1)


def is_finished(payment: Payment, event: PaymentEvent) -> bool:
    return (
        payment.webhook_delivered_at is not None
        or payment.delivery_failed_at is not None
        or event.attempt <= payment.attempts
    )


def record_failure(session, payment: Payment, event: PaymentEvent, error: str, base: float):
    now = datetime.now(UTC)
    payment.attempts = event.attempt
    payment.last_error = error
    if event.attempt >= MAX_ATTEMPTS:
        payment.delivery_failed_at = now
        session.add(
            Outbox(
                topic="payments.dlq",
                payload={**event.model_dump(mode="json"), "error": error},
                available_at=now,
            )
        )
    else:
        session.add(
            Outbox(
                topic="payments.new",
                payload={"payment_id": str(payment.id), "attempt": event.attempt + 1},
                available_at=now + timedelta(seconds=retry_delay(event.attempt, base)),
            )
        )


class PaymentProcessor:
    def __init__(
        self, sessions, gateway: Gateway, client: httpx.AsyncClient, retry_base: float = 1
    ):
        self.sessions = sessions
        self.gateway = gateway
        self.client = client
        self.retry_base = retry_base

    async def _locked_payment(self, session, event: PaymentEvent) -> Payment:
        payment = (
            await session.execute(
                select(Payment).where(Payment.id == event.payment_id).with_for_update()
            )
        ).scalar_one_or_none()
        if payment is None:
            raise MissingPaymentError(str(event.payment_id))
        if event.attempt > payment.attempts + 1:
            raise ValueError("Attempt is ahead of persisted progress")
        return payment

    async def process(self, event: PaymentEvent) -> None:
        # Commit the gateway result before contacting the webhook. A webhook failure
        # must never turn a successful payment into a failed one or charge it again.
        async with self.sessions() as session, session.begin():
            payment = await self._locked_payment(session, event)
            if is_finished(payment, event):
                return
            if payment.status == "pending":
                try:
                    result = await self.gateway.charge(payment.id)
                    if result not in {"succeeded", "failed"}:
                        raise ValueError("Invalid gateway result")
                except Exception as exc:
                    record_failure(
                        session, payment, event, f"gateway:{type(exc).__name__}", self.retry_base
                    )
                    return
                payment.status = result
                payment.processed_at = datetime.now(UTC)

        async with self.sessions() as session, session.begin():
            payment = await self._locked_payment(session, event)
            if is_finished(payment, event):
                return
            event_id = f"payment:{payment.id}:result"
            payload = {
                "event_id": event_id,
                "payment_id": str(payment.id),
                "status": payment.status,
                "amount": format(payment.amount, ".2f"),
                "currency": payment.currency,
                "description": payment.description,
                "metadata": payment.payment_metadata,
                "created_at": payment.created_at.isoformat(),
                "processed_at": payment.processed_at.isoformat(),
            }
            try:
                response = await self.client.post(
                    payment.webhook_url,
                    json=payload,
                    headers={"Idempotency-Key": event_id, "X-Webhook-Event-ID": event_id},
                )
                response.raise_for_status()
            except httpx.HTTPError as exc:
                # Never persist/log full exception text: it can contain callback secrets.
                record_failure(
                    session, payment, event, f"webhook:{type(exc).__name__}", self.retry_base
                )
                return
            payment.webhook_delivered_at = datetime.now(UTC)
            payment.attempts = event.attempt
            payment.last_error = None
