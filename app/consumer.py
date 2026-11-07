import asyncio
import logging
import signal

import httpx
from faststream import AckPolicy
from faststream.rabbit.annotations import RabbitMessage
from pydantic import ValidationError

from app.config import Settings
from app.db import create_database
from app.gateway import SimulatedGateway
from app.messaging import (
    NEW_QUEUE,
    PAYMENTS_EXCHANGE,
    EventPublisher,
    create_broker,
    declare_topology,
)
from app.outbox import OutboxRelay
from app.processing import MissingPaymentError, PaymentProcessor
from app.schemas import PaymentEvent

logger = logging.getLogger(__name__)


def register_handler(broker, processor):
    async def raw_body(message):
        return message.body

    @broker.subscriber(NEW_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.MANUAL, decoder=raw_body)
    async def handle(body: bytes, message: RabbitMessage):
        try:
            event = PaymentEvent.model_validate_json(body)
            await processor.process(event)
        except (ValidationError, MissingPaymentError, ValueError) as exc:
            logger.warning("Invalid event rejected to DLQ (%s)", type(exc).__name__)
            await message.reject(requeue=False)
        except Exception as exc:
            logger.warning("Infrastructure failure (%s); requeueing", type(exc).__name__)
            await asyncio.sleep(1)
            await message.nack(requeue=True)
        else:
            await message.ack()

    return handle


async def main():
    settings = Settings()
    logging.basicConfig(level=settings.log_level)
    engine, sessions = create_database(settings.database_url)
    broker = create_broker(settings.rabbitmq_url)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    async with httpx.AsyncClient(
        timeout=settings.webhook_timeout, follow_redirects=False
    ) as client:
        processor = PaymentProcessor(
            sessions, SimulatedGateway(), client, settings.retry_base_delay
        )
        register_handler(broker, processor)
        try:
            await broker.connect()
            await declare_topology(broker)
            await broker.start()
            relay = OutboxRelay(sessions, EventPublisher(broker))
            task = asyncio.create_task(relay.run(settings.outbox_poll_interval))
            logger.info("Consumer and outbox relay started")
            try:
                await stop.wait()
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        finally:
            await broker.stop()
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
