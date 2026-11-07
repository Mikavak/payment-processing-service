from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.schemas import Channel

PAYMENTS_EXCHANGE = RabbitExchange("payments", durable=True)
DEAD_EXCHANGE = RabbitExchange("payments.dead", durable=True)
NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "payments.dead",
        "x-dead-letter-routing-key": "payments.dlq",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
        # Separate safety cap for transport redeliveries, not business attempts.
        # A positive value also avoids pamqp 3.3's signed-octet encoding issue.
        "x-delivery-limit": 1000,
        "x-single-active-consumer": True,
    },
)
DLQ = RabbitQueue("payments.dlq", durable=True, arguments={"x-queue-type": "quorum"})


def create_broker(url: str) -> RabbitBroker:
    return RabbitBroker(
        url,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
    )


async def declare_topology(broker: RabbitBroker):
    exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    dead = await broker.declare_exchange(DEAD_EXCHANGE)
    queue = await broker.declare_queue(NEW_QUEUE)
    dlq = await broker.declare_queue(DLQ)
    await queue.bind(exchange, routing_key="payments.new")
    await dlq.bind(dead, routing_key="payments.dlq")


class EventPublisher:
    def __init__(self, broker: RabbitBroker):
        self.broker = broker

    async def publish(self, event):
        exchange = DEAD_EXCHANGE if event.topic == "payments.dlq" else PAYMENTS_EXCHANGE
        await self.broker.publish(
            event.payload,
            exchange=exchange,
            routing_key=event.topic,
            message_id=str(event.id),
            persist=True,
            mandatory=True,
            timeout=5,
        )
