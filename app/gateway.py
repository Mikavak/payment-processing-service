import asyncio
import random
from uuid import UUID


class SimulatedGateway:
    async def charge(self, payment_id: UUID) -> str:
        await asyncio.sleep(random.uniform(2, 5))
        # Stable outcome on crash/replay: a real gateway must accept payment_id
        # as its idempotency key. UUID4's random final bits give a ~90/10 split.
        return "succeeded" if payment_id.int % 10 < 9 else "failed"
