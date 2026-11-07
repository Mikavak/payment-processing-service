"""Run inside the API container with the demo Compose profile enabled."""

import asyncio
import json
import os
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import httpx


async def main():
    headers = {"X-API-Key": os.environ["API_KEY"]}
    async with httpx.AsyncClient(base_url="http://localhost:8000", timeout=10) as client:
        assert (await client.get("/health")).status_code == 401
        assert (await client.get("/health", headers=headers)).status_code == 200
        payload = {
            "amount": "19.90",
            "currency": "EUR",
            "metadata": {"smoke": True},
            "description": "Compose smoke test",
            "webhook_url": "http://webhook:8080/webhook",
        }
        idem_headers = {**headers, "Idempotency-Key": f"smoke-{uuid4()}"}
        responses = await asyncio.gather(
            *[
                client.post("/api/v1/payments", json=payload, headers=idem_headers)
                for _ in range(12)
            ]
        )
        assert all(r.status_code == 202 for r in responses)
        ids = {r.json()["payment_id"] for r in responses}
        assert len(ids) == 1
        payment_id = ids.pop()
        failed = await client.post(
            "/api/v1/payments",
            json={**payload, "webhook_url": "http://webhook:8080/fail"},
            headers={**headers, "Idempotency-Key": f"smoke-fail-{uuid4()}"},
        )
        assert failed.status_code == 202
        failed_id = failed.json()["payment_id"]
        async with asyncio.timeout(45):
            while True:
                payment = (
                    await client.get(f"/api/v1/payments/{payment_id}", headers=headers)
                ).json()
                exhausted = (
                    await client.get(f"/api/v1/payments/{failed_id}", headers=headers)
                ).json()
                if payment["webhook_delivered_at"] and exhausted["delivery_failed_at"]:
                    break
                await asyncio.sleep(0.2)
        assert payment["attempts"] == 1
        assert exhausted["attempts"] == 3
        assert exhausted["status"] in {"succeeded", "failed"}
        events = (await client.get("http://webhook:8080/events")).json()
        assert len([e for e in events if e["payment_id"] == payment_id]) == 1

        url = urlsplit(os.environ["RABBITMQ_URL"])
        auth = (unquote(url.username), unquote(url.password))
        async with asyncio.timeout(10):
            while True:
                response = await client.post(
                    "http://rabbitmq:15672/api/queues/%2F/payments.dlq/get",
                    auth=auth,
                    json={"count": 100, "ackmode": "ack_requeue_true", "encoding": "auto"},
                )
                response.raise_for_status()
                if any(
                    json.loads(e["payload"])["payment_id"] == failed_id for e in response.json()
                ):
                    break
                await asyncio.sleep(0.2)
        print("PASS: authenticated API, 12 concurrent HTTP requests -> one payment")
        print("PASS: real gateway simulation, webhook delivery, 3 failed attempts -> RabbitMQ DLQ")


if __name__ == "__main__":
    asyncio.run(main())
