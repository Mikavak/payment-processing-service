import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import event, func, select
from sqlalchemy.orm import Session

from app.models import Outbox, Payment

pytestmark = pytest.mark.integration


async def counts(sessions):
    async with sessions() as session:
        return (
            await session.scalar(select(func.count()).select_from(Payment)),
            await session.scalar(select(func.count()).select_from(Outbox)),
        )


async def test_create_and_get(client, sessions, payload):
    response = await client.post(
        "/api/v1/payments", json=payload, headers={"Idempotency-Key": "one"}
    )
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pending"
    assert body["created_at"]
    result = await client.get(f"/api/v1/payments/{body['payment_id']}")
    assert result.status_code == 200
    assert result.json()["amount"] == "1200.50"
    assert result.json()["metadata"] == {"order_id": 42}
    assert result.json()["processed_at"] is None
    assert await counts(sessions) == (1, 1)
    async with sessions() as session:
        outbox = (await session.scalars(select(Outbox))).one()
        assert outbox.payload == {"payment_id": body["payment_id"], "attempt": 1}
        assert outbox.published_at is None


@pytest.mark.parametrize("key", [None, "wrong", "ключ"])
@pytest.mark.parametrize("path", ["/health", "/docs", "/openapi.json", "/api/v1/payments"])
async def test_all_routes_require_api_key(client, key, path):
    client.headers.clear()
    headers = {} if key is None else {"X-API-Key": key.encode()}
    response = (
        await client.get(path, headers=headers)
        if path != "/api/v1/payments"
        else (await client.post(path, json={}, headers=headers))
    )
    assert response.status_code == 401


async def test_missing_payment_and_invalid_id(client):
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404
    assert (await client.get("/api/v1/payments/not-a-uuid")).status_code == 422


async def test_missing_idempotency_key(client, payload):
    response = await client.post("/api/v1/payments", json=payload)
    assert response.status_code == 422


async def test_conflict_does_not_create_event(client, sessions, payload):
    headers = {"Idempotency-Key": "same"}
    first = await client.post("/api/v1/payments", json=payload, headers=headers)
    repeat = await client.post("/api/v1/payments", json=payload, headers=headers)
    conflict = await client.post(
        "/api/v1/payments", json={**payload, "amount": "999"}, headers=headers
    )
    assert repeat.json() == first.json()
    assert conflict.status_code == 409
    assert await counts(sessions) == (1, 1)


async def test_concurrent_identical_requests_create_one_payment_and_event(
    client, sessions, payload
):
    responses = await asyncio.gather(
        *[
            client.post("/api/v1/payments", json=payload, headers={"Idempotency-Key": "race"})
            for _ in range(30)
        ]
    )
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert await counts(sessions) == (1, 1)


async def test_concurrent_conflicting_requests(client, sessions, payload):
    responses = await asyncio.gather(
        *[
            client.post(
                "/api/v1/payments",
                json={**payload, "amount": str(n + 1)},
                headers={"Idempotency-Key": "race-conflict"},
            )
            for n in range(20)
        ]
    )
    assert [r.status_code for r in responses].count(202) == 1
    assert [r.status_code for r in responses].count(409) == 19
    assert await counts(sessions) == (1, 1)


async def test_payment_rolls_back_when_outbox_insert_fails(client, sessions, payload):
    def fail_outbox(session, *_):
        if any(isinstance(item, Outbox) for item in session.new):
            raise RuntimeError("Injected outbox failure")

    event.listen(Session, "before_flush", fail_outbox)
    try:
        with pytest.raises(RuntimeError, match="Injected outbox"):
            await client.post(
                "/api/v1/payments", json=payload, headers={"Idempotency-Key": "rollback"}
            )
    finally:
        event.remove(Session, "before_flush", fail_outbox)
    assert await counts(sessions) == (0, 0)
