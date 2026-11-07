import os
from uuid import UUID

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from app.api import create_app
from app.config import Settings
from app.db import create_database

TEST_API_KEY = "test-api-key-at-least-16-chars"


@pytest.fixture
def payload():
    return {
        "amount": "1200.50",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://merchant.example/webhook",
    }


@pytest.fixture
async def sessions():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL integration tests")
    if not (make_url(url).database or "").endswith("_test"):
        pytest.fail("Refusing to truncate a database whose name does not end with _test")
    engine, factory = create_database(url)
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox"))
    try:
        yield factory
    finally:
        await engine.dispose()


@pytest.fixture
async def client(sessions):
    settings = Settings(
        _env_file=None,
        api_key=TEST_API_KEY,
        database_url="postgresql+asyncpg://unused/unused_test",
        rabbitmq_url="amqp://unused/",
    )
    application = create_app(settings, sessions)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application),
        base_url="http://test",
        headers={"X-API-Key": TEST_API_KEY},
    ) as http:
        yield http


@pytest.fixture
async def payment_id(client, payload):
    response = await client.post(
        "/api/v1/payments", json=payload, headers={"Idempotency-Key": "test-payment"}
    )
    assert response.status_code == 202, response.text
    return UUID(response.json()["payment_id"])
