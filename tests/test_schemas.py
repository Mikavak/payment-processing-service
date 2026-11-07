from uuid import UUID

import pytest
from pydantic import ValidationError

from app.gateway import SimulatedGateway
from app.processing import retry_delay
from app.schemas import PaymentCreate, PaymentEvent


@pytest.mark.parametrize("amount", ["0", "-1", "0.001", "NaN", "Infinity", "1e20"])
def test_invalid_amounts(payload, amount):
    with pytest.raises(ValidationError):
        PaymentCreate(**{**payload, "amount": amount})


@pytest.mark.parametrize("currency", ["BTC", "rub", "", "GBP"])
def test_invalid_currency(payload, currency):
    with pytest.raises(ValidationError):
        PaymentCreate(**{**payload, "currency": currency})


@pytest.mark.parametrize(
    "url", ["ftp://example.com/a", "not-a-url", "https://user:pass@example.com", "https://x.com/#x"]
)
def test_invalid_webhook(payload, url):
    with pytest.raises(ValidationError):
        PaymentCreate(**{**payload, "webhook_url": url})


def test_canonical_fingerprint(payload):
    first = PaymentCreate(**{**payload, "amount": "12.0", "metadata": {"a": 1, "b": 2}})
    second = PaymentCreate(**{**payload, "amount": "12.00", "metadata": {"b": 2, "a": 1}})
    assert first.fingerprint() == second.fingerprint()
    assert first.fingerprint() != PaymentCreate(**payload).fingerprint()


def test_metadata_limit(payload):
    with pytest.raises(ValidationError):
        PaymentCreate(**{**payload, "metadata": {"large": "x" * 17000}})


def test_extra_fields_forbidden(payload):
    with pytest.raises(ValidationError):
        PaymentCreate(**payload, status="succeeded")


@pytest.mark.parametrize("attempt", [0, 4, -1])
def test_attempt_bounds(attempt):
    with pytest.raises(ValidationError):
        PaymentEvent(payment_id=UUID(int=1), attempt=attempt)


def test_exponential_delays():
    assert [retry_delay(n, 1) for n in [1, 2]] == [1, 2]
    assert [retry_delay(n, 3) for n in [1, 2]] == [3, 6]


async def test_gateway_distribution_delay_and_stable_replay(monkeypatch):
    delays = []

    async def sleep(delay):
        delays.append(delay)

    monkeypatch.setattr("app.gateway.asyncio.sleep", sleep)
    gateway = SimulatedGateway()
    outcomes = [await gateway.charge(UUID(int=i)) for i in range(100)]
    assert outcomes.count("succeeded") == 90
    assert outcomes.count("failed") == 10
    assert all(2 <= delay <= 5 for delay in delays)
    assert await gateway.charge(UUID(int=9)) == "failed"
