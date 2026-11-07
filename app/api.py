import secrets
import uuid
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert

from app.config import Settings
from app.db import create_database
from app.models import Outbox, Payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetail

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def authenticate(request: Request, key: Annotated[str | None, Depends(api_key_header)]):
    expected = request.app.state.settings.api_key.get_secret_value()
    if key is None or not secrets.compare_digest(key.encode(), expected.encode()):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or missing API key")


def create_app(settings: Settings | None = None, session_factory=None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        engine = None
        if session_factory is None:
            engine, application.state.sessions = create_database(settings.database_url)
        yield
        if engine is not None:
            await engine.dispose()

    app = FastAPI(
        title="Asynchronous Payment Processing",
        version="0.1.0",
        lifespan=lifespan,
        dependencies=[Depends(authenticate)],
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.settings = settings
    app.state.sessions = session_factory

    @app.get("/health", include_in_schema=False)
    async def health(request: Request):
        async with request.app.state.sessions() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ok"}

    @app.get("/openapi.json", include_in_schema=False)
    async def openapi():
        return JSONResponse(app.openapi())

    @app.get("/docs", include_in_schema=False)
    async def docs():
        return get_swagger_ui_html(openapi_url="/openapi.json", title=app.title)

    @app.post("/api/v1/payments", status_code=202, response_model=PaymentAccepted)
    async def create_payment(
        body: PaymentCreate,
        request: Request,
        idempotency_key: Annotated[
            str, Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"^\S+$")
        ],
    ):
        payment_id = uuid.uuid4()
        async with request.app.state.sessions() as session, session.begin():
            result = await session.execute(
                insert(Payment)
                .values(
                    id=payment_id,
                    amount=body.amount,
                    currency=body.currency,
                    description=body.description,
                    payment_metadata=body.metadata,
                    status="pending",
                    idempotency_key=idempotency_key,
                    request_hash=body.fingerprint(),
                    webhook_url=str(body.webhook_url),
                    attempts=0,
                )
                .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
                .returning(Payment)
            )
            payment = result.scalar_one_or_none()
            if payment is None:
                payment = (
                    await session.execute(
                        select(Payment).where(Payment.idempotency_key == idempotency_key)
                    )
                ).scalar_one()
                if payment.request_hash != body.fingerprint():
                    raise HTTPException(409, "Idempotency key was used with a different request")
            else:
                session.add(
                    Outbox(
                        topic="payments.new", payload={"payment_id": str(payment_id), "attempt": 1}
                    )
                )
            response = PaymentAccepted(
                payment_id=payment.id, status=payment.status, created_at=payment.created_at
            )
        return response

    @app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetail)
    async def get_payment(payment_id: uuid.UUID, request: Request):
        async with request.app.state.sessions() as session:
            payment = await session.get(Payment, payment_id)
            if payment is None:
                raise HTTPException(404, "Payment not found")
            return PaymentDetail(
                payment_id=payment.id,
                amount=payment.amount,
                currency=payment.currency,
                description=payment.description,
                metadata=payment.payment_metadata,
                status=payment.status,
                webhook_url=payment.webhook_url,
                created_at=payment.created_at,
                processed_at=payment.processed_at,
                webhook_delivered_at=payment.webhook_delivered_at,
                delivery_failed_at=payment.delivery_failed_at,
                attempts=payment.attempts,
            )

    return app
