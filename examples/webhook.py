"""Demo receiver. In-memory storage is intentional; never use this in production."""

from fastapi import FastAPI, Response

app = FastAPI()
received: dict[str, dict] = {}


@app.post("/webhook")
async def webhook(body: dict):
    received.setdefault(body["event_id"], body)
    return {"received": True}


@app.get("/events")
async def events():
    return list(received.values())


@app.post("/fail")
async def fail():
    return Response(status_code=503)
