from fastapi import FastAPI

from shared.service_runtime import service_lifespan
from shared.domain import process_payment_event

app = FastAPI(title="Payments Service", lifespan=service_lifespan("payments", process_payment_event))


@app.get("/health")
async def health():
    return {"status": "ok", "service": "payments"}
