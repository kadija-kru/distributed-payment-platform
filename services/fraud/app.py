from fastapi import FastAPI

from shared.domain import process_fraud_event
from shared.service_runtime import service_lifespan

app = FastAPI(title="Fraud Service", lifespan=service_lifespan("fraud", process_fraud_event))


@app.get("/health")
async def health():
    return {"status": "ok", "service": "fraud"}
