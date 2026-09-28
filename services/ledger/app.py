from fastapi import FastAPI

from shared.domain import process_ledger_event
from shared.service_runtime import service_lifespan

app = FastAPI(title="Ledger Service", lifespan=service_lifespan("ledger", process_ledger_event))


@app.get("/health")
async def health():
    return {"status": "ok", "service": "ledger"}
