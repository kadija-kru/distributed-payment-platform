from fastapi import FastAPI

from shared.domain import process_accounts_event
from shared.service_runtime import service_lifespan

app = FastAPI(title="Accounts Service", lifespan=service_lifespan("accounts", process_accounts_event))


@app.get("/health")
async def health():
    return {"status": "ok", "service": "accounts"}
