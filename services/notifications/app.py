from fastapi import FastAPI

from shared.domain import process_notification_event
from shared.service_runtime import service_lifespan

app = FastAPI(title="Notifications Service", lifespan=service_lifespan("notifications", process_notification_event))


@app.get("/health")
async def health():
    return {"status": "ok", "service": "notifications"}
