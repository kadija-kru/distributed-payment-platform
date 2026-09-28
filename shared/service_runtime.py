from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Awaitable, Callable

from fastapi import FastAPI

from shared.broker import outbox_publisher, run_kafka_worker
from shared.db import init_db, session_scope
from shared.logging import configure_logging

Handler = Callable[[dict], Awaitable[None]]


def service_lifespan(service_name: str, handler: Handler | None = None):
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        configure_logging(service_name)
        await init_db()
        stop_event = asyncio.Event()
        tasks = []
        if service_name == "payments":
            tasks.append(asyncio.create_task(outbox_publisher(stop_event)))
        if handler is not None:
            async def wrapped(event: dict):
                async with session_scope() as session:
                    await handler(session, event)
            tasks.append(asyncio.create_task(run_kafka_worker(service_name, wrapped, stop_event)))
        try:
            yield
        finally:
            stop_event.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    return lifespan
