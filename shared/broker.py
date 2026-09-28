from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Awaitable, Callable
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import select

from shared.config import get_settings
from shared.db import session_scope
from shared.logging import correlation_id_var
from shared.models import OutboxEvent, ProcessedEvent

logger = logging.getLogger(__name__)
EventHandler = Callable[[dict], Awaitable[None]]


class InMemoryBroker:
    def __init__(self) -> None:
        self.messages: deque[tuple[str, dict]] = deque()

    async def publish(self, topic: str, event: dict) -> None:
        self.messages.append((topic, event))

    async def drain(self) -> list[tuple[str, dict]]:
        items = list(self.messages)
        self.messages.clear()
        return items


_broker = None


async def get_broker():
    global _broker
    if _broker is not None:
        return _broker
    settings = get_settings()
    if settings.kafka_bootstrap_servers.startswith("memory://"):
        _broker = InMemoryBroker()
    else:
        _broker = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
        await _broker.start()
    return _broker


def queue_outbox_event(session, topic: str, payload: dict, event_key: str) -> None:
    session.add(OutboxEvent(id=str(uuid4()), topic=topic, event_key=event_key, event_type=payload["event_type"], payload=payload))


async def flush_outbox_once() -> int:
    broker = await get_broker()
    async with session_scope() as session:
        rows = list((await session.execute(select(OutboxEvent).where(OutboxEvent.published_at.is_(None)).order_by(OutboxEvent.created_at).limit(50))).scalars())
        for row in rows:
            if isinstance(broker, InMemoryBroker):
                await broker.publish(row.topic, row.payload)
            else:
                await broker.send_and_wait(row.topic, json.dumps(row.payload).encode("utf-8"), key=row.event_key.encode("utf-8"))
            row.published_at = datetime.now(UTC)
        return len(rows)


async def outbox_publisher(stop_event: asyncio.Event) -> None:
    settings = get_settings()
    while not stop_event.is_set():
        await flush_outbox_once()
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=settings.publisher_poll_seconds)
        except TimeoutError:
            continue


async def already_processed(session, service_name: str, event: dict) -> bool:
    existing = await session.execute(
        select(ProcessedEvent).where(ProcessedEvent.event_id == event["event_id"], ProcessedEvent.service_name == service_name)
    )
    return existing.scalar_one_or_none() is not None


async def mark_processed(session, service_name: str, event: dict) -> None:
    session.add(ProcessedEvent(event_id=event["event_id"], service_name=service_name, event_type=event["event_type"]))


@asynccontextmanager
async def kafka_consumer(service_name: str):
    settings = get_settings()
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"payment-platform-{service_name}",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.stop()


async def run_kafka_worker(service_name: str, handler: EventHandler, stop_event: asyncio.Event) -> None:
    settings = get_settings()
    if settings.kafka_bootstrap_servers.startswith("memory://"):
        return
    async with kafka_consumer(service_name) as consumer:
        while not stop_event.is_set():
            result = await consumer.getmany(timeout_ms=1000, max_records=10)
            for records in result.values():
                for message in records:
                    event = message.value
                    correlation_id_var.set(event.get("correlation_id", "system"))
                    try:
                        await handler(event)
                    except Exception as exc:  # pragma: no cover
                        attempt = int(event.get("attempt", 0))
                        logger.exception("event-handler-failed", extra={"service": service_name})
                        producer = await get_broker()
                        if attempt >= settings.max_event_retries:
                            dlq_event = dict(event)
                            dlq_event["error"] = str(exc)
                            if isinstance(producer, InMemoryBroker):
                                await producer.publish(settings.kafka_dlq_topic, dlq_event)
                            else:
                                await producer.send_and_wait(settings.kafka_dlq_topic, json.dumps(dlq_event).encode("utf-8"))
                        else:
                            retry_event = dict(event)
                            retry_event["attempt"] = attempt + 1
                            if isinstance(producer, InMemoryBroker):
                                await producer.publish(settings.kafka_topic, retry_event)
                            else:
                                await producer.send_and_wait(settings.kafka_topic, json.dumps(retry_event).encode("utf-8"), key=event["payment_id"].encode("utf-8"))
                    else:
                        await consumer.commit()
