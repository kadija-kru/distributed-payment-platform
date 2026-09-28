import asyncio
import importlib
import os
import sys

import pytest
from fastapi.testclient import TestClient


def _reset_runtime(tmp_path):
    os.environ['APP_DATABASE_URL'] = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    os.environ['APP_KAFKA_BOOTSTRAP_SERVERS'] = 'memory://'
    os.environ['APP_REDIS_URL'] = 'memory://'
    os.environ['APP_JWT_SECRET'] = 'test-secret-key-with-32-plus-bytes'
    os.environ['APP_RATE_LIMIT_PER_MINUTE'] = '1000'
    os.environ['APP_FRAUD_REJECT_ABOVE'] = '10000'
    import shared.config as config
    import shared.db as db
    import shared.cache as cache
    import shared.broker as broker
    import shared.metrics as metrics

    config.get_settings.cache_clear()
    if db._engine is not None:
        asyncio.run(db._engine.dispose())
    db._engine = None
    db._session_factory = None
    cache._cache = None
    broker._broker = None
    metrics._metrics.clear()
    for module_name in [
        'gateway.app',
        'services.accounts.app',
        'services.payments.app',
        'services.fraud.app',
        'services.ledger.app',
        'services.notifications.app',
    ]:
        if module_name in sys.modules:
            importlib.reload(importlib.import_module(module_name))


@pytest.fixture
def client(tmp_path):
    _reset_runtime(tmp_path)
    from gateway.app import app
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def auth_headers(client):
    token = client.post('/auth/token', json={'username': 'demo', 'password': 'demo-password'}).json()['access_token']
    return {'Authorization': 'Bearer ' + token}


async def drain_events(max_cycles: int = 30):
    from shared.broker import flush_outbox_once, get_broker
    from shared.config import get_settings
    from shared.db import session_scope
    from shared.domain import (
        process_accounts_event,
        process_fraud_event,
        process_ledger_event,
        process_notification_event,
        process_payment_event,
    )

    handler_map = {
        'payment.created': [process_accounts_event],
        'payment.funds_reserved': [process_payment_event],
        'payment.fraud_check_requested': [process_fraud_event],
        'payment.fraud_approved': [process_payment_event],
        'payment.fraud_rejected': [process_payment_event],
        'payment.release_funds_requested': [process_accounts_event],
        'payment.funds_released': [process_payment_event],
        'payment.ledger_post_requested': [process_ledger_event],
        'payment.ledger_posted': [process_payment_event],
        'payment.completed': [process_notification_event],
        'payment.failed': [process_notification_event],
    }
    settings = get_settings()
    broker = await get_broker()
    dlq = []
    for _ in range(max_cycles):
        flushed = await flush_outbox_once()
        events = await broker.drain()
        if flushed == 0 and not events:
            break
        for topic, event in events:
            if topic == settings.kafka_dlq_topic:
                dlq.append(event)
                continue
            for handler in handler_map.get(event['event_type'], []):
                try:
                    async with session_scope() as session:
                        await handler(session, event)
                except Exception as exc:
                    attempt = int(event.get('attempt', 0))
                    if attempt >= settings.max_event_retries:
                        await broker.publish(settings.kafka_dlq_topic, {**event, 'error': str(exc)})
                    else:
                        retry = dict(event)
                        retry['attempt'] = attempt + 1
                        await broker.publish(settings.kafka_topic, retry)
    return dlq
