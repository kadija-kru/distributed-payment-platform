import asyncio

from sqlalchemy import func, select

from tests.conftest import drain_events


def test_fraud_rejection_compensates_and_fails(client, auth_headers):
    source = client.post('/accounts', json={'owner_id': 'alice', 'currency': 'USD', 'balance': '100.00'}, headers=auth_headers).json()
    destination = client.post('/accounts', json={'owner_id': 'bob', 'currency': 'USD', 'balance': '25.00'}, headers=auth_headers).json()
    payload = {
        'source_account_id': source['id'],
        'destination_account_id': destination['id'],
        'amount': '15.00',
        'currency': 'USD',
        'metadata': {'force_fraud_reject': True},
    }
    response = client.post('/payments', json=payload, headers={**auth_headers, 'Idempotency-Key': 'pay-fraud'})
    asyncio.run(drain_events())

    payment = client.get(f"/payments/{response.json()['id']}", headers=auth_headers).json()
    source_after = client.get(f"/accounts/{source['id']}", headers=auth_headers).json()
    destination_after = client.get(f"/accounts/{destination['id']}", headers=auth_headers).json()

    assert payment['state'] == 'FAILED'
    assert payment['failure_reason'] == 'fraud rejected'
    assert source_after['balance'] == '100.00'
    assert source_after['reserved_balance'] == '0.00'
    assert destination_after['balance'] == '25.00'


def test_duplicate_event_protection_prevents_double_reservation(client, auth_headers):
    from shared.db import session_scope
    from shared.domain import process_accounts_event
    from shared.models import Account, Payment, ProcessedEvent

    source = client.post('/accounts', json={'owner_id': 'alice', 'currency': 'USD', 'balance': '100.00'}, headers=auth_headers).json()
    destination = client.post('/accounts', json={'owner_id': 'bob', 'currency': 'USD', 'balance': '25.00'}, headers=auth_headers).json()
    response = client.post(
        '/payments',
        json={'source_account_id': source['id'], 'destination_account_id': destination['id'], 'amount': '10.00', 'currency': 'USD', 'metadata': {}},
        headers={**auth_headers, 'Idempotency-Key': 'dup-key'},
    )
    payment_id = response.json()['id']
    event = {
        'event_id': 'evt-dup-1',
        'event_type': 'payment.created',
        'payment_id': payment_id,
        'correlation_id': 'corr-dup',
        'attempt': 0,
        'payload': {'payment_id': payment_id},
    }

    async def run_twice():
        async with session_scope() as session:
            await process_accounts_event(session, event)
        async with session_scope() as session:
            await process_accounts_event(session, event)
        async with session_scope() as session:
            account = await session.get(Account, source['id'])
            count = (await session.execute(select(func.count()).select_from(ProcessedEvent).where(ProcessedEvent.event_id == 'evt-dup-1', ProcessedEvent.service_name == 'accounts'))).scalar_one()
            payment = await session.get(Payment, payment_id)
            return str(account.reserved_balance), count, payment.state.value

    reserved_balance, processed_count, state = asyncio.run(run_twice())
    assert reserved_balance == '10.00'
    assert processed_count == 1
    assert state == 'CREATED'


def test_ledger_failure_retries_and_hits_dlq(client, auth_headers):
    source = client.post('/accounts', json={'owner_id': 'alice', 'currency': 'USD', 'balance': '100.00'}, headers=auth_headers).json()
    destination = client.post('/accounts', json={'owner_id': 'bob', 'currency': 'USD', 'balance': '25.00'}, headers=auth_headers).json()
    response = client.post(
        '/payments',
        json={'source_account_id': source['id'], 'destination_account_id': destination['id'], 'amount': '10.00', 'currency': 'USD', 'metadata': {'force_ledger_failure': True}},
        headers={**auth_headers, 'Idempotency-Key': 'dlq-key'},
    )
    dlq = asyncio.run(drain_events())
    payment = client.get(f"/payments/{response.json()['id']}", headers=auth_headers).json()

    assert payment['state'] == 'FRAUD_APPROVED'
    assert dlq
    assert dlq[-1]['event_type'] == 'payment.fraud_approved'
    assert 'simulated ledger failure' in dlq[-1]['error']
