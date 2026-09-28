import asyncio

from tests.conftest import drain_events


def test_idempotent_payment_happy_path(client, auth_headers):
    source = client.post('/accounts', json={'owner_id': 'alice', 'currency': 'USD', 'balance': '100.00'}, headers=auth_headers).json()
    destination = client.post('/accounts', json={'owner_id': 'bob', 'currency': 'USD', 'balance': '50.00'}, headers=auth_headers).json()

    payload = {
        'source_account_id': source['id'],
        'destination_account_id': destination['id'],
        'amount': '20.00',
        'currency': 'USD',
        'metadata': {'order_id': 'ord-1'},
    }
    headers = {**auth_headers, 'Idempotency-Key': 'pay-123', 'X-Correlation-ID': 'corr-123'}
    first = client.post('/payments', json=payload, headers=headers)
    second = client.post('/payments', json=payload, headers=headers)

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()['id'] == second.json()['id']
    assert first.json()['state'] == 'CREATED'

    asyncio.run(drain_events())

    payment = client.get(f"/payments/{first.json()['id']}", headers=auth_headers).json()
    source_after = client.get(f"/accounts/{source['id']}", headers=auth_headers).json()
    destination_after = client.get(f"/accounts/{destination['id']}", headers=auth_headers).json()
    audit = client.get(f"/payments/{first.json()['id']}/audit", headers=auth_headers).json()

    assert payment['state'] == 'COMPLETED'
    assert source_after['balance'] == '80.00'
    assert source_after['reserved_balance'] == '0.00'
    assert destination_after['balance'] == '70.00'
    assert any(entry['state_to'] == 'COMPLETED' for entry in audit)


def test_idempotency_key_reuse_with_different_payload_is_rejected(client, auth_headers):
    source = client.post('/accounts', json={'owner_id': 'alice', 'currency': 'USD', 'balance': '100.00'}, headers=auth_headers).json()
    destination = client.post('/accounts', json={'owner_id': 'bob', 'currency': 'USD', 'balance': '50.00'}, headers=auth_headers).json()

    headers = {**auth_headers, 'Idempotency-Key': 'pay-conflict'}
    first = client.post('/payments', json={
        'source_account_id': source['id'],
        'destination_account_id': destination['id'],
        'amount': '20.00',
        'currency': 'USD',
        'metadata': {},
    }, headers=headers)
    second = client.post('/payments', json={
        'source_account_id': source['id'],
        'destination_account_id': destination['id'],
        'amount': '21.00',
        'currency': 'USD',
        'metadata': {},
    }, headers=headers)

    assert first.status_code == 202
    assert second.status_code == 409
    assert second.json()['detail'] == 'idempotency key reuse with different payload'
