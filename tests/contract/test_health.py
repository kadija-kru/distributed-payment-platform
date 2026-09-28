from fastapi.testclient import TestClient


def test_gateway_health(client):
    response = client.get('/health')
    assert response.status_code == 200
    assert response.json()['service'] == 'gateway'


def test_service_health_endpoints(tmp_path):
    from tests.conftest import _reset_runtime
    _reset_runtime(tmp_path)
    for module_name, expected in [
        ('services.accounts.app', 'accounts'),
        ('services.payments.app', 'payments'),
        ('services.fraud.app', 'fraud'),
        ('services.ledger.app', 'ledger'),
        ('services.notifications.app', 'notifications'),
    ]:
        module = __import__(module_name, fromlist=['app'])
        with TestClient(module.app) as service_client:
            response = service_client.get('/health')
            assert response.status_code == 200
            assert response.json()['service'] == expected
