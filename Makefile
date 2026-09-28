PYTHON ?= python3
PIP ?= pip

install:
	$(PIP) install -e .[dev]

test:
	pytest

run-gateway:
	uvicorn gateway.app:app --reload --host 0.0.0.0 --port 8000

run-payments:
	uvicorn services.payments.app:app --reload --host 0.0.0.0 --port 8001

run-accounts:
	uvicorn services.accounts.app:app --reload --host 0.0.0.0 --port 8002

run-fraud:
	uvicorn services.fraud.app:app --reload --host 0.0.0.0 --port 8003

run-ledger:
	uvicorn services.ledger.app:app --reload --host 0.0.0.0 --port 8004

run-notifications:
	uvicorn services.notifications.app:app --reload --host 0.0.0.0 --port 8005

up:
	docker compose up --build

down:
	docker compose down -v
