FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY pyproject.toml README.md ./
COPY gateway ./gateway
COPY services ./services
COPY shared ./shared
COPY docs ./docs
RUN pip install --no-cache-dir .

CMD ["uvicorn", "gateway.app:app", "--host", "0.0.0.0", "--port", "8000"]
