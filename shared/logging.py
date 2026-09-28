import contextvars
import json
import logging
import sys
from typing import Any

correlation_id_var = contextvars.ContextVar("correlation_id", default="system")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": correlation_id_var.get(),
        }
        if hasattr(record, "service"):
            payload["service"] = record.service
        return json.dumps(payload)


_configured = False


def configure_logging(service_name: str) -> None:
    global _configured
    if _configured:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    logging.getLogger(service_name).info("logging-configured", extra={"service": service_name})
    _configured = True
