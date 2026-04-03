import logging
import sys
from typing import Any

import structlog


def configure_logging(level: str) -> None:
    """Configure stdlib + structlog for structured JSON output.

    Processor chain:
    1. merge_contextvars  — pulls in any request-scoped bindings (e.g. request_id)
    2. add_log_level      — injects the log level string
    3. TimeStamper        — adds ISO-8601 timestamp
    4. StackInfoRenderer  — renders stack info when present
    5. format_exc_info    — renders exception tracebacks
    6. JSONRenderer       — serialises everything to a JSON line
    """
    logging.basicConfig(level=level, stream=sys.stdout, format="%(message)s")

    structlog.configure(
        processors=[
            # Must be first so context-var bindings (request_id, etc.) are merged
            # before any other processor sees the event dict.
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level.upper())),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> Any:
    """Return a bound structlog logger for *name*.

    Prefer this over calling ``structlog.get_logger`` directly so all modules
    go through a single, consistent entry point.

    Usage::

        from app.core.logging import get_logger
        log = get_logger(__name__)
        log.info("something_happened", key="value")
    """
    return structlog.get_logger(name)

