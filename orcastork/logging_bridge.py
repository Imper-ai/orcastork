"""Loguru → OpenTelemetry log bridge — ships the framework's existing logs as OTel log records.

The framework logs through ``loguru`` (human-readable, locally formatted). A deployment that
wires an OTel logs pipeline usually wants those same records as backend log entries,
correlated with the spans the framework emits — without a second logging call next to every
``logger.*`` call site. The bridge is that single seam: one loguru sink that emits each
record through the OTel logs API, with the structured kwargs as attributes, exception info
mapped onto the conventional ``exception.*`` attribute keys, and the active span's
trace/span ids stamped on by the API itself.

Process-global by design (loguru's logger is process-global): attach **once** at deployment
wiring time, next to provider setup — never per runtime or per session, or records would be
forwarded once per attachment. The sink runs inline on the logging call, so it only does
sync, non-blocking work; export happens off-thread in the deployment's batching processor.
"""

from __future__ import annotations

import traceback
from typing import Any

from loguru import logger
from opentelemetry._logs import LoggerProvider, SeverityNumber, get_logger

_INSTRUMENTATION_NAME = 'orcastork'

_SEVERITY_NUMBERS = {
    'TRACE': SeverityNumber.TRACE,
    'DEBUG': SeverityNumber.DEBUG,
    'INFO': SeverityNumber.INFO,
    'SUCCESS': SeverityNumber.INFO,
    'WARNING': SeverityNumber.WARN,
    'ERROR': SeverityNumber.ERROR,
    'CRITICAL': SeverityNumber.FATAL,
}

# OTel attribute values are scalars (or sequences thereof); anything richer that rides a
# loguru kwarg (a list of operator ids, a mapping) is stringified rather than dropped.
_SCALARS = (str, bool, int, float)


def attach_otel_log_bridge(
    *, level: str = 'INFO', module_filter: str | None = None, logger_provider: LoggerProvider | None = None
) -> int:
    """Attach the bridge; returns the loguru sink id (pass it to :func:`detach_otel_log_bridge`).

    ``level`` is the minimum loguru level forwarded. ``module_filter`` (a module-path prefix,
    e.g. ``'orcastork'``) restricts forwarding to that module tree — for a host
    application that already ships its own logs elsewhere and only wants the framework's.
    ``logger_provider`` defaults to the process global.
    """
    otel_logger = get_logger(_INSTRUMENTATION_NAME, logger_provider=logger_provider)

    def forward(message: Any) -> None:
        record = message.record
        attributes: dict[str, Any] = {'logger.name': record['name'] or ''}
        for key, value in record['extra'].items():
            attributes[key] = value if isinstance(value, _SCALARS) else repr(value)
        exception = record['exception']
        if exception is not None and exception.value is not None:
            attributes['exception.type'] = type(exception.value).__name__
            attributes['exception.message'] = str(exception.value)
            attributes['exception.stacktrace'] = ''.join(traceback.format_exception(exception.value))
        otel_logger.emit(
            severity_number=_SEVERITY_NUMBERS.get(record['level'].name, SeverityNumber.INFO),
            severity_text=record['level'].name,
            body=record['message'],
            attributes=attributes,
        )

    return logger.add(forward, level=level, filter=module_filter)


def detach_otel_log_bridge(bridge_id: int) -> None:
    """Remove a previously attached bridge."""
    logger.remove(bridge_id)
