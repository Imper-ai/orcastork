"""OpenTelemetry test probe — per-test providers wired to synchronous in-memory exporters.

A fresh probe per test keeps telemetry assertions isolated without touching OTel's
process-global providers (which can only be installed once per process). ``Simple*``
processors export synchronously — exactly what deterministic assertions need here, and
exactly what a production deployment must NOT wire (it uses the batching processors).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter, SimpleLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from orcastork.telemetry import Telemetry


class TelemetryProbe:
    def __init__(self) -> None:
        self.span_exporter = InMemorySpanExporter()
        tracer_provider = TracerProvider()
        tracer_provider.add_span_processor(SimpleSpanProcessor(self.span_exporter))
        self.metric_reader = InMemoryMetricReader()
        self.log_exporter = InMemoryLogRecordExporter()
        self.logger_provider = LoggerProvider()
        self.logger_provider.add_log_record_processor(SimpleLogRecordProcessor(self.log_exporter))
        self.telemetry = Telemetry(
            tracer_provider=tracer_provider,
            meter_provider=MeterProvider(metric_readers=[self.metric_reader]),
            logger_provider=self.logger_provider,
        )

    def metric_points(self, name: str) -> list[Any]:
        """Every data point of metric ``name`` from a fresh collection pass."""
        data = self.metric_reader.get_metrics_data()
        if data is None:
            return []
        return [
            point
            for resource_metrics in data.resource_metrics
            for scope_metrics in resource_metrics.scope_metrics
            for metric in scope_metrics.metrics
            if metric.name == name
            for point in metric.data.data_points
        ]

    def counter(self, name: str, attributes: Mapping[str, str] | None = None) -> int:
        """The aggregated count for one exact series (``0`` if never incremented)."""
        for point in self.metric_points(name):
            if dict(point.attributes) == dict(attributes or {}):
                return int(point.value)
        return 0

    def counter_total(self, name: str) -> int:
        """The count summed across every attribute combination of ``name``."""
        return sum(int(point.value) for point in self.metric_points(name))

    def histogram(self, name: str, attributes: Mapping[str, str] | None = None) -> Any | None:
        """The histogram data point (``.count``/``.sum``/...) for one exact series, or ``None``."""
        for point in self.metric_points(name):
            if dict(point.attributes) == dict(attributes or {}):
                return point
        return None

    def metric_attribute_keys(self) -> frozenset[str]:
        """Every metric attribute key ever emitted, across all metrics (cardinality-rule assertions)."""
        data = self.metric_reader.get_metrics_data()
        if data is None:
            return frozenset()
        return frozenset(
            key
            for resource_metrics in data.resource_metrics
            for scope_metrics in resource_metrics.scope_metrics
            for metric in scope_metrics.metrics
            for point in metric.data.data_points
            for key in (point.attributes or {})
        )

    def spans(self, name: str | None = None) -> tuple[ReadableSpan, ...]:
        """Finished spans in START order (the exporter holds them in end order), optionally by name."""
        finished = sorted(self.span_exporter.get_finished_spans(), key=lambda span: span.start_time or 0)
        return tuple(span for span in finished if name is None or span.name == name)

    def parent_of(self, span: ReadableSpan) -> ReadableSpan | None:
        """The finished span that is ``span``'s parent, or ``None`` for a root span."""
        if span.parent is None:
            return None
        for candidate in self.span_exporter.get_finished_spans():
            if candidate.context is not None and candidate.context.span_id == span.parent.span_id:
                return candidate
        return None

    def logs(self) -> tuple[Any, ...]:
        """Every emitted OTel log record, in emission order."""
        return tuple(log.log_record for log in self.log_exporter.get_finished_logs())
