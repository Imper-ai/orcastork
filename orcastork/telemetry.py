"""The framework's OpenTelemetry instrumentation handles — OTel **is** the telemetry standard here.

OpenTelemetry is already the multi-backend abstraction (providers/exporters decide where the
data goes), so the framework instruments straight against the OTel API instead of wrapping it
in a port. Without an SDK wired, every API call is a no-op — a deployment that configures
nothing loses nothing, exactly as before. A deployment that wants the data configures
providers through ``opentelemetry-sdk`` (globally, or injected here explicitly) and **must
use batching exporters** (``BatchSpanProcessor``, ``PeriodicExportingMetricReader``,
``BatchLogRecordProcessor``): the orchestrator emits inline on its single gathering loop, so
nothing may await, lock or perform I/O on the calling path — the batching processors buffer
locally and export off-thread, which is what keeps a telemetry outage an observability
problem and never a correctness one.

:class:`Telemetry` bundles the tracer, the OTel logger and every metric instrument the
framework emits, created once up front (re-creating instruments per emission would make the
SDK warn about duplicates). It rides the runtime like every other injected dependency, so
tests pin providers with in-memory exporters while production simply defaults to the
process globals.

**Metric label cardinality rule:** metric attributes must stay LOW-cardinality — values
drawn from small, closed sets such as ``operator_id``, ``capability_id``, ``outcome``,
``kind`` or ``disposition``. Per-session identifiers (``session_id``, ``namespace_id``)
are NOT metric attributes: one time series per session would explode the backend's series
count. Span and log attributes are the opposite case — each record stands alone, so
per-session identifiers belong there; they are what "find this session's trace" is built from.
"""

from __future__ import annotations

from opentelemetry import metrics, trace
from opentelemetry._logs import Logger, LoggerProvider, get_logger
from opentelemetry.metrics import MeterProvider
from opentelemetry.trace import Tracer, TracerProvider

_INSTRUMENTATION_NAME = 'orcastork'


class Telemetry:
    """Tracer + logger + the framework's metric instruments, resolved once from the providers.

    ``None`` providers resolve to the process globals (the OTel default), which are inert
    no-ops until a deployment installs an SDK — so a bare ``Telemetry()`` is always safe.
    """

    def __init__(
        self,
        *,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        logger_provider: LoggerProvider | None = None,
    ) -> None:
        self.tracer: Tracer = trace.get_tracer(_INSTRUMENTATION_NAME, tracer_provider=tracer_provider)
        self.logger: Logger = get_logger(_INSTRUMENTATION_NAME, logger_provider=logger_provider)
        meter = metrics.get_meter(_INSTRUMENTATION_NAME, meter_provider=meter_provider)
        self.sessions_total = meter.create_counter('sessions_total')
        self.session_deadline_hits_total = meter.create_counter('session_deadline_hits_total')
        self.operator_runs_total = meter.create_counter('operator_runs_total')
        self.operator_retries_total = meter.create_counter('operator_retries_total')
        self.operator_reruns_total = meter.create_counter('operator_reruns_total')
        self.data_points_merged_total = meter.create_counter('data_points_merged_total')
        self.inbox_entries_total = meter.create_counter('inbox_entries_total')
        self.capability_activations_total = meter.create_counter('capability_activations_total')
        self.aggregator_dead_letters_total = meter.create_counter('aggregator_dead_letters_total')
        self.operator_run_seconds = meter.create_histogram('operator_run_seconds', unit='s')
        self.session_gather_seconds = meter.create_histogram('session_gather_seconds', unit='s')
        self.session_aggregation_seconds = meter.create_histogram('session_aggregation_seconds', unit='s')
        self.archive_flush_entries = meter.create_histogram('archive_flush_entries')
