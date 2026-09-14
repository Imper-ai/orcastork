# orcastork_lite

The scheduling core of [orcastork](../README.md), and nothing else.

`Operator`s consume and produce `DataPoint`s, `Capability`s are injected action providers, and a
session-scoped `Orchestrator` schedules by **data readiness**: an operator runs when its inputs
exist, reruns (debounced) when relevant new data lands, retries on a backoff window if its policy
asks for it, and is timeout-bounded and fault-isolated throughout. When nothing can run any more
the session is complete and the gathered DataPoints are returned.

It deliberately has **no** durability, resumability, fencing epochs, locks, inbox, parking,
aggregators, durable store, audit trail, archive, telemetry or `ctx.once`. A session lives and dies
inside one process, bounded by a session deadline; the only thing that leaves the process while it
runs is a stream of **session events** (optionally onto a Redis stream) so a consumer can react to a
DataPoint the moment it lands. If you need any of the rest, use `orcastork`.

## Quickstart

```python
import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timezone

from orcastork_lite import (
    DataPoint, DataPointEmission, NamespaceId, Operator, OperatorContext, OperatorId,
    OperatorPolicy, Orchestrator, SessionId, build_runtime,
)


# 1. The data. Identity is (class, value), so re-observing a value merges instead of duplicating.
class Url(DataPoint[str]): ...
class Title(DataPoint[str]): ...


# 2. The work. It runs because a Url exists, not because anything called it.
class TitleFetcher(Operator):
    operator_id = OperatorId('title_fetcher')
    policy = OperatorPolicy(rerun_on_new_data=True)
    depends_on = frozenset({Url})
    produces = frozenset({Title})

    async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
        for url in ctx.delta.added:                      # incremental: only what is new since the last run
            if isinstance(url, Url):
                yield Title.emit(f'Title of {url.value}')


async def main() -> None:
    now = datetime.now(timezone.utc)
    seed = [Url(value='https://example.com', retrieved_by=OperatorId('seed'), first_retrieved=now, last_retrieved=now)]
    result = await Orchestrator(
        session_id=SessionId('run-1'),
        namespace_id=NamespaceId('default'),
        runtime=build_runtime(),
        operators=[TitleFetcher],
        seed=seed,
    ).run()
    print(result.operator_runs)                          # {'title_fetcher': 1}
    print([t.value for t in result.data_points.of_type(Title)])  # ['Title of https://example.com']


asyncio.run(main())
```

Nothing declared an order. Add a second operator that depends on `Title` and it schedules itself.

## What you write

| You write | Declares | Implements |
|---|---|---|
| `DataPoint[V]` subclass | nothing — the class is the type; any subclass may be used as an abstract dependency | — |
| `Operator` subclass | `operator_id`, `policy`, `depends_on`, `uses`, `produces`, `requires`, `consumes` | `async def run(ctx) -> AsyncIterator[DataPointEmission]` (yield `Leaf.emit(value)`) |
| `Capability` subclass | `capability_id`, `depends_on`, `requires` | `async def activate(ctx)` (build a client from `ctx.credentials`; `ctx.namespace_id` lets you cache one per tenant) plus your own typed action methods |

Operators and capabilities are passed to the orchestrator as **classes**; a fresh no-argument
instance is constructed per run. There are no registries: two classes with the same id in one
orchestrator raise `DuplicateIdError`.

### Scheduling knobs (`OperatorPolicy`)

- `rerun_on_new_data` (no default): rerun when new data of a `depends_on`/`uses` type arrives, or a
  capability becomes available. `rerun_on=RerunOn.ADDED_ONLY` ignores freshness-only re-observations.
- `debounce=timedelta(...)`: arrivals inside the window collapse into one rerun. The window is
  waited out on the injected clock before the session may complete.
- `timeout=timedelta(...)`: per-operator override of the orchestrator's `operation_timeout`.
- `retry=RetryPolicy(max_attempts, base_delay, jitter)`: a failed run is relaunched on a jittered
  exponential backoff window with the **same delta**; the budget exhausted, the failure is terminal
  and reported in `result.failures`.
- `max_cycles=N`: required for an operator on a dependency cycle (it bounds the loop); an unbounded
  cycle is rejected at construction with `UnboundedCycleError`.

**Session-wide bounds** live on the `Orchestrator`: `operation_timeout` (default 30s) caps one
operator run and one capability activation, and `session_deadline` (default 300s, `None` for
unbounded) caps the whole session on the injected clock. The deadline is checked between passes and
clips every window the loop waits out; when it passes, in-flight operators are cancelled, their
already-streamed emissions are kept, and each is reported in `result.failures`; `result.deadline_hit`
says it happened. A run in flight can overshoot by at most its own timeout.

`uses` types re-trigger a rerun but never gate readiness. `consumes` marks the sink types a flow
exists to produce: when any operator declares it, operators whose output cannot reach a sink are
pruned before the session starts.

### Dependency injection

- **Capabilities** are the injection point for external actions. An operator declares
  `requires={SomeCapability}` and calls `ctx.capabilities.require(SomeCapability)`; it is scheduled
  only once the capability is available, and the framework builds the instance lazily from the
  catalog's credentials the first moment it becomes available. Several providers of one abstract
  capability resolve by the namespace's `preferred_order`, alphabetical tie-break.
- **`CapabilityCatalog`** (a `Protocol`) says, per namespace, which capabilities and operators may
  run (`permitted_operators` returning `None` means unrestricted; an empty set means run nothing),
  which credentials each capability gets, and the provider preference. `InMemoryCapabilityCatalog`
  is the shipped implementation; a deployment writes its own over its configuration store.
- **`Runtime(clock, catalog, events)`** is the whole injected bundle. Tests pass a fake `Clock`, so
  debounce windows and retry backoff run against controlled time.

### Following a session while it runs

Session state is private to the orchestrator, so the loop publishes every change to the runtime's
`SessionEventSink`: a `DataPointMerged` per DataPoint added or freshened (with its value, provenance
and the session revision), an `OperatorRunCompleted` per finished run (`succeeded`, `failed`,
`retrying`, `cancelled`), a `CapabilityActivated` per activation outcome, and one `SessionCompleted`.
The default sink drops them. `orcastork_lite.adapters.memory.InMemorySessionEventSink` keeps them in a
list; `orcastork_lite.adapters.redis.RedisSessionEventSink` appends each to a Redis stream per session
(`orcastork_lite:events:<session_id>`, field `kind` plus the event as JSON, capped by `maxlen`):

```python
from redis.asyncio import Redis
from orcastork_lite import build_runtime
from orcastork_lite.adapters.redis import RedisSessionEventSink

runtime = build_runtime(events=RedisSessionEventSink(Redis.from_url('redis://localhost')))
# a consumer: XREAD BLOCK 0 STREAMS orcastork_lite:events:run-1 $
```

A sink that raises is logged and the event dropped; publishing never stops the session. Capability
activation is bounded by `operation_timeout` for the same reason: it runs on the gathering loop, and
a hung client build would otherwise stall every operator, not only the ones that need it.

## Graph tool (separate)

`orcastork-lite-graph` lives in `orcastork_lite.tools` — the library never imports it. It imports the
flow modules you name, collects every concrete `Operator` / `Capability` class those modules expose,
validates the bounded-cycle rule and renders the graph as Mermaid:

```bash
orcastork-lite-graph -m myflow.operators -m myflow.capabilities --check --mermaid graph.mmd
orcastork-lite-graph -m myflow.operators --permitted breach_intel,idp   # the per-namespace subgraph
```
