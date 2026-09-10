# orcastork

Notes for anyone — human or agent — changing this codebase. The user-facing explanation of
what the framework is and how to build on it lives in [README.md](README.md); this file is
about *working on the framework itself*.

Reusable, standalone dataflow orchestration library: `Operator`s consume/produce
`DataPoint`s, `Capability`s provide actions, `Aggregator`s write durable outputs, and a
session-scoped `Orchestrator` (supervised by a `SessionOrchestrationManager`) schedules by
**data readiness**. Not a running service — a library other applications embed.

## Shape of the codebase

- **Library, not a service** — no HTTP entrypoint; imported and run in-process by the
  embedding application's own worker/event loop.
- Session-scoped: all state (locks, watermarks, mirror, inbox) is keyed by `session_id`; ports have
  swappable backends under `adapters/{memory,redis,mongo}/` (locks/inbox on Redis, durable output
  and archive on Mongo, keyed by the output model's `__table_name__`).
- Flow: an `Orchestrator` per session schedules registered `Operator`s as their input `DataPoint`s
  become ready, routes side effects through `Capability`s, and lets `Aggregator`s write curated
  durable output plus continuous live-archiving — i.e. it provides the "run this dataflow graph,
  resumably, per session" abstraction.

## Where to make which change

- **New DataPoint / Operator / Capability** — subclass and set the registration key;
  `__init_subclass__` registers it. Duplicate keys raise. Import the module at startup so
  registration actually runs.
- **New scheduling behavior** (retry, idle wait, completion condition) — `scheduling/`. Retries
  are loop-scheduled backoff relaunches, never in-task sleeps; idle `completes_when` waits
  **park** (`PARKED`, not finalized) after `park_after`; completion conditions are a declarative
  AST in `scheduling/completion.py` — no callables.
- **New backend for a port** (storage, rate limiter, etc.) — add it under
  `adapters/{memory,redis,mongo}/`, never in core modules. Core depends only on `ports/`
  Protocols + `ids`/`clock`, injected via `runtime.py`.
- **Non-idempotent side effect inside an Operator** — go through the claim/commit/revert context
  manager `async with ctx.once(key)` (`operators/effects.py`); never fire-and-forget.
- **Anything touching wall-clock time** — use the injected `Clock` protocol (`clock.py`); never
  call `datetime.now()` / `asyncio.sleep` in core logic, no exceptions. Tests drive `FakeClock`.
- **Anything that mutates session state** — must go through the orchestrator's sole-mutator path:
  reads from the local `SessionStateMirror` (`orchestrator/mirror.py`), writes via
  `DataPointStore.apply_resolved`. Never add a second store writer.
- **Durable output routing** — both write paths (`DurableStore` curated writes from aggregators
  at finalize, `DataPointArchive` live-archiving from the orchestrator at the sole-mutator merge
  point) route by the model's `__table_name__` ClassVar, never a hardcoded collection name.
  Archive writes are keyed-upsert, epoch-fenced, and go off the hot path via a write-behind
  buffer (`append_many`/`archive_many`). Audit appends are durable where `replay` reads them,
  so the audit has no buffer to drain.
- **Telemetry** — instrument straight against `opentelemetry-api` in `telemetry.py` (OTel is the
  abstraction; it's a core dep, not a port). `logging_bridge.py` ships loguru records as OTel log
  records, attached once per process. Deployments must wire batching exporters — nothing may
  block the gathering loop.

## Conventions

- **The declared dependencies are the whole dependency list, enforced.** Nothing in the package
  imports anything `pyproject.toml` does not declare, and the optional backends (`redis`,
  `pymongo`) only appear under `adapters/`. `tests/test_harness.py` reads the allowed set out of
  `pyproject.toml` and fails on both — don't route around it.
- **Ports & adapters.** Every mutating port method takes an `epoch`; stale-epoch writes are
  rejected everywhere (effect marks, session meta, deadline, flow fingerprint). `SessionLock` is
  a liveness hint only — the epoch is what makes it correct. `RateLimiter` defaults to
  `NullRateLimiter` (a no-op) so a deployment that wires nothing loses nothing.
- **PII** is sealed through an injected `ValueCipher` (default `NullCipher`); the framework ships
  no cryptography itself — the embedding flow wires real encryption.
- **The completion tail is BOUNDED, not renewed.** The loop's renews stop when gathering returns, so
  `aggregate → flush → inbox re-check → mark_complete` holds the epoch with nothing renewing it, and the
  lock's TTL is specified to exceed the longest single unrenewed await. A slow durable write in the tail
  used to blow that promise silently: the lease lapsed under a live owner, the manager legitimately read
  the session as orphaned (`is_orphaned` = started, not complete, **lock not held**), and resumed it at a
  higher epoch mid-finalize. Every tail step now runs through `_bounded_tail_step` under
  `operation_timeout` and raises `CompletionTailTimeoutError` — the run stops, the epoch is released, and
  a successor re-drives from durable state. Cancelling a half-done flush is safe: the archive's
  write-behind buffer is durable and replayed on resume. **Do not "fix" this with a
  background keepalive** — renewing from a task that is not the one doing the work turns the lease from
  "this session is progressing" into "this process still has an event loop", so a wedged pod looks healthy
  and recovery is delayed rather than avoided. That was tried and reverted.
- **`final` is terminal for an aggregator's durable output.** `AggregationHelpers.upsert` refuses a
  non-final write over an already-final record. An `interim_refresh` aggregator rejoins the gather set on
  a resumed/reopened epoch, while `_aggregate` skips its finalize once the contribution is marked — so
  without that guard the interim run walks `final` back to `in_progress` and nothing is left to restore
  it. Consumers wait for `final`, so the record reads as empty forever.
- **A coalescing window is never charged to the completion tail.** An armed debounce window normally
  holds the gathering loop open until it comes due — that is what makes the coalescing real rather
  than advisory. The sole exemption (`scheduling/debounce.py`, `window_defers_to_finalize`) is an
  `interim_refresh` aggregator's window once the completion condition is already satisfied: the loop
  is then one step from the aggregation phase, whose finalize rewrites that same document, so waiting
  the window out bought a second `in_progress` write that `final` overwrote in the same instant — the
  same durable writes as no window at all, plus the window's full width on every session that
  completes. **Do not make the exemption unconditional.** Under an *unsatisfied* condition the
  session is instead heading for an inbox wait of unbounded length, where the interim write is the
  only live view a reader gets, so the window is waited out (once) before going idle. Retry windows
  never defer either — a retry's window IS its backoff. The completion verdict is evaluated once per
  gather pass and shared with `_plan`: abandoning a window and breaking to aggregation must be
  decided against the same view, or a refold is dropped by a pass that then waits instead.
- **Manager API is flow-based** — `start_session`/`resume`/`deliver` take a `FlowDefinition`
  (`flow.py`); its fingerprint backs resume drift detection.

## `orcastork_lite/` — the scheduling-only sibling

A second, independent package in the same distribution (`pyproject.toml` `[tool.poetry].packages`).
It keeps the scheduler (readiness, debounced reruns, `rerun_on`, `uses`, `consumes` pruning, retry
backoff, timeouts, `max_cycles`) and the dependency injection (capabilities, `CapabilityCatalog`
with per-namespace operator/capability gating, `Runtime(clock, catalog)`) and **nothing else**: no
store port, epochs, locks, inbox, parking, aggregators, durable store, audit, archive, telemetry or
`ctx.once`. Session state is an in-memory `SessionState`; `Orchestrator.run()` returns the final
DataPoints. Rules that carry over unchanged: injected `Clock` only, loop-scheduled windows (never an
in-task sleep), sole-writer loop, fault isolation per operator, house style, `tests/test_harness.py`.
Rules that do not: there are no registries (classes are handed to the orchestrator; duplicates raise
there) and DataPoints have no `type` Literal or `config` — the class is the type. Do not import
`orcastork` from it or vice versa, and do not let the removed features creep back in: the point of
the package is what it lacks. `orcastork_lite/tools/` (the `orcastork-lite-graph` CLI) is the one
subpackage the library must never import; a test enforces it.

## Build & test

- `make lint` — `poetry check` + ruff + ruff format --check + mypy.
- `make test` — `poetry run pytest` (in-memory adapters only, no Docker).
- Integration tests are marked `@pytest.mark.integration` and excluded from the default run:
  `poetry run pytest -m integration` (needs Docker). **Only Mongo has a real backend** — the
  `real_mongo_database` fixture, via `pytest-mock-resources`. The `integration`-marked Redis tests
  still run on fakeredis, so the marker there means "slow/edge-case", not "real server".
- Reach for `real_mongo_database` only where the SERVER is the subject (an aggregation stage, how a
  batch of upserts resolves, an index being used) and stay on `mongo_database` otherwise — the
  double needs no Docker and costs milliseconds. It emulates what it has grown and cannot tell you
  which of those you are relying on: its `$setOnInsert`/`$max` matches a real server (measured),
  while a `bulk_write` of `UpdateOne` only works because `tests/conftest.py` drops the `sort=None`
  pymongo always sends, and `$unionWith` is absent entirely.
- `make deps` → `poetry install --all-extras --with dev`; `--all-extras` is what puts the redis
  and mongo backends in front of mypy and the adapter tests.
- `orcastork-graph` (console script, `tools/graph.py`) runs deploy-time graph checks + emits Mermaid.
- Requires Python 3.13+.

## House style

- Line length 119, single quotes, 4-space indent — `make fix_lint` settles all three.
- Type hints on every function, parameters **and** return, `-> None` included; an unannotated
  function is invisible to mypy.
- Prefer `match` over an if/elif chain when branching on an enum or a status.
- No tuple return types in new code — return a small pydantic model, dataclass or `NamedTuple`
  so the call site reads and refactors safely.
- Imports at module level, always: stdlib, third-party, then first-party.
- Custom exceptions from `exceptions.py` for every known error case; never a bare `Exception`.
- `logger.*` takes kwargs, never an f-string: `logger.info('Session started', session_id=sid)`.
- Comments explain **why**, not what, and stay timeless — no ticket numbers, no "fixes the bug
  above", nothing that stops making sense once the surrounding code moves.
- Tests are deterministic: no absolute dates, no local clock or timezone, no `sleep`, no
  dependence on test order. Drive time through `FakeClock`.
- Registry tests must isolate registries — use the autouse fixture in `tests/conftest.py` so one
  test's registrations don't leak into another.
