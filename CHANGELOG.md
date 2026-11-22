# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). While the
version is below 1.0, minor releases may contain breaking changes; they will always be listed
under **Changed** with the migration.

## [Unreleased]

## [0.2.0]

Fixes for sessions that live for minutes to hours, across pods and deploys. It breaks backwards
compatibility with 0.1.1 where marked **Breaking**; nothing depended on 0.1.1, so there is no
migration path.

### Changed

- **Breaking: Redis state written by 0.1.1 is not carried over.** A dict or set value's identity
  now sorts by `repr`, which changes the Redis field name of some stored DataPoints, and the inbox's
  pending count is the stream length, no longer reduced by the `acked:` counter 0.1.1 kept. Start
  0.2.0 on a Redis with no 0.1.1 sessions in it.
- **Breaking: `start_session` on an existing session no longer starts another run.** A completed session is
  returned as it is (`COMPLETED`, its current epoch, no operator runs), a live one is returned as
  the new `SessionStatus.RUNNING` instead of raising `LockHeldError`, and a parked or orphaned one
  is resumed (without the seed, which it already holds). A session with no state is new, as before.
  A caller that relied on `LockHeldError`, or on re-running a completed session through
  `start_session`, checks the returned status instead, or calls `reopen` for a completed session.
- **The lease is renewed while operators run.** The gathering loop renewed only between passes, so
  an operator that outlived the lock TTL lost its session to another pod, which re-ran it while the
  first was still running. A renewal that finds a higher epoch still stops the run as `SUPERSEDED`.
  The lock TTL no longer has to exceed the longest `OperatorPolicy.timeout`, only `operation_timeout`.
- **Breaking: a failed audit write is logged and dropped,** bounded by `Orchestrator(audit_timeout=...)`
  (5s by default), instead of stalling or crashing the run. A `StaleEpochError` still stops it.
- **Breaking: an inbox entry of a DataPoint type this process does not know is delivered as poison** and
  quarantined, instead of raising out of every pass of the session.
- The completion tail's per-step log is DEBUG, no longer INFO.

### Added

- **`orcastork_lite`**, a second package in the same distribution: the scheduling core and the
  dependency injection of orcastork with everything else removed. Operators (`depends_on` /
  `uses` / `produces` / `requires` / `consumes`, `OperatorPolicy` with rerun, `rerun_on`, debounce,
  timeout, retry and `max_cycles`), lazily-activated capabilities, a `CapabilityCatalog` with
  per-namespace gating, and an `Orchestrator` that runs a session to quiescence in one process and
  returns its DataPoints, bounded by a `session_deadline`. Every change is published to a
  `SessionEventSink` (in-memory and Redis-stream adapters ship under `orcastork_lite.adapters`) so a
  consumer can follow a session while it runs. No durability, resumability, epochs, inbox, parking,
  aggregators, audit, archive or telemetry. The `orcastork-lite-graph` CLI (`orcastork_lite.tools`)
  validates the cycle policy and renders Mermaid from the classes a module exposes.
- **`build_redis_runtime`** (`orcastork.adapters.redis`): one client and one state TTL,
  wiring the store, inbox and lock, with no-op audit, archive and durable store ports.
- **`NullAuditSink`, `NullDataPointArchive`, `NullDurableStore`** (`orcastork.ports`).
- `UnhashableValueError`, `SessionStatus.RUNNING`, `DataPoint.identity`.
- **A re-drive for input no orchestrator processed.** `SessionOrchestrationManager.redrive_stalled` resumes
  the sessions holding unacked inbox entries that no live orchestrator owns, read oldest first from a
  pending-input index that `Inbox.append(..., namespace_id=...)` fills and the draining ack or
  quarantine empties (`Inbox.find_pending`, `Inbox.drop_pending`, `PendingSession`). It is the host's
  periodic backstop for an owner and a delivering pod that both died with input unread. `deliver`
  now indexes what it appends.
- **`append_input` and `drive`**, the two halves of `deliver`, so an ingress can answer once its
  input is durable and drive the session in the background.

### Fixed

- **A park rechecks the inbox before it releases the lease.** A delivery landing as the session
  parked waited for the delivering pod's in-memory recheck, and was stranded if that pod died. It is
  now gathered on the same epoch.
- **The Redis inbox stream is trimmed** as entries are applied or quarantined, and a stream created
  by a read expires with the session state. It used to grow by one entry per delivery, and a stream
  created by a read never expired.
- **A stored DataPoint of an unknown type is skipped with a warning** on every Redis store read
  (rehydrate, snapshot, change set) instead of failing the whole session, so a deploy that adds a
  type can be rolled back.
- Ported from `orcastork_lite`:
  - **The session deadline is hard for in-flight runs:** each run's timeout is clipped to the budget
    left, and completions already queued at the deadline are recorded rather than dropped.
  - **A DataPoint's identity is computed once**, at construction, and an unhashable value is refused
    there (`UnhashableValueError`), failing only the operator that emitted it. A set mixing types, and
    a pydantic-model value, get a stable identity.
  - **An armed retry is made terminal when its operator loses readiness,** instead of silently
    vanishing.
  - **Armed debounce and retry windows fire while other operators run,** instead of waiting for a
    quiet operator to emit or finish.

## [0.1.1]

First installable release.

### Fixed

- Nothing in the library changed from `0.1.0`. That version was published to PyPI and then
  deleted, which permanently retires its filenames — PyPI refuses to serve or accept
  `orcastork-0.1.0-*` ever again, so the version is unusable rather than merely absent. This
  release is the same code under a version number that can actually be installed.

## [0.1.0]

Withdrawn — published to PyPI and deleted, which burns the version permanently. Use `0.1.1`.
Its contents were:

### Added

- **Blackboard scheduling.** `Operator`s declare `depends_on` / `produces` / `requires` and
  run when their inputs exist, rather than in a fixed order. Quiescence — no operator having
  anything left to do — is what triggers the aggregation phase.
- **`DataPoint`s** as the unit of data: frozen, discriminated-union pydantic models whose
  identity excludes timestamps, so re-observing a value merges instead of duplicating.
- **`Capability`s** as injected action providers, activated lazily the moment they become
  available and resolved by a configurable preference order.
- **`Aggregator`s** as the sole writers of curated durable output — idempotent, OCC-guarded,
  and dead-lettered rather than lost on repeated failure.
- **Session resumability.** A fencing `Epoch` on every write path, so a crashed or parked
  session can be re-driven by another process and the superseded run stops cleanly.
- **`completes_when` and parking.** A declarative completion condition (`TypePresent` composed
  with `all_of` / `any_of`) lets a session wait for mid-session input, and release its process
  entirely once the wait goes idle past `park_after`.
- **`ctx.once`**, a claim/commit/revert guard for non-idempotent side effects that holds across
  reruns, retries and crash-resumes.
- **`FlowDefinition`** naming a flow once — operators, capabilities, completion condition,
  policies, tuning — with a fingerprint that detects flow drift on resume.
- **Ports and adapters.** Nine `typing.Protocol` ports with three adapter families: in-memory
  (no infrastructure), Redis (store, inbox, lock, cooldown gate, rate limiter) and MongoDB
  (durable store, DataPoint archive, audit sink). One conformance suite runs against all of
  them, so families are behaviourally interchangeable.
- **`DataPointArchive`**, a second durable write path that live-archives every non-ephemeral
  DataPoint off the hot path via a write-behind buffer, with PII sealed through an injected
  `ValueCipher`.
- **Operational tooling.** `replay_session` reconstructs a session from its audit trail,
  `describe_session` reports why a live session is stuck, and the `orcastork-graph` console
  script validates the dependency graph at deploy time and renders it as Mermaid.
- **Built-in OpenTelemetry**, instrumented against the API only so it no-ops until a
  deployment wires an SDK, plus a loguru-to-OTel log bridge.
- **Injected `Clock`** throughout, which is what makes the test suite deterministic and fast.

[Unreleased]: https://github.com/Imper-ai/orcastork/compare/0.2.0...HEAD
[0.2.0]: https://github.com/Imper-ai/orcastork/compare/0.1.1...0.2.0
[0.1.1]: https://github.com/Imper-ai/orcastork/releases/tag/0.1.1
[0.1.0]: https://github.com/Imper-ai/orcastork/releases/tag/0.1.0
