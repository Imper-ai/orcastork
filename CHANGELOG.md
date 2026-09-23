# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). While the
version is below 1.0, minor releases may contain breaking changes; they will always be listed
under **Changed** with the migration.

## [Unreleased]

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
- **A TypeScript port**, in [`typescript/`](typescript/README.md): one npm package `orcastork` with
  `orcastork` and `orcastork/lite` entrypoints, mirroring both Python packages module for module.
  All nine ports with their in-memory, Redis (node-redis) and Mongo (mongodb driver) adapters,
  published as `orcastork/adapters/{memory,redis,mongo}` and `orcastork/lite/adapters/{memory,redis}`
  with `redis` and `mongodb` as optional peer dependencies; both graph CLIs (`orcastork-graph`,
  `orcastork-lite-graph`). Everything persisted or published is written in the Python package's
  wire format — Redis keys, Lua scripts, hash fields, inbox payloads, Mongo collections and document
  shapes, audit and archive rows, flow fingerprints, the lite event stream — and checked against the
  Python source and its outputs (identical Lua scripts, key and collection names, document shapes,
  CPython-generated canonical-value fixtures, an identical fingerprint digest). A mixed Python/Node
  deployment is the design goal; it has not yet been exercised end to end. Two caveats are
  documented rather than fixed: a DataPoint value that Python holds as an integral float (`1.0`) gets
  a different identity in Node, which has one number type; and a node-redis client's `socketTimeout`
  must exceed the inbox wakeup keepalive (1 s), because node-redis treats that deadline as fatal and
  does not reconnect. 1,268 tests, one per Python test that has a meaning there, with the port
  contracts run as shared conformance suites against every adapter family — and against a real
  `redis-server` and `mongodb-memory-server` rather than in-process fakes. Toolchain: Node ≥ 22,
  TypeScript strict with ESM/`NodeNext`, TS 5 standard decorators for registration, biome for lint
  and format, vitest for tests, zod at the public surface only.

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

[Unreleased]: https://github.com/Imper-ai/orcastork/compare/0.1.1...HEAD
[0.1.1]: https://github.com/Imper-ai/orcastork/releases/tag/0.1.1
[0.1.0]: https://github.com/Imper-ai/orcastork/releases/tag/0.1.0
