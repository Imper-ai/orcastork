# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). While the
version is below 1.0, minor releases may contain breaking changes; they will always be listed
under **Changed** with the migration.

## [Unreleased]

## [0.1.0]

First public release.

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

[Unreleased]: https://github.com/Imper-ai/orcastork/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Imper-ai/orcastork/releases/tag/v0.1.0
