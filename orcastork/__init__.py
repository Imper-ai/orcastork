"""orcastork — a reusable, standalone dataflow orchestration framework.

A blackboard/dataflow engine: ``Operator``s consume and produce ``DataPoint``s,
``Capability``s provide actions, ``Aggregator``s write durable outputs, and a
session-scoped ``Orchestrator`` (supervised by a ``SessionOrchestrationManager``)
schedules work by data readiness rather than fixed phases.

The core is infrastructure- and domain-agnostic: it depends only on a set of
``ports`` (Protocols); concrete backends (in-memory, Redis, Mongo) are injected as
``adapters``. It is a library, not a service: an embedding application runs it in its
own event loop.
"""
