# Deploying orcastork

orcastork is a library, so "deploying" it means running your own process that imports it and
provisioning the infrastructure its adapters expect. This page is the operator's side of that:
what to provision, what the library creates inside it, and which knobs actually matter.

For what the framework *is* and how to write flows, start at the [README](../README.md).

## Contents

- [Choosing adapters](#choosing-adapters)
- [Redis: keyspace and TTLs](#redis-keyspace-and-ttls)
- [MongoDB: collections and indexes](#mongodb-collections-and-indexes)
- [Timeouts: the one relationship you must not break](#timeouts-the-one-relationship-you-must-not-break)
- [Running more than one process](#running-more-than-one-process)
- [Telemetry exporters](#telemetry-exporters)
- [Encryption](#encryption)
- [Deploy-time graph validation](#deploy-time-graph-validation)
- [Pre-flight checklist](#pre-flight-checklist)

## Choosing adapters

Every port has an in-memory implementation, and `build_in_memory_runtime()` wires a complete
runtime out of them. That runtime is not a degraded mode for tests only — it is the correct
choice for a single-process deployment that can afford to lose in-flight sessions on restart,
and it needs no infrastructure at all.

You move a port to Redis or Mongo when you need something the in-memory adapter cannot give
you:

| You need | Move to |
|---|---|
| A session to survive a process restart | Redis store, Redis lock |
| More than one process to work the same sessions safely | Redis lock (the fencing epoch is what makes takeover correct) |
| Mid-session input delivered from a different process | Redis inbox |
| Curated output readable by other systems | Mongo durable store |
| A queryable history of everything a session observed | Mongo archive |
| A replayable event trail per session | Mongo audit sink |

The mixes are free: the ports are independent, and the same conformance suite
(`tests/doubles/conformance.py`) runs against every family, so a Redis store and an in-memory
rate limiter compose without ceremony.

## Redis: keyspace and TTLs

**Give orcastork its own logical database or its own Redis.** The keys are *not* prefixed with
anything of the library's own — they are `dp:{session_id}`, `lock:{session_id}` and so on. If
you share a database with another workload that uses short keys, you own the collision.

| Key | Holds |
|---|---|
| `dp:{session}`, `added:{session}`, `upd:{session}` | The blackboard: DataPoints and their add/update revisions |
| `rev:{session}` | Monotonic revision counter (drives delta computation) |
| `epoch:{session}`, `mint:{session}` | Fencing epoch and the monotonic mint counter |
| `lock:{session}` | The liveness lease (`SET NX PX`) |
| `complete:{session}` | Completion flag |
| `wm:{session}` | Per-operator watermarks |
| `fx:{session}` | Effect-guard claim/commit marks (`ctx.once`) |
| `meta:{session}` | Session meta: wall-clock deadline, flow fingerprint |
| `inbox:{session}` | Mid-session input (a Stream with a consumer group) |
| `acked:{session}`, `quarantine:{session}` | Inbox acknowledgements and poison quarantine |
| `cooldown:{key}` | The manager's scheduling gate |
| `ratelimit:{key}` | Token-bucket state for capability pacing |

Every per-session key carries a long sliding TTL, refreshed on each write
(`DEFAULT_STATE_TTL_MS`, 24 hours). An active session therefore never expires, and a finished
one is reclaimed without a reaper job. Raise it if a session can legitimately sit parked for
longer than a day; the only constraint is that it must stay far larger than the second-scale
fencing and recovery windows.

Redis persistence is a real choice here, not a detail: with AOF disabled, a Redis restart
loses the blackboard, and sessions resume from whatever the durable stores hold.

## MongoDB: collections and indexes

The adapters create their own indexes lazily, on first write, and they are idempotent — there
is no migration step to run. What appears in the database:

| Collection | Written by | Indexes |
|---|---|---|
| `orcastork-datapoints` | `MongoDataPointArchive` (committed rows) | `_id` is the composite `(session_id, type, value_hash)`; `namespace_type_scan` on `(namespace_id, type)`; `retention_reaper` TTL on `last_retrieved` when a retention is configured |
| `datapoint_archive_buffer` | The archive's write-behind buffer | `session_trail` on `(session_id, sequence)` |
| `datapoint_archive_meta` | Archive sequence allocation | `_id` by session |
| `orcastork-audit-log` | `MongoAuditSink` | `session_trail` on `(session_id, sequence)` |
| `audit_meta` | Audit sequence allocation | `_id` by session |
| *your* `__table_name__` | `MongoDurableStore`, one collection per output model | `_id` is the record key |
| `<table>-fencing` | Epoch fence per curated table | TTL on `updated_at` |
| `contributions`, `contributions-fencing` | Aggregator contribution markers | `_id` by marker |

Two things follow from that table:

- **The archive grows without bound unless you configure a retention.** Pass
  `retention_seconds` to `MongoDataPointArchive` and it builds the TTL index; leave it unset
  and nothing reaps the collection. This is deliberate — a framework should not silently
  delete your data — but it means the decision is yours to make, not to postpone.
- **Changing a retention later needs an operator.** A TTL index that already exists with a
  different `expireAfterSeconds` is left alone, with a warning logged, rather than failing the
  write that noticed. Drop and recreate the index yourself when you change the value.

The durable store's `_id`s are your record keys, so two flows writing the same
`__table_name__` and key write the same document — which is what makes an aggregator
idempotent, and what makes a careless key collision silent. Namespace your keys if two
unrelated flows share a table.

## Timeouts: the one relationship you must not break

The lock is a liveness lease and the epoch is what makes takeover correct — but there is one
timing promise the framework cannot enforce for you:

> **The lock TTL must exceed the longest single unrenewed await.**

The gathering loop renews the lease as it goes (every `DEFAULT_LEASE_RENEW_INTERVAL`, 10s),
and stops renewing when gathering returns. Everything after that — aggregate, flush, re-check
the inbox, mark complete — runs on the lease it already holds. Each of those steps is
individually bounded by `operation_timeout` and raises `CompletionTailTimeoutError` rather
than overrunning silently, so the rule in practice is:

```
RedisSessionLock(ttl_ms=...)  >  operation_timeout
```

Set them too close and a slow finalize lets the lease lapse under a live owner: the manager
then reads the session as orphaned (started, not complete, lock not held), correctly resumes
it at a higher epoch, and the original run is fenced mid-finalize. Nothing corrupts — that is
what the epoch is for — but the work is done twice and the failure looks mysterious.

**The shipped defaults leave no headroom.** `RedisSessionLock`'s `ttl_ms` defaults to
`DEFAULT_TTL_MS` = 30_000 ms and the orchestrator's `operation_timeout` defaults to
`DEFAULT_OPERATION_TIMEOUT` = 30.0 s — equal, not greater. A tail step that actually consumes
its full budget therefore expires the lease at the same instant it gives up. Raise `ttl_ms`
(or lower `operation_timeout`) when you wire a real lock rather than inheriting both defaults:

```python
lock = RedisSessionLock(redis, ttl_ms=60_000)   # 2x the 30s operation_timeout
```

Any per-operator `OperatorPolicy.timeout` larger than `operation_timeout` raises the bar
further — the TTL has to clear the largest of them, not the global default.

Do **not** reach for a background keepalive to widen the margin. Renewing from a task that is
not the one doing the work turns the lease from "this session is progressing" into "this
process still has an event loop", which makes a wedged process look healthy and delays
recovery instead of avoiding it.

## Running more than one process

Nothing needs to elect a leader or shard sessions. Point every process at the same Redis and
Mongo and let them all call `resume`: the lock hands ownership to one, the epoch fences the
others, and a fenced run stops cleanly as `SUPERSEDED`.

What each process must do:

- **Import the modules that define your DataPoints, Operators and Capabilities at startup.**
  Registration happens in `__init_subclass__`, so a class nobody imported does not exist as
  far as the registries are concerned — and a flow that references it will not resume.
- **Run the same code.** A `FlowDefinition`'s fingerprint is checked on resume; a process
  running a different graph is detected as drift rather than quietly resuming into the wrong
  flow. Expect drift errors during a rolling deploy that changes a flow, and plan the rollout
  accordingly.
- **Call `resume` for orphaned sessions on some schedule.** A crashed or parked session is
  re-driven when someone asks; the manager does not poll on its own.

## Telemetry exporters

The framework instruments itself against the OpenTelemetry **API** only, which no-ops until a
deployment installs an SDK. Nothing is emitted, and nothing is lost, if you wire nothing.

If you do wire an SDK: **use the batching exporters.** Spans, metrics and logs are emitted
from the gathering loop, and a synchronous exporter that blocks on the network blocks the loop
that is scheduling your operators. `attach_otel_log_bridge()` ships loguru records as OTel
log records; attach it once per process (`detach_otel_log_bridge(bridge_id)` undoes it).

## Encryption

The framework ships no cryptography. `ValueCipher` is a two-method port, the default
`NullCipher` is a passthrough, and the archive adapter **fails closed**: archiving a value
marked `is_pii` while only `NullCipher` is wired raises `UnprotectedPiiError` instead of
persisting plaintext. A `NamespaceCipherProvider` resolves a cipher per namespace if your keys
are partitioned that way.

`mac` derives the archive key for a PII value and must be **keyed** — that is what stops the
stored key being an offline confirmation oracle for anyone holding the archive without the
key. `NullCipher.mac` falls back to an unkeyed SHA-256, which is why the Mongo adapter refuses
it for PII.

## Deploy-time graph validation

Run the graph check in CI or at startup, before the first session:

```bash
orcastork-graph -m my_flows.operators -m my_flows.capabilities --check
```

It builds the dependency graph from the registries and exits non-zero on an unbounded cycle —
the failure mode you least want to discover from a session that will not quiesce. `--mermaid -`
renders the graph to stdout, which is worth committing to your own docs.

## Pre-flight checklist

- [ ] Redis has its own logical database (the keys are unprefixed)
- [ ] Redis persistence matches how much in-flight state you can afford to lose
- [ ] `ttl_ms > operation_timeout` — the defaults are both 30s, i.e. no headroom
- [ ] The archive has a retention configured, or you have accepted unbounded growth
- [ ] Every module defining a DataPoint, Operator or Capability is imported at startup
- [ ] `orcastork-graph --check` runs in CI
- [ ] A real `ValueCipher` is wired if any DataPoint is marked `is_pii`
- [ ] OTel exporters, if any, are the batching ones
- [ ] Something calls `resume` for orphaned sessions on a schedule
