# orcastork (TypeScript)

Notes for anyone — human or agent — working on the TypeScript port. **The Python packages in
`../orcastork` and `../orcastork_lite` are the specification**: same behaviour, same invariants,
same tests, same wire formats. The root [CLAUDE.md](../CLAUDE.md) explains *why* the framework is
shaped the way it is; every invariant there applies here unchanged. This file is only about how
those decisions are expressed in TypeScript.

## Layout

One npm package, `orcastork`, with two entrypoints and no imports between them:

| npm entrypoint     | source                  | mirrors             |
|--------------------|-------------------------|---------------------|
| `orcastork`        | `src/orcastork/`        | `../orcastork/`     |
| `orcastork/lite`   | `src/orcastork_lite/`   | `../orcastork_lite/`|

- **Files mirror the Python modules one-to-one and keep their snake_case names**:
  `orcastork/ports/datapoint_store.py` → `src/orcastork/ports/datapoint_store.ts`,
  `orcastork/adapters/redis/inbox.py` → `src/orcastork/adapters/redis/inbox.ts`. A reader must be
  able to find the Python original of any file in one step. Each directory has an `index.ts` barrel
  mirroring the Python `__init__.py` exports.
- **Tests mirror `../tests/`**: `tests/test_orchestrator.py` → `tests/orchestrator.test.ts`,
  `tests/adapters/test_redis.py` → `tests/adapters/redis.test.ts`, `tests/orcastork_lite/test_x.py`
  → `tests/orcastork_lite/x.test.ts`, `tests/doubles/*.py` → `tests/doubles/*.ts`. One Python test
  function → one vitest `it(...)` with the same intent and assertions, named in plain English. A test
  that cannot be ported is not deleted silently: it is `it.skip`ped with the reason in the name.
- `src/orcastork/tools/` and `src/orcastork_lite/tools/` (the graph CLIs) are never imported by the
  library; `tests/harness.test.ts` enforces it, together with the dependency and adapter boundaries.
- Tiny cross-cutting helpers a package needs (a bounded async queue, stable JSON, a deferred) live in
  that package's `internal/` directory. Never a shared `utils` between the two packages.

## Toolchain

- Node ≥ 22, TypeScript strict, **ESM with `NodeNext` resolution** — relative imports carry the
  `.js` extension (`import { Clock } from './clock.js'`), `verbatimModuleSyntax` is on (`import type`
  for types), `noUncheckedIndexedAccess` and `exactOptionalPropertyTypes` are on.
- `npm run lint` = `biome check` + `tsc --noEmit`. `npm run fix_lint` = `biome check --write`.
- `npm test` = `vitest run` (the default suite; needs no services beyond what the test setup spawns
  itself). `npm run test:integration` runs `*.integration.test.ts`.
- Biome is the formatter and linter: single quotes, 2-space indent, line width 119, no semicolon
  omission games (semicolons always).
- **The declared dependencies are the whole dependency list, enforced.** `tests/harness.test.ts`
  reads `package.json` and fails on any import outside it (Node builtins excepted). Runtime deps:
  `zod`, `@opentelemetry/api`, `@opentelemetry/api-logs`. Optional peer deps (`redis`, `mongodb`)
  may only be imported under `adapters/`. Adding a dependency is a deliberate, reviewed act.

## Idioms (Python → TypeScript)

| Python | TypeScript |
|---|---|
| `NewType('SessionId', str)` | Branded type + constructor function: `type SessionId = Brand<string, 'SessionId'>`, `const SessionId = (v: string): SessionId => v as SessionId`. Integer ids (`Epoch`, `Revision`) brand `number`. |
| `typing.Protocol` (a port) | `interface`. |
| `@dataclass(frozen=True)` (engine-internal) | Plain `readonly` interface or class, frozen with `Object.freeze` where it is handed out. No runtime validation on the hot path (see the lite package's note in the root CLAUDE.md — measured). |
| pydantic model at the public surface (policies, events, `SessionResult`, `FlowDefinition`) | A zod schema **plus** a factory/class that parses on construction, e.g. `OperatorPolicy({ rerunOnNewData: true })`. Validation pays only at the trust boundary. |
| pydantic class hierarchy (DataPoints, Operators, Capabilities) | Classes. |
| `__init_subclass__` registration | **Class decorators** (TS 5 standard decorators): `@dataPointType('url', { pii: false, ephemeral: false }) class UrlDataPoint extends BaseDataPoint<string> {}`, `@operator class X extends Operator {…}`, `@capability class Y extends Capability {…}`. The decorator sets the statics, validates the declaration and registers; a duplicate key throws `DuplicateRegistrationError` at definition time exactly as Python does. Abstract intermediates are simply not decorated. |
| class attributes (`operator_id`, `policy`, `depends_on`, `uses`, `produces`, `requires`, `consumes`) | `static readonly` fields, camelCase (`operatorId`, `dependsOn`, …). Type sets are declared as `readonly` arrays of classes (`static readonly dependsOn = [UrlDataPoint]`); the engine dedups into `Set`s. |
| `type[BaseDataPoint]`, `isinstance`, `issubclass` | A `DataPointClass` constructor type; `instanceof`; an `isSubclass(a, b)` helper walking the prototype chain. Subtype substitution works through the class hierarchy, as in Python. |
| `Enum` | `as const` object + derived union type. Member names stay UPPER_SNAKE and **values stay the exact Python strings** — they reach the audit trail and the wire. Branch with `switch` and an exhaustive `never` default. |
| `datetime` (always UTC) | `Date`. Serialized with `toISOString()`; parsed from either `Z` or `+00:00`. |
| `timedelta` / `float` seconds | `number` **milliseconds**, and the name ends in `Ms`: `debounceMs`, `timeoutMs`, `parkAfterMs`, `baseDelayMs`, `operationTimeoutMs`. `Clock.now(): Date`, `Clock.monotonic(): number` (ms), `Clock.sleep(ms)`. Defaults keep the Python values (30 s → `30_000`). |
| `clock.sleep(...)` — a debounce / backoff / idle **window** | `clock.sleep(ms)` on the injected clock, loop-scheduled. Never a timer. |
| `asyncio.wait_for(awaitable, t)` — a **timeout** bounding a real await (an operator run, an aggregator run, a capability activation, an event publish, a completion-tail step) | `withTimeout(promise, ms)` from the package's `internal/timeouts.ts`, on a real, `unref`'d `setTimeout` — exactly as Python's `wait_for` runs on real time. This and `SystemClock.sleep` are the only timers in core. A `FakeClock` sleep is instantaneous in real time, so a bound only fires when something genuinely hangs, which is what the Python tests rely on (a stub that really sleeps 5 s against a 20 ms timeout). Cancellation is an `AbortSignal` on the context: on timeout or deadline the loop aborts it and calls `return()` on the generator; already-yielded emissions are kept, as in Python. |
| `asyncio.shield(...)` | Promises cannot be cancelled, so a shielded await is a plain `await`; keep the comment saying why it must run to completion. |
| `asyncio.sleep(0)` — yield to the loop | `await new Promise<void>((resolve) => setImmediate(resolve))`. In tests and test doubles freely. In an engine loop only at its own wait points, and only because asyncio resumes the gathering task after an operator has run its whole non-suspending burst while a JS waiter wakes on the first `put`: without the yield a fast operator's emissions split across passes and the revision numbers the Python tests pin come out wrong. `orcastork_lite/orchestrator.ts` documents the two places; copy that reasoning, not the habit. |
| `asyncio.Queue(maxsize)`, `TaskGroup` | A bounded async queue in `internal/`; `Promise.all` / `Promise.allSettled`. |
| `async def run(...) -> AsyncIterator[DataPointEmission]` | `async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission>`. |
| exception classes | Classes extending the package's base error (`OrchestrationError` / `OrcastorkLiteError`), same names as Python, `name` set to the class name. Core never throws a bare `Error`. |
| `hash(obj)` / `_make_hashable` | Identity is a canonical **string**: `stableStringify` (sorted object keys, order-insensitive sets) of the value, so `Map`s and `Set`s key on it. `canonical_value` is the same function. |
| `orjson.dumps(..., OPT_SORT_KEYS)`, `hashlib.sha256` | `stableStringify` + `node:crypto`. |
| `loguru` (`logger.info('msg', key=value)`) | `logging.ts` in each package: a `Logger` interface, `getLogger()`/`setLogger()`, a console JSON default. Always `logger.info('Session started', { sessionId })` — a fixed message plus fields, never interpolation. `logging_bridge.ts` ships records to `@opentelemetry/api-logs`. |
| `frozenset`, `Mapping`, `tuple` returned to callers | `ReadonlySet`, `ReadonlyMap`, `readonly T[]`. No tuple return types: return a small named `interface`. |
| `TypeAdapter` discriminated-union parsing of a serialized DataPoint | `parseDataPoint(json)` over the registry; an unknown `type` throws `UnknownDataPointTypeError`. |

### Wire compatibility is a requirement

Anything that is persisted or published — Redis keys and hash fields, Lua-script semantics, Mongo
collection names and document shapes, audit rows, archive rows, inbox payloads, the lite Redis event
stream — keeps the Python **snake_case field names, key names and values**, so a TypeScript worker and
a Python worker can share one deployment and a session written by one can be read by the other.
TypeScript property names stay camelCase; the adapter maps at its boundary and nowhere else. When in
doubt, open the Python adapter and copy the exact string.

## Tests

- vitest, deterministic: no absolute dates, no local clock, no test-order dependence. Everything the
  engine times on the clock (windows, deadlines, TTL arithmetic) is driven through `FakeClock`
  (`tests/doubles/clock.ts`), which mirrors the Python double: `sleep(ms)` advances the clock and
  yields once so concurrent tasks interleave. Never `vi.useFakeTimers()` for engine logic.
- A stub that must stay **in flight** (to trip an operation timeout or the session deadline) uses a
  real, abortable sleep tied to `ctx.signal` — `sleepAfterMs: 5000` against a 20 ms timeout, mirroring
  the Python doubles' `asyncio.sleep(5.0)` — with its timer `unref`'d and cleared on abort, so a test
  never waits it out and a stray timer never holds the worker open.
- Registry isolation: `tests/setup.ts` (registered as a vitest `setupFiles`) snapshots and restores the
  DataPoint / Operator / Capability registries around every test, like the Python autouse fixture.
- Port-conformance suites are functions — `describeDataPointStoreConformance(binding)` — that call
  `describe`/`it` themselves, written against the port interface only; each adapter test file binds
  them with a factory and an `advanceTime` callback.
- Redis adapter tests run against a `redis-server` the test setup spawns on a free port (the binary
  is on PATH here and in CI); Mongo adapter tests run against `mongodb-memory-server`. Both suites
  skip with a loud warning when the server is unavailable — CI is the gate.
- Integration tests are `*.integration.test.ts` and excluded from `npm test`.

## House style

- TSDoc on every exported symbol. **Carry the Python docstrings over**: they explain *why*, and the
  why is the part a port loses first. Comments stay timeless — no ticket numbers, nothing that stops
  making sense when the code moves.
- Explicit return types on every exported function and method; `readonly` by default; `import type`
  for types; Node builtins imported with the `node:` prefix; imports at the top of the module.
- `unknown` over `any`. An `any` needs a `// biome-ignore lint/suspicious/noExplicitAny: <why>`.
- Prefer `switch` with an exhaustive `never` default over if/else chains on an enum or status.
- Custom exceptions from `exceptions.ts` for every known error case.
- Same rules as Python for the engine: the injected `Clock` is the only time source; the orchestrator
  is the sole store writer through the mirror; every mutating port method takes an `epoch`; the
  completion tail is bounded, not renewed; retries are loop-scheduled windows, never in-task sleeps;
  completion conditions are a declarative AST, never callables; `orcastork_lite` stays without the
  features it deliberately lacks.
