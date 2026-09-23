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
- [README.md](README.md) is the npm package's front door and the TypeScript counterpart of
  [../README.md](../README.md) — same structure, same depth, translated rather than summarised. It is
  a *user* document; this file is the maintainer one, and neither should grow into the other.
  `tests/readme.test.ts` executes it, so a public-surface change lands in both or fails CI.

## Toolchain

- Node ≥ 22, TypeScript strict, **ESM with `NodeNext` resolution** — relative imports carry the
  `.js` extension (`import { Clock } from './clock.js'`), `verbatimModuleSyntax` is on (`import type`
  for types), `noUncheckedIndexedAccess` and `exactOptionalPropertyTypes` are on.
- `npm run lint` = `biome check` + `tsc --noEmit`. `npm run fix_lint` = `biome check --write` —
  which rewrites whole files, so read "The NUL hazard" below before running it over an adapter.
- `npm test` = `vitest run` (the default suite; needs no services beyond what the test setup spawns
  itself). `npm run test:integration` runs `*.integration.test.ts`. `make deps lint test` wraps the
  three, and `make deps` is `npm ci` — the lock file is the dependency list.
- **Decorators are TS 5 standard decorators**; `experimentalDecorators` is off on purpose (legacy
  decorators would be a third dialect the runtime, vitest's transpiler and `tsc` would each read
  differently). `tests/internal/decorators.test.ts` is the canary: registration *is* a decorator
  here, so a toolchain that silently dropped or mis-lowered one would produce a package with empty
  registries rather than a loud failure.
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
| `__init_subclass__` registration | **Class decorators** (TS 5 standard decorators): `@dataPointType('url', { pii: false, ephemeral: false }, { value: z.string() }) class UrlDataPoint extends BaseDataPoint<string> {}`, `@operator class X extends Operator {…}`, `@capability class Y extends Capability {…}`, `@aggregator` (the same function as `@operator`, under a name that reads better). The decorator sets the statics, validates the declaration and registers; a duplicate key throws `DuplicateRegistrationError` at definition time exactly as Python does. An abstract intermediate carries `@abstractDataPoint(config?, options?)` when it wants to hand its leaves a shared config or value contract, and is otherwise simply undecorated — the graph and `instanceof` read the prototype chain either way. |
| a decorator that must read what the class **declared** | `context.addInitializer(function () { … })`, not the decorator body. `tsc` lowers a class decorator to run **before** the static fields are initialized, while esbuild (what vitest transpiles with) runs it after — so a decorator body that reads `target.operatorId` works under one toolchain and sees `undefined` under the other. A class-decorator initializer runs after the statics under both. `operators/base.ts::operator` documents this and also accepts a hand-applied call (`operator(SomeClass)`, how a class *expression* in a test factory registers), where the class is already initialized. |
| class attributes (`operator_id`, `policy`, `depends_on`, `uses`, `produces`, `requires`, `consumes`) | `static readonly` fields, camelCase (`operatorId`, `dependsOn`, …). Type sets are declared as `readonly` arrays of classes (`static readonly dependsOn = [UrlDataPoint]`); the engine dedups into `Set`s. |
| `type[BaseDataPoint]`, `isinstance`, `issubclass` | A `DataPointClass` constructor type; `instanceof`; an `isSubclass(a, b)` helper walking the prototype chain. Subtype substitution works through the class hierarchy, as in Python. |
| `Enum` | `as const` object + derived union type. Member names stay UPPER_SNAKE and **values stay the exact Python strings** — they reach the audit trail and the wire. Branch with `switch` and an exhaustive `never` default. |
| `datetime` (always UTC) | `Date`. Serialized with `toISOString()`; parsed from either `Z` or `+00:00`. |
| `timedelta` / `float` seconds | `number` **milliseconds**, and the name ends in `Ms`: `debounceMs`, `timeoutMs`, `parkAfterMs`, `baseDelayMs`, `operationTimeoutMs`. `Clock.now(): Date`, `Clock.monotonic(): number` (ms), `Clock.sleep(ms, signal?)`. Defaults keep the Python values (30 s → `30_000`). |
| `clock.sleep(...)` — a debounce / backoff / idle **window** | `clock.sleep(ms, signal?)` on the injected clock, loop-scheduled. Never a timer. The optional `AbortSignal` is how a caller that **raced** the sleep and lost gives it up (`Promise.race([queue.whenNotEmpty(), clock.sleep(ms, controller.signal)])`, then `controller.abort()` in a `finally`): Python cancels the task it raced, and without the signal a `SystemClock` timer stays armed for the window's full width on every pass. The implementation rejects with `SleepAbortedError`; a clock that advances time synchronously (`FakeClock`) has nothing pending and ignores it. `Inbox.waitForEntry(sessionId, signal?)` takes one for the same reason — the Redis subscriber is a real socket plus a keepalive, and an abandoned wait would hold both for the life of the process, one per parked session. |
| `asyncio.wait_for(awaitable, t)` — a **timeout** bounding a real await (an operator run, an aggregator run, a capability activation, an event publish, a completion-tail step) | `withTimeout(promise, ms)` from the package's `internal/timeouts.ts`, on a real, `unref`'d `setTimeout` — exactly as Python's `wait_for` runs on real time. This and `SystemClock.sleep` are the only timers in core. A `FakeClock` sleep is instantaneous in real time, so a bound only fires when something genuinely hangs, which is what the Python tests rely on (a stub that really sleeps 5 s against a 20 ms timeout). Cancellation is an `AbortSignal` on the context: on timeout or deadline the loop aborts it and calls `return()` on the generator; already-yielded emissions are kept, as in Python. |
| `asyncio.shield(...)` | Promises cannot be cancelled, so a shielded await is a plain `await`; keep the comment saying why it must run to completion. |
| `asyncio.sleep(0)` — yield to the loop | `yieldToEventLoop()` — `await new Promise<void>((resolve) => setImmediate(resolve))`. In tests and test doubles freely. In an engine loop only at its own wait points, and only because asyncio resumes the gathering task after an operator has run its whole non-suspending burst while a JS waiter wakes on the first `put`: without the yield a fast operator's emissions split across passes, one store apply and one re-plan per emission, and the revision numbers the Python tests pin come out wrong. There are **three** such yields in the whole port and each carries that reasoning in a comment: `orcastork/orchestrator/orchestrator.ts::nextSignal`, and `orcastork_lite/orchestrator.ts::nextSignal` plus its `drainRemaining` (a run whose last `put` is still in flight has finished, and Python would already have seen it queued). Copy the reasoning, not the habit — and if a fourth is ever needed, say in the comment which of those two effects it is buying. |
| `asyncio.Queue(maxsize)`, `TaskGroup` | A bounded async queue in `internal/`; `Promise.all` / `Promise.allSettled`. |
| `async def run(...) -> AsyncIterator[DataPointEmission]` | `async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission>`. |
| `async with ctx.once(key) as acquired:` — a block with setup and teardown | A **callback**: `await ctx.once(key, async (acquired) => { … }, options?)`. JavaScript has no async context manager, so the body runs exactly where the `async with` body would, with the same claim before it and the same commit/revert after it. Two things a port loses if it is not careful, both deliberate here: the body runs **whether or not** the claim was acquired (`acquired` is a parameter, not a gate — a non-owning attempt must be able to fail on its own unrelated work without resolving a mark it does not own), and `once` also *returns* the acquired flag for a caller that would rather branch after the block. A body that throws propagates, as it does out of `async with`. |
| exception classes | Classes extending the package's base error (`OrchestrationError` / `OrcastorkLiteError`), same names as Python, `name` set to the class name. Core never throws a bare `Error`. |
| `hash(obj)` / `_make_hashable` | Identity is a canonical **string**, so `Map`s and `Set`s key on it. `canonical_value` is `repr(_make_hashable(value))` and that string reaches the wire (a Redis hash field, the archive key's digest input), so `internal/python_repr.ts` reproduces CPython's `repr` byte for byte — floats, string escaping, code-point key order and all — and `canonicalValue` is `pythonRepr`. Two limits are inherent: an integral number renders as the Python **int** (a JS number cannot know it was `1.0`), and an integer past 2^53 renders as the double it rounded to. `stableStringify` (`internal/stable_json.ts`) stays for callers that want stable JSON. |
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

Two divergences are known, documented in the README's "Sharing a deployment with Python workers",
and are **not** to be silently "fixed" by inventing a second canonical form:

- **An integral float canonicalizes differently.** JavaScript has one number type, so a value Python
  holds as `1.0` reprs as `1.0` there and as `1` here — a different identity, hence a different Redis
  hash field and archive key for the same logical DataPoint. (An integer past 2^53 has the same
  problem via `JSON.parse` rounding.) Everything else about `pythonRepr` is exact and pinned against
  fixtures CPython generated (`tests/internal/python_repr_fixtures.py` → `.json`); regenerate the
  fixtures rather than adjusting the expectations.
- **`socketTimeout` must exceed the inbox keepalive.** The Redis inbox's wakeup subscriber pings its
  connection every `WAKEUP_POLL_TIMEOUT_MS` (1 s) so a quiet session never idles into the client's
  read deadline. node-redis treats that deadline as **fatal and does not reconnect**, which is
  stricter than redis-py, so a deployment whose `socketTimeout` is at or below the ping interval
  strands parked sessions on a dead subscription. If that constant changes, the README and
  `docs/deployment.md` both state it and must change with it.

### The NUL hazard

Several wire-critical strings contain a NUL separator: the DataPoint identity
(`` `${type}\x00${canonicalValue(value)}` ``), the Mongo composite `_id`s, the durable store's
`key\x00field`. **They must stay as escape sequences in the source** (`'\u0000'`, `\x00` inside a
template literal). Both `biome check --write` and a careless whole-file rewrite have, in this
repository, turned an escape into a literal NUL **byte** — which still works, and still passes the
tests, but makes the file binary to git, to diff tooling and to review. After any bulk rewrite or
format of a file that carries one, check: `git diff --stat` reporting `Bin` for a `.ts` file, or
`grep -rlP '\x00' src/ tests/` returning anything, means an escape was eaten. Restore the escape;
never accept the byte.

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
  DataPoint / Operator / Capability registries around every test, like the Python autouse fixture. It
  covers registries that do not exist yet — everything built on `internal/registry.ts` joins the
  process-wide list on construction, so a new registry needs no change here.
- Port-conformance suites are functions — `describeStoreConformance(binding)`,
  `describeInboxConformance`, `describeLockConformance`, `describeAuditSinkConformance`,
  `describeDurableStoreConformance`, `describeDataPointArchiveConformance`,
  `describeCooldownGateConformance` (`tests/doubles/conformance/`) — that call `describe`/`it`
  themselves, written against the port interface only; each adapter test file binds them with a
  factory and an `advanceTime` callback.
- **`monkeypatch.setattr(module, 'Name', …)` becomes `vi.mock`.** An ESM import binding cannot be
  reassigned, so a test that has to observe what the engine constructed mocks the module the caller
  imports and returns `{ ...actual, Name: <subclass that records> }` — with the recorder built in
  `vi.hoisted` (the factory is hoisted above the imports) and the module re-imported through a
  top-level `await import(...)`. `tests/manager.test.ts` and `tests/replay.test.ts` are the two
  places; both keep the real behaviour and only record.
- Redis adapter tests run against a `redis-server` the test setup spawns on a free port (the binary
  is on PATH here and in CI); Mongo adapter tests run against `mongodb-memory-server`. Both honour an
  externally provided server (`ORCASTORK_TEST_REDIS_URL`, `ORCASTORK_TEST_MONGO_URL`) and skip with a
  loud warning when the server is unavailable — CI is the gate, and nothing skips there.
- Integration tests are `*.integration.test.ts` and excluded from `npm test`.
- `tests/readme.test.ts` executes the README's quickstarts and guide examples, and compiles its
  wiring snippets. Changing a public signature means changing the README in the same commit — the
  test is what makes that mechanical rather than remembered.

### The harness guards (`tests/harness.test.ts`)

They scan source **text**, so they run even where an optional backend is not installed and cannot be
defeated by a module that fails to load. Do not route around one; widen the declared set instead.

- **No undeclared dependency.** The allowed set is read out of `package.json` (runtime deps, optional
  peers, Node builtins), so adding a dependency there is the only way to widen it.
- **Infrastructure SDKs (`redis`, `mongodb`, `bson`) only under `adapters/`.** `@opentelemetry/*` is
  deliberately exempt: the OTel API is the built-in telemetry standard, not a backend behind a port.
- **The two packages never import each other**, in either direction.
- **Real time in three modules per package only** — `clock.ts` (the `SystemClock`),
  `internal/timeouts.ts` (`withTimeout`, which bounds a real await) and `logging.ts` (its own console
  record stamps). The pattern covers `setTimeout`/`setInterval`/`Date.now()`/`performance.now()`/
  `new Date()`; `setImmediate` is exempt because yielding to the loop is not a time source.
- **Only the wiring seams and the adapters themselves import an adapter** — `runtime.ts` and the
  adapter barrels, nothing in core.
- **The library never imports `tools/`** (the graph CLIs), in either package.
- **The walk itself finds files**, so a broken scan cannot pass every other guard vacuously.

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
