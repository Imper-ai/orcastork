/**
 * `SessionOrchestrationManager` — the fleet-level supervisor.
 *
 * Three jobs: supply the per-namespace capability catalog (already on the runtime), mint a higher
 * fencing epoch on **every** grant (delegated to the SessionLock the orchestrator acquires —
 * strictly increasing, so a predecessor is fenced), and resurrect orphaned sessions. A session is
 * **orphaned** when it is not yet COMPLETED (a flag on the {@link SessionLock}, co-located with the
 * fencing epoch and set by the orchestrator that finished it) yet its ownership lock has expired;
 * resume spawns a fresh orchestrator that rehydrates from the persisted store and re-drives
 * unfinished work (capabilities are re-activated fresh, never resumed in place). Completion lives on
 * the lock alongside the epoch, not in this process, so a peer supervisor on another pod sees a
 * finished session and never re-drives it.
 *
 * Every call drives one {@link FlowDefinition} — the flow is named once and its fingerprint travels
 * with every spawn, so a resume whose flow no longer matches the session's persisted one is detected
 * and audited by the orchestrator instead of silently changing scheduling semantics mid-session.
 *
 * A {@link SchedulingGate} decides when a *new* session may start (e.g. the device-posture
 * cooldown) — a clock-driven gate, not an in-session timer; {@link
 * SessionOrchestrationManager.startSession} enforces it and refuses a start that is still cooling
 * down.
 *
 * {@link SessionOrchestrationManager.deliver} is the ingress front door for mid-session input: it
 * appends the DataPoint to the durable inbox first, then guarantees some orchestrator will process
 * it — a live holder is woken by the inbox push; an orphaned session is resumed on this pod. Because
 * the appending ingress always runs on a live pod, append-triggered resume closes the pod-kill
 * recovery loop without a global orphan scanner; a host-level periodic re-drive (the host owns the
 * session index) remains the backstop for the double-failure case.
 *
 * @module
 */

import type { ConcreteCapabilityClass } from '../capabilities/index.js';
import type { AnyDataPoint } from '../datapoints/index.js';
import { isSubclass } from '../datapoints/index.js';
import { LockHeldError, SchedulingGateBlockedError } from '../exceptions.js';
import type { FlowDefinition } from '../flow.js';
import type { NamespaceId, SessionId } from '../ids.js';
import { Deferred } from '../internal/deferred.js';
import { getLogger } from '../logging.js';
import { LOGGER_NAME_FIELD } from '../logging_bridge.js';
import type { ConcreteOperatorClass } from '../operators/index.js';
import { Aggregator } from '../operators/index.js';
import type { OrchestratorResult } from '../orchestrator/index.js';
import { Orchestrator, SessionStatus } from '../orchestrator/index.js';
import type { CooldownGate } from '../ports/cooldown_gate.js';
import type { OrchestratorRuntime } from '../runtime.js';
import { withSpan } from '../telemetry.js';

/** The module a log record names as its origin, so the OTel bridge can filter on it. */
const LOGGER_NAME = 'orcastork.manager.manager';

/**
 * How long deliver waits before rechecking a session whose lock was held at delivery time.
 *
 * Must exceed the lock adapter's lease TTL: if the holder died right after the append, its lease has
 * expired by the recheck and the resume re-drives the message.
 */
export const DEFAULT_REDELIVERY_RECHECK_MS = 35_000;

/**
 * How many times the manager's post-run backstop may re-spawn to drain a straggler.
 *
 * The backstop re-spawns to drain a straggler that landed in the completing orchestrator's
 * non-atomic check→release gap. Each re-spawn re-checks and re-drives, so the loop terminates as
 * soon as no deliver races the gap. This bound only fires under an adversarial burst of gap-landing
 * delivers; it caps the manager's own work so a pathological caller cannot spin it forever.
 */
export const MAX_BACKSTOP_RESPAWNS = 8;

/**
 * `SessionLock.currentEpoch` reports 0 for a session whose lock was never acquired, which is the one
 * signal that tells a never-started session apart from one that started and then lost its owner —
 * the two states an expired (absent) lease otherwise looks identical from.
 */
const NEVER_MINTED_EPOCH = 0;

/**
 * A per-key cooldown deciding when the next session for that key may start.
 *
 * A thin policy wrapper over the {@link CooldownGate} port: the cooldown state lives in the backing
 * store (cross-pod, restart-safe), and check-and-arm is one atomic step, so two racing starts can
 * never both pass.
 */
export class SchedulingGate {
  private readonly gate: CooldownGate;
  private readonly cooldownMs: number;

  public constructor(gate: CooldownGate, options: { readonly cooldownMs: number }) {
    this.gate = gate;
    this.cooldownMs = options.cooldownMs;
  }

  /** Atomically arm the cooldown for `key` iff none is active; `true` means proceed. */
  public async tryStart(key: string): Promise<boolean> {
    return await this.gate.tryAcquire(key, this.cooldownMs);
  }
}

/**
 * A mutual-exclusion lock over promises — what `asyncio.Lock` is in Python.
 *
 * Each acquirer chains onto the previous holder's release, so callers enter in arrival order and a
 * release wakes exactly the caller queued behind it.
 */
class AsyncLock {
  /** The promise the next acquirer waits on: the current tail holder's release. */
  private tail: Promise<void> | null = null;

  /** Wait for the lock, then hand back the release callable. */
  public async acquire(): Promise<() => void> {
    const previous = this.tail;
    const released = new Deferred<void>();
    this.tail = released.promise;
    if (previous !== null) {
      await previous;
    }
    return () => {
      released.resolve();
    };
  }
}

/**
 * One session's takeover lock and how many callers currently hold or await it.
 *
 * The count is the eviction signal because the lock cannot be one: a lock reports itself unlocked in
 * the window between the release and the woken waiter re-acquiring, so a caller that is queued is
 * invisible to the lock itself.
 */
class TakeoverGuardEntry {
  public readonly lock = new AsyncLock();
  public users = 0;
}

/** How a {@link SessionOrchestrationManager} is configured. */
export interface SessionOrchestrationManagerOptions {
  /** Enforced by `startSession` only; recovery paths (resume/deliver) are deliberately exempt. */
  readonly schedulingGate?: SchedulingGate | null;

  /** Deferred re-drive delay for a delivery that landed on a held lock. */
  readonly redeliveryRecheckMs?: number;
}

/** What `startSession` needs (Python's keyword-only arguments). */
export interface StartSessionOptions {
  readonly sessionId: SessionId;

  readonly namespaceId: NamespaceId;

  readonly flow: FlowDefinition;

  /** DataPoints the new session starts from; a resume never re-seeds. */
  readonly seed?: Iterable<AnyDataPoint>;

  /** The scheduling gate's key; defaults to the namespace. */
  readonly gateKey?: string | null;
}

/** What `resume` / `reopen` need (Python's keyword-only arguments). */
export interface ResumeOptions {
  readonly sessionId: SessionId;

  readonly namespaceId: NamespaceId;

  readonly flow: FlowDefinition;
}

/** What `deliver` needs (Python's keyword-only arguments). */
export interface DeliverOptions extends ResumeOptions {
  readonly dataPoint: AnyDataPoint;

  /** The caller's explicit declaration that this late data affects the durable result. */
  readonly reopenIfComplete?: boolean;
}

/** How a deferred recheck re-drives the session once the lease it deferred to has had its chance. */
export type Redrive = (options: ResumeOptions) => Promise<OrchestratorResult | null>;

/** Spawns, resumes, re-opens and feeds the sessions of one fleet. */
export class SessionOrchestrationManager {
  private readonly runtime: OrchestratorRuntime;
  private readonly gate: SchedulingGate | null;
  private readonly redeliveryRecheckMs: number;

  /**
   * Held so fire-and-forget rechecks are not collected before they run.
   *
   * @internal Exposed for tests, which await the scheduled rechecks rather than racing them.
   */
  public readonly recheckTasks = new Set<Promise<void>>();

  /**
   * Serializes RESUME attempts for one session WITHIN this process.
   *
   * Deliberately not applied to reopen: a serialized reopen caller finds the session complete again
   * once the winner finishes and correctly re-opens it, so a lock there queues re-opens instead of
   * collapsing them. Resume has no such property — a serialized caller sees `isComplete` and stands
   * down. The distributed lock already decides who owns a session, but it is acquired inside the
   * spawn — so without this every racer first builds an orchestrator and activates the whole
   * capability set, and only then discovers it lost. That is the expensive half of a takeover paid N
   * times over: N sets of Mongo/Redis/HTTP clients and N rounds of secret loading, which is enough
   * to OOM the pod on a burst. Holding this first means the losers re-run the cheap `isHeld` guard,
   * see the winner, and stand down having built nothing.
   */
  protected readonly takeoverLocks = new Map<SessionId, TakeoverGuardEntry>();

  public constructor(runtime: OrchestratorRuntime, options: SessionOrchestrationManagerOptions = {}) {
    this.runtime = runtime;
    this.gate = options.schedulingGate ?? null;
    this.redeliveryRecheckMs = options.redeliveryRecheckMs ?? DEFAULT_REDELIVERY_RECHECK_MS;
  }

  /**
   * Run `body` under this process's takeover lock for one session, cheapest-check-first.
   *
   * Entries are dropped once no caller holds or awaits them, so a long-lived process does not
   * accumulate a lock per session it ever touched.
   */
  private async withTakeoverGuard<T>(sessionId: SessionId, body: () => Promise<T>): Promise<T> {
    let entry = this.takeoverLocks.get(sessionId);
    if (entry === undefined) {
      entry = new TakeoverGuardEntry();
      this.takeoverLocks.set(sessionId, entry);
    }
    // Counted BEFORE the acquire, so a caller queued behind the current holder keeps the entry
    // mapped. Dropping it there would hand the next arrival an empty slot: it mints a second lock,
    // serializes against nobody, and enters the spawn alongside the queued caller — the concurrent
    // orchestrator-and-client build this guard exists to collapse.
    entry.users += 1;
    try {
      const release = await entry.lock.acquire();
      try {
        return await body();
      } finally {
        release();
      }
    } finally {
      entry.users -= 1;
      // Identity-checked so a departing caller can only ever evict the entry it counted itself
      // into, never a successor's live one.
      if (entry.users === 0 && this.takeoverLocks.get(sessionId) === entry) {
        this.takeoverLocks.delete(sessionId);
      }
    }
  }

  /**
   * Spawn a fresh orchestrator for a new session (mints epoch 1, injects the catalog).
   *
   * When a scheduling gate is configured the start is enforced here, keyed on `gateKey` (defaulting
   * to the namespace): a start still cooling down raises {@link SchedulingGateBlockedError}.
   * Enforcement lives in the manager so a caller cannot bypass the cooldown, and the gate's
   * check-and-arm is atomic so concurrent starts for the same key admit exactly one.
   */
  public async startSession(options: StartSessionOptions): Promise<OrchestratorResult> {
    const { sessionId, namespaceId, flow } = options;
    return await withSpan(
      this.runtime.telemetry.tracer,
      'session.start',
      async () => {
        getLogger().debug('Session start requested; spawning a fresh orchestrator for the new session', {
          [LOGGER_NAME_FIELD]: LOGGER_NAME,
          session_id: sessionId,
          namespace_id: namespaceId,
          flow_name: flow.name,
        });
        if (this.gate !== null) {
          const key = options.gateKey ?? String(namespaceId);
          if (!(await this.gate.tryStart(key))) {
            getLogger().warning('Session start blocked by the scheduling gate cool-down', {
              [LOGGER_NAME_FIELD]: LOGGER_NAME,
              session_id: sessionId,
              gate_key: key,
            });
            // Propagates through the span, so blocked starts are visible in traces.
            throw new SchedulingGateBlockedError(`scheduling gate is cooling down for key '${key}'`);
          }
        }
        return await this.spawnAndDrain(sessionId, namespaceId, flow, [...(options.seed ?? [])]);
      },
      { attributes: { session_id: sessionId, namespace_id: namespaceId, flow_name: flow.name } },
    );
  }

  /** Orphaned iff the session was started, is not COMPLETED, and its ownership lock has expired. */
  public async isOrphaned(sessionId: SessionId): Promise<boolean> {
    if (await this.runtime.lock.isComplete(sessionId)) {
      return false;
    }
    if ((await this.runtime.lock.currentEpoch(sessionId)) === NEVER_MINTED_EPOCH) {
      // Never started: no owner can have died and nothing is persisted to rehydrate.
      return false;
    }
    return !(await this.runtime.lock.isHeld(sessionId));
  }

  /**
   * Resume an orphaned session with a fresh, higher-epoch orchestrator (rehydrate + re-drive).
   *
   * Returns `null` if the session is already COMPLETED (never resumed) or still owned by a live
   * orchestrator. Resume does not re-seed — it rehydrates from the persisted store. `flow` should be
   * the same definition the session started with; one that drifted (a deploy changed the operator
   * set) is detected against the session's persisted flow fingerprint and audited, then driven
   * anyway. A *parked* session holds no lock and is not complete, so it resumes exactly like a
   * crashed one — with a shrinking deadline budget, since the wall-clock deadline persisted at the
   * first gather is rehydrated.
   */
  public async resume(options: ResumeOptions): Promise<OrchestratorResult | null> {
    return await this.withTakeoverGuard(options.sessionId, async () => await this.resumeLocked(options));
  }

  /** `resume` with this process's takeover lock already held. */
  private async resumeLocked(options: ResumeOptions): Promise<OrchestratorResult | null> {
    const { sessionId, namespaceId, flow } = options;
    return await withSpan(
      this.runtime.telemetry.tracer,
      'session.resume',
      async (span) => {
        if (await this.runtime.lock.isComplete(sessionId)) {
          span.setAttribute('disposition', 'already_complete');
          getLogger().debug('Resume skipped: session already completed', {
            [LOGGER_NAME_FIELD]: LOGGER_NAME,
            session_id: sessionId,
          });
          return null;
        }
        if ((await this.runtime.lock.currentEpoch(sessionId)) === NEVER_MINTED_EPOCH) {
          // Resume rehydrates from the persisted store, so a session that was never started has
          // nothing to rehydrate: spawning here would create it from whatever the inbox happens to
          // hold, with no seed. That is reachable from `deliver` — a caller that spawns the session
          // with the seed (`startSession`) races any delivery that arrives before its acquire lands,
          // and the loser is fenced out. Standing down keeps the seeded start the session's sole
          // creator; the delivery is already durably appended, so the start drains it.
          span.setAttribute('disposition', 'never_started');
          getLogger().debug('Resume skipped: session was never started', {
            [LOGGER_NAME_FIELD]: LOGGER_NAME,
            session_id: sessionId,
          });
          return null;
        }
        if (await this.runtime.lock.isHeld(sessionId)) {
          span.setAttribute('disposition', 'still_owned');
          getLogger().debug('Resume skipped: session still owned by a live orchestrator', {
            [LOGGER_NAME_FIELD]: LOGGER_NAME,
            session_id: sessionId,
          });
          return null;
        }
        getLogger().info('Resuming orphaned session with a fresh higher-epoch orchestrator', {
          [LOGGER_NAME_FIELD]: LOGGER_NAME,
          session_id: sessionId,
          namespace_id: namespaceId,
          flow_name: flow.name,
        });
        let result: OrchestratorResult;
        try {
          result = await this.spawnAndDrain(sessionId, namespaceId, flow, []);
        } catch (error) {
          if (!(error instanceof LockHeldError)) {
            throw error;
          }
          // Lost the acquire race to a concurrent resumer (the isHeld check above is not a claim).
          // The winner owns the session and its higher epoch fences us; nothing was mutated, so
          // standing down is the correct outcome — same contract as "still owned".
          span.setAttribute('disposition', 'lost_acquire_race');
          getLogger().debug('Resume skipped: lost the acquire race to a concurrent resumer', {
            [LOGGER_NAME_FIELD]: LOGGER_NAME,
            session_id: sessionId,
          });
          return null;
        }
        span.setAttribute('disposition', 'resumed');
        return result;
      },
      { attributes: { session_id: sessionId, namespace_id: namespaceId, flow_name: flow.name } },
    );
  }

  /**
   * Re-open a completed session to fold in late result-affecting data.
   *
   * Clears the completion flag and each aggregator's contribution marker so the freshly spawned
   * orchestrator re-runs the aggregation phase, then marks complete again. A fresh deadline is
   * granted so the re-open session can run even when the original deadline has elapsed (a budgeted
   * run completes in seconds; late participant data arrives much later). The re-open acquires a
   * strictly higher epoch — exactly like resume — so the re-aggregation write is epoch-fenced
   * against any stale predecessor.
   *
   * Returns `null` if the session is no longer complete (another reopen raced ahead and cleared the
   * flag first), if the lock is held by a live orchestrator (a concurrent reopen already won the
   * race), or if this reopen loses the acquire race ({@link LockHeldError} stand-down).
   */
  public async reopen(options: ResumeOptions): Promise<OrchestratorResult | null> {
    const { sessionId, namespaceId, flow } = options;
    return await withSpan(
      this.runtime.telemetry.tracer,
      'session.reopen',
      async (span) => {
        if (!(await this.runtime.lock.isComplete(sessionId))) {
          span.setAttribute('disposition', 'not_complete');
          getLogger().debug('Reopen skipped: session is not complete (raced ahead)', {
            [LOGGER_NAME_FIELD]: LOGGER_NAME,
            session_id: sessionId,
          });
          return null;
        }
        if (await this.runtime.lock.isHeld(sessionId)) {
          span.setAttribute('disposition', 'still_owned');
          getLogger().debug('Reopen skipped: session still owned by a live orchestrator', {
            [LOGGER_NAME_FIELD]: LOGGER_NAME,
            session_id: sessionId,
          });
          return null;
        }
        // Clear the completion flag before spawning so the new orchestrator can acquire the lock.
        await this.runtime.lock.clearComplete(sessionId);
        // Clear each aggregator's contribution marker so the re-spawn re-runs aggregation and folds
        // in the late data. The new orchestrator marks contributions again under a fresh, higher
        // epoch — the epoch fence on markContribution keeps the re-marks safe.
        await this.clearAggregatorContributions(sessionId, flow);
        getLogger().info(
          'Re-opening completed session for late result-affecting data; re-aggregating with a fresh epoch',
          {
            [LOGGER_NAME_FIELD]: LOGGER_NAME,
            session_id: sessionId,
            namespace_id: namespaceId,
            flow_name: flow.name,
          },
        );
        let result: OrchestratorResult;
        try {
          result = await this.spawnAndDrain(sessionId, namespaceId, flow, [], true);
        } catch (error) {
          if (!(error instanceof LockHeldError)) {
            throw error;
          }
          // Lost the acquire race to a concurrent reopener. The winner's higher epoch fences us;
          // nothing was mutated by the acquire itself, so standing down is correct — same contract
          // as resume's LockHeldError stand-down.
          span.setAttribute('disposition', 'lost_acquire_race');
          getLogger().debug('Reopen skipped: lost the acquire race to a concurrent reopener', {
            [LOGGER_NAME_FIELD]: LOGGER_NAME,
            session_id: sessionId,
          });
          return null;
        }
        span.setAttribute('disposition', 'reopened_for_late_data');
        return result;
      },
      { attributes: { session_id: sessionId, namespace_id: namespaceId, flow_name: flow.name } },
    );
  }

  /**
   * Deliver mid-session input and guarantee some orchestrator will process it.
   *
   * The durable inbox append always happens first — delivery never depends on what follows. Then
   * delivery guarantees a consumer only when the lock is free: a COMPLETED session with a free lock
   * is re-opened (aggregator re-runs, folds in the late data) when the caller passes
   * `reopenIfComplete`; otherwise the entry is appended and left to expire unprocessed. Re-open is
   * the caller's explicit intent, NOT inferred from the DataPoint — attribute leaves like
   * NAME/WORK_EMAIL are `ephemeral` (never individually archived) yet the aggregator DOES fold them
   * into the durable result, so ephemerality cannot be the discriminator. A session whose lock is
   * HELD is left to its live owner (woken by the inbox push): a completing owner drains its own
   * inbox before releasing (the orchestrator owns the inbox while it holds the epoch), so a deliver
   * landing in that window is folded in-run — no deliver-side reopen-recheck is needed. The one
   * held-lock recheck that remains guards the *dead-holder* case: an owner that died right after the
   * append is re-driven by a resume once its lease frees. An orphaned session is resumed on this pod
   * and the result returned. A *parked* session holds no lock and is not complete, so the orphan
   * branch covers it: the delivery itself resumes the session with the new entry folded in. Callers
   * on a request path typically leave this promise unawaited.
   */
  public async deliver(options: DeliverOptions): Promise<OrchestratorResult | null> {
    const { sessionId, namespaceId, flow, dataPoint } = options;
    const reopenIfComplete = options.reopenIfComplete ?? false;
    return await withSpan(
      this.runtime.telemetry.tracer,
      'session.deliver',
      async (span) => {
        await this.runtime.inbox.append(sessionId, dataPoint);
        getLogger().debug('DataPoint delivered to the session inbox', {
          [LOGGER_NAME_FIELD]: LOGGER_NAME,
          session_id: sessionId,
          data_point_type: dataPoint.type,
        });
        if (await this.runtime.lock.isComplete(sessionId)) {
          if (reopenIfComplete) {
            // The caller declares this late data affects the durable result (e.g. the participant
            // endpoint delivering NAME / WORK_EMAIL): re-open so the aggregator re-runs and folds it
            // in. reopen is a free-lock spawn — it stands down if the lock is still held, because a
            // completing holder drains its own inbox before releasing the epoch, so a deliver
            // landing in that window is folded in-run without any deliver-side recheck.
            span.setAttribute('disposition', 'reopened_for_late_data');
            return await this.reopen({ sessionId, namespaceId, flow });
          }
          // No re-open requested: the entry is durably appended but a completed session will not be
          // re-driven for it, so it expires unprocessed.
          span.setAttribute('disposition', 'already_complete_no_reopen');
          getLogger().info(
            'Delivery to a completed session without a re-open request; the entry will expire unprocessed',
            {
              [LOGGER_NAME_FIELD]: LOGGER_NAME,
              session_id: sessionId,
              data_point_type: dataPoint.type,
            },
          );
          return null;
        }
        if (await this.runtime.lock.isHeld(sessionId)) {
          span.setAttribute('disposition', 'owner_notified');
          getLogger().debug('Delivery left to the live session owner; a redelivery recheck is scheduled', {
            [LOGGER_NAME_FIELD]: LOGGER_NAME,
            session_id: sessionId,
            recheck_ms: this.redeliveryRecheckMs,
          });
          // Carry the caller's re-open intent into the deferred re-drive. By the time it fires the
          // holder may have COMPLETED rather than died, and `resume` refuses a completed session —
          // so redriving with `resume` alone silently drops an entry the caller was told was
          // delivered.
          this.scheduleRecheck({
            sessionId,
            namespaceId,
            flow,
            redrive: reopenIfComplete
              ? async (redriveOptions) => await this.reopenOrResume(redriveOptions)
              : async (redriveOptions) => await this.resume(redriveOptions),
          });
          return null;
        }
        span.setAttribute('disposition', 'resumed');
        return await this.resume({ sessionId, namespaceId, flow });
      },
      {
        attributes: {
          session_id: sessionId,
          namespace_id: namespaceId,
          flow_name: flow.name,
          data_point_type: dataPoint.type,
        },
      },
    );
  }

  /**
   * Re-drive a session whose late data affects the durable result, whichever state it reached.
   *
   * The deferred recheck is scheduled for a holder that may have died — but by the time it fires the
   * holder may instead have finished, and `resume` deliberately stands down on a COMPLETED session.
   * A caller that passed `reopenIfComplete` has declared its entry changes the durable result, so
   * the completed case has to re-open rather than stand down: otherwise the entry is dropped after
   * the caller was told it was delivered, and every stand-down on that path logs at DEBUG.
   */
  private async reopenOrResume(options: ResumeOptions): Promise<OrchestratorResult | null> {
    if (await this.runtime.lock.isComplete(options.sessionId)) {
      return await this.reopen(options);
    }
    return await this.resume(options);
  }

  /**
   * Schedule one deferred re-drive after the lease TTL, covering the dead-holder delivery race.
   *
   * With `redrive = resume` the holder seen by deliver may have died (or parked) right after the
   * append, before processing it; a resume in that window re-drives the message (a no-op if the
   * session completed or another owner took over; a racing second resumer loses the acquire and
   * stands down). A *completing* holder needs no recheck here: it drains its own inbox before
   * releasing the epoch, so a deliver landing in the completion window is folded in-run; the
   * manager's post-run backstop covers the tiny non-atomic check→release gap.
   *
   * @internal Called by `deliver`; `protected` so a test can drive one recheck directly.
   */
  protected scheduleRecheck(options: ResumeOptions & { readonly redrive: Redrive }): void {
    const { sessionId, namespaceId, flow, redrive } = options;
    const recheck = async (): Promise<void> => {
      await this.runtime.clock.sleep(this.redeliveryRecheckMs);
      try {
        await redrive({ sessionId, namespaceId, flow });
      } catch (error) {
        // Fire-and-forget: nothing awaits this task, so a raised error would vanish. Surface it;
        // the host-level periodic re-drive remains the backstop.
        getLogger().error("Deferred recheck failed; the host's periodic re-drive must recover this session", {
          [LOGGER_NAME_FIELD]: LOGGER_NAME,
          session_id: sessionId,
          namespace_id: namespaceId,
          error,
        });
      }
    };
    const task = recheck();
    this.recheckTasks.add(task);
    // The bookkeeping chain is detached, so it swallows anything the recheck itself could not (only
    // an injected clock whose sleep rejects): an unhandled rejection here would take the process
    // down, where Python merely logs the task's never-retrieved exception. A caller awaiting
    // `recheckTasks` still sees the original failure, because it awaits `task`, not this chain.
    void task
      .finally(() => {
        this.recheckTasks.delete(task);
      })
      .catch(() => undefined);
  }

  /**
   * Spawn an orchestrator run and backstop the drain handoff at its non-atomic check→release gap.
   *
   * The orchestrator drains its own inbox before releasing the epoch (it owns the inbox while it
   * holds it), but the check→release window is non-atomic and spans two separate ops: the
   * orchestrator reads the inbox while it still holds the epoch, so a cross-pod deliver appending a
   * straggler after that read yet before the release is structurally invisible to it. This wrapper
   * reads `pendingCount` AFTER the run returns (i.e. after the orchestrator released), so it
   * observes exactly those gap-landing stragglers. The manager holds no epoch, so only it can then
   * re-drive: it re-spawns (fenced with a fresh, higher epoch — exactly like a reopen) to fold the
   * straggler, and each re-spawn drains the inbox, so the loop terminates once `pendingCount` hits
   * 0. Bounded by {@link MAX_BACKSTOP_RESPAWNS} so an adversarial burst of gap-landing delivers
   * cannot spin the manager; the common case is zero backstop re-spawns.
   *
   * Residual: a pod crash in the microscopic [release, manager-probe] window is a double-failure
   * that needs a host-level reopen-capable re-drive scanner (out of scope here); a deliver landing
   * AFTER the probe is already covered by `deliver` spawning on a free lock (reopen on a free lock).
   *
   * Shared by every spawn site (startSession / resume / reopen) so the handoff is backstopped
   * uniformly.
   *
   * @internal `protected` so a test can stand in for the whole spawn, as Python's monkeypatch does.
   */
  protected async spawnAndDrain(
    sessionId: SessionId,
    namespaceId: NamespaceId,
    flow: FlowDefinition,
    seed: readonly AnyDataPoint[],
    freshDeadline = false,
  ): Promise<OrchestratorResult> {
    let result = await this.spawn(sessionId, namespaceId, flow, seed, freshDeadline);
    for (let attempt = 0; attempt < MAX_BACKSTOP_RESPAWNS; attempt += 1) {
      if (result.status !== SessionStatus.COMPLETED || (await this.runtime.inbox.pendingCount(sessionId)) === 0) {
        return result;
      }
      getLogger().debug(
        'Straggler observed in the inbox after the completing run released; backstop re-spawning to drain',
        {
          [LOGGER_NAME_FIELD]: LOGGER_NAME,
          session_id: sessionId,
          namespace_id: namespaceId,
          epoch: result.epoch,
        },
      );
      try {
        result = await this.respawnToDrain(sessionId, namespaceId, flow);
      } catch (error) {
        if (!(error instanceof LockHeldError)) {
          throw error;
        }
        // A concurrent consumer (a racing deliver-driven reopen) grabbed the lock first; its higher
        // epoch fences us and it will drain the straggler. Standing down is correct — same contract
        // as resume/reopen's LockHeldError. Return the completed result unchanged.
        getLogger().debug('Backstop re-spawn lost the acquire race; the concurrent owner drains the straggler', {
          [LOGGER_NAME_FIELD]: LOGGER_NAME,
          session_id: sessionId,
          namespace_id: namespaceId,
        });
        return result;
      }
    }
    return result;
  }

  /**
   * Fenced free-lock re-spawn of a just-completed session to fold a gap-landing straggler.
   *
   * Mirrors {@link SessionOrchestrationManager.reopen}'s clear steps (clear the completion flag so
   * the re-spawn can acquire, clear each aggregator's contribution so it re-runs and folds the new
   * data) then spawns with a fresh deadline under a strictly higher epoch. Calls the raw `spawn` —
   * not `spawnAndDrain` — because the enclosing loop already owns the drain-until-clean bound.
   */
  private async respawnToDrain(
    sessionId: SessionId,
    namespaceId: NamespaceId,
    flow: FlowDefinition,
  ): Promise<OrchestratorResult> {
    await this.runtime.lock.clearComplete(sessionId);
    await this.clearAggregatorContributions(sessionId, flow);
    return await this.spawn(sessionId, namespaceId, flow, [], true);
  }

  /** Clear every aggregator's durable "ran" marker, so the next spawn re-runs the aggregation phase. */
  private async clearAggregatorContributions(sessionId: SessionId, flow: FlowDefinition): Promise<void> {
    for (const operator of flow.operators) {
      if (isSubclass(operator, Aggregator)) {
        await this.runtime.durable.clearContribution(sessionId, operator.operatorId);
      }
    }
  }

  private async spawn(
    sessionId: SessionId,
    namespaceId: NamespaceId,
    flow: FlowDefinition,
    seed: readonly AnyDataPoint[],
    freshDeadline = false,
  ): Promise<OrchestratorResult> {
    const orchestrator = new Orchestrator({
      sessionId,
      namespaceId,
      runtime: this.runtime,
      // A flow names the classes to RUN, so they are constructible; the declaration-only class type
      // the flow stores is the one the graph functions read declarations off.
      operators: flow.operators as readonly ConcreteOperatorClass[],
      capabilities: flow.capabilities as readonly ConcreteCapabilityClass[],
      seed,
      completesWhen: flow.completesWhen,
      retryPolicy: flow.retryPolicy,
      parkAfterMs: flow.parkAfterMs,
      // Flow-level tuning rides through as-is: null means "use the orchestrator default".
      operationTimeoutMs: flow.operationTimeoutMs,
      sessionDeadlineMs: flow.sessionDeadlineMs,
      maxInboxDeliveries: flow.maxInboxDeliveries,
      emissionQueueSize: flow.emissionQueueSize,
      flowIdentity: flow.identity(),
      freshDeadline,
    });
    return await orchestrator.run();
  }
}
