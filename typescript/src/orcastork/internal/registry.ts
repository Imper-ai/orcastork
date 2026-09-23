/**
 * The registry every class registry is built from.
 *
 * Python registers DataPoint leaves, operators and capabilities in module-level dicts filled by
 * `__init_subclass__`; the port fills the same tables from class decorators. Both share two
 * problems this module solves once:
 *
 * - **A duplicate key is a definition-time error**, not a silent overwrite. The policy lives with
 *   the caller (which exception, which message), so {@link Registry.set} takes it as a callback
 *   rather than guessing.
 * - **A test must be able to undo registrations.** Registering is a side effect of loading a
 *   module, so a test that defines a throwaway DataPoint pollutes every later test unless the
 *   tables are snapshotted and restored around it — the executable form of the Python autouse
 *   fixture. Every registry constructed anywhere joins a process-wide list precisely so the test
 *   setup can do that without knowing which registries exist.
 *
 * Each registry also carries a version counter, bumped on every mutation, so a derived structure
 * (the discriminated-union parser rebuilt from the DataPoint leaves) can be recomputed lazily and
 * only when something actually changed.
 *
 * @module
 */

/** A registry as the process-wide list sees it, with its key and value types erased. */
export interface ManagedRegistry {
  /** Human-readable name, used only in diagnostics. */
  readonly name: string;

  /** Capture the current contents; the value is opaque to everything but this registry. */
  captureSnapshot(): unknown;

  /** Restore contents captured by {@link ManagedRegistry.captureSnapshot}. */
  applySnapshot(capture: unknown): void;

  /** Drop every entry. */
  clear(): void;
}

/** A point-in-time capture of every registry in the process. */
export interface RegistriesSnapshot {
  readonly captures: ReadonlyMap<ManagedRegistry, unknown>;
}

const allRegistries: ManagedRegistry[] = [];

/**
 * A keyed table of registered classes (or anything else keyed by a stable id).
 *
 * Construction registers it with the process-wide list, so it is snapshotted and restored by the
 * test setup automatically — a registry that opts out of that would be a registry a test cannot
 * isolate.
 */
export class Registry<K, V> implements ManagedRegistry {
  private readonly entries = new Map<K, V>();
  private mutations = 0;

  public constructor(public readonly name: string) {
    allRegistries.push(this);
  }

  /** The registered value for `key`, or `undefined` when nothing is registered under it. */
  public get(key: K): V | undefined {
    return this.entries.get(key);
  }

  /** Whether anything is registered under `key`. */
  public has(key: K): boolean {
    return this.entries.has(key);
  }

  /**
   * Register `value` under `key`.
   *
   * `onDuplicate` is the caller's collision policy: it is invoked with the key and the value
   * already registered under it and is expected to throw (Python raises `DuplicateRegistrationError`
   * at class-definition time). If it returns, the new value replaces the old one. Without it, `set`
   * overwrites silently — the registry itself has no opinion.
   */
  public set(key: K, value: V, onDuplicate?: (key: K, existing: V) => void): void {
    const existing = this.entries.get(key);
    if (existing !== undefined && onDuplicate !== undefined) {
      onDuplicate(key, existing);
    }
    this.entries.set(key, value);
    this.mutations += 1;
  }

  /** Remove `key`; returns whether anything was registered under it. */
  public delete(key: K): boolean {
    const removed = this.entries.delete(key);
    if (removed) {
      this.mutations += 1;
    }
    return removed;
  }

  /** Every registered key, in registration order. */
  public keys(): readonly K[] {
    return [...this.entries.keys()];
  }

  /** Every registered value, in registration order. */
  public values(): readonly V[] {
    return [...this.entries.values()];
  }

  /** How many entries are registered. */
  public get size(): number {
    return this.entries.size;
  }

  /**
   * A counter bumped on every mutation.
   *
   * Something derived from the whole table — the lazily rebuilt DataPoint union parser — compares
   * this against the version it was built at instead of rebuilding on every call.
   */
  public get version(): number {
    return this.mutations;
  }

  /** A detached copy of the current contents. */
  public snapshot(): ReadonlyMap<K, V> {
    return new Map(this.entries);
  }

  /** Replace the contents with a snapshot taken earlier. */
  public restore(snapshot: ReadonlyMap<K, V>): void {
    this.entries.clear();
    for (const [key, value] of snapshot) {
      this.entries.set(key, value);
    }
    this.mutations += 1;
  }

  /** Drop every entry. */
  public clear(): void {
    if (this.entries.size > 0) {
      this.entries.clear();
      this.mutations += 1;
    }
  }

  public captureSnapshot(): unknown {
    return this.snapshot();
  }

  public applySnapshot(capture: unknown): void {
    this.restore(capture as ReadonlyMap<K, V>);
  }
}

/** Every registry constructed in this process, in construction order. */
export const registries = (): readonly ManagedRegistry[] => [...allRegistries];

/** Capture every registry's contents in one value. */
export const snapshotAllRegistries = (): RegistriesSnapshot => {
  const captures = new Map<ManagedRegistry, unknown>();
  for (const registry of allRegistries) {
    captures.set(registry, registry.captureSnapshot());
  }
  return { captures };
};

/**
 * Restore every registry from a snapshot.
 *
 * A registry constructed *after* the snapshot was taken (a module imported by the test itself) is
 * cleared rather than left behind, so the process ends up in exactly the state the snapshot
 * describes.
 */
export const restoreAllRegistries = (snapshot: RegistriesSnapshot): void => {
  for (const registry of allRegistries) {
    const capture = snapshot.captures.get(registry);
    if (capture === undefined) {
      registry.clear();
    } else {
      registry.applySnapshot(capture);
    }
  }
};

/** Empty every registry — the blunt instrument; prefer snapshot/restore around a test. */
export const resetAllRegistries = (): void => {
  for (const registry of allRegistries) {
    registry.clear();
  }
};
