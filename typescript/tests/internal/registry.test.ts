import { describe, expect, it } from 'vitest';
import { DuplicateRegistrationError } from '../../src/orcastork/exceptions.js';
import {
  Registry,
  resetAllRegistries,
  restoreAllRegistries,
  snapshotAllRegistries,
} from '../../src/orcastork/internal/registry.js';

const raise = (key: string): void => {
  throw new DuplicateRegistrationError(`'${key}' is already registered`);
};

describe('Registry', () => {
  it('stores and reads back values by key', () => {
    const registry = new Registry<string, number>('numbers');
    registry.set('one', 1);

    expect(registry.get('one')).toBe(1);
    expect(registry.has('one')).toBe(true);
    expect(registry.has('two')).toBe(false);
    expect(registry.get('two')).toBeUndefined();
    expect(registry.size).toBe(1);
  });

  it('lets the caller decide what a duplicate key means', () => {
    const registry = new Registry<string, number>('numbers');
    registry.set('one', 1, raise);

    expect(() => registry.set('one', 2, raise)).toThrow(DuplicateRegistrationError);
    expect(registry.get('one')).toBe(1);
  });

  it('overwrites silently when no duplicate policy is given', () => {
    const registry = new Registry<string, number>('numbers');
    registry.set('one', 1);
    registry.set('one', 2);

    expect(registry.get('one')).toBe(2);
  });

  it('reports keys and values in registration order', () => {
    const registry = new Registry<string, number>('numbers');
    registry.set('b', 2);
    registry.set('a', 1);

    expect(registry.keys()).toEqual(['b', 'a']);
    expect(registry.values()).toEqual([2, 1]);
  });

  it('removes a key and says whether anything was there', () => {
    const registry = new Registry<string, number>('numbers');
    registry.set('one', 1);

    expect(registry.delete('one')).toBe(true);
    expect(registry.delete('one')).toBe(false);
    expect(registry.size).toBe(0);
  });

  it('bumps its version on every mutation, so a derived structure can rebuild lazily', () => {
    const registry = new Registry<string, number>('numbers');
    const initial = registry.version;

    registry.set('one', 1);
    const afterSet = registry.version;
    expect(afterSet).toBeGreaterThan(initial);

    expect(registry.get('one')).toBe(1);
    expect(registry.version).toBe(afterSet);

    registry.delete('one');
    expect(registry.version).toBeGreaterThan(afterSet);
  });

  it('does not bump its version when a delete removed nothing', () => {
    const registry = new Registry<string, number>('numbers');
    const before = registry.version;

    registry.delete('absent');

    expect(registry.version).toBe(before);
  });

  it('snapshots and restores its contents', () => {
    const registry = new Registry<string, number>('numbers');
    registry.set('one', 1);
    const snapshot = registry.snapshot();

    registry.set('two', 2);
    registry.delete('one');
    registry.restore(snapshot);

    expect(registry.keys()).toEqual(['one']);
    expect(registry.get('two')).toBeUndefined();
  });

  it('hands out a detached snapshot, so later mutations cannot rewrite it', () => {
    const registry = new Registry<string, number>('numbers');
    registry.set('one', 1);
    const snapshot = registry.snapshot();

    registry.set('two', 2);

    expect(snapshot.size).toBe(1);
  });
});

describe('the process-wide registry list', () => {
  it('restores every registry at once, which is what isolates one test from the next', () => {
    const first = new Registry<string, number>('first');
    const second = new Registry<string, number>('second');
    first.set('kept', 1);
    const snapshot = snapshotAllRegistries();

    first.set('leaked', 2);
    second.set('leaked', 3);
    restoreAllRegistries(snapshot);

    expect(first.keys()).toEqual(['kept']);
    expect(second.size).toBe(0);
  });

  it('clears a registry that did not exist when the snapshot was taken', () => {
    const snapshot = snapshotAllRegistries();
    const late = new Registry<string, number>('late');
    late.set('defined-by-the-test', 1);

    restoreAllRegistries(snapshot);

    expect(late.size).toBe(0);
  });

  it('empties everything on demand', () => {
    const registry = new Registry<string, number>('numbers');
    registry.set('one', 1);

    resetAllRegistries();

    expect(registry.size).toBe(0);
  });
});
