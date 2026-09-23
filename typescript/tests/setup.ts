/**
 * Registry isolation for every test.
 *
 * Registering a DataPoint type, an operator or a capability is a side effect of *defining the
 * class*, so a test that declares a throwaway type leaks it into every test that runs after it —
 * and into the duplicate-key check, which would then fail for the wrong reason. Snapshotting the
 * registries before each test and restoring them after is the executable form of the Python
 * package's autouse fixture, and it covers registries that do not exist yet: everything built on
 * `Registry` joins the process-wide list on construction.
 *
 * @module
 */

import { afterEach, beforeEach } from 'vitest';
import type { RegistriesSnapshot } from '../src/orcastork/internal/registry.js';
import { restoreAllRegistries, snapshotAllRegistries } from '../src/orcastork/internal/registry.js';

let taken: RegistriesSnapshot | undefined;

beforeEach(() => {
  taken = snapshotAllRegistries();
});

afterEach(() => {
  if (taken !== undefined) {
    restoreAllRegistries(taken);
    taken = undefined;
  }
});
