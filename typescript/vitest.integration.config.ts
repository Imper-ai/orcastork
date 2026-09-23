import { defineConfig } from 'vitest/config';

/**
 * The integration suite: only `*.integration.test.ts`.
 *
 * Kept out of `npm test` because these are the slow ones (a large fixture, a server behaviour no
 * double can emulate); CI runs them as their own job, exactly as `pytest -m integration` does in
 * the Python package. Standalone rather than a merge over `vitest.config.ts`: merging concatenates
 * `include` and `exclude` instead of replacing them, which would quietly run the default suite
 * here and the integration files nowhere.
 */
export default defineConfig({
  test: {
    include: ['tests/**/*.integration.test.ts'],
    exclude: ['**/node_modules/**', '**/dist/**'],
    setupFiles: ['tests/setup.ts'],
    // The suite is empty until the adapters land, and an empty suite is not a failure here: the
    // job exists so that the first integration test is run by CI on the day it is written.
    passWithNoTests: true,
  },
});
