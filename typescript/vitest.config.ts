import { defineConfig } from 'vitest/config';

/**
 * The default suite: everything under `tests/`, minus the integration files.
 *
 * It needs no service the setup cannot start itself, which is what keeps `npm test` the command a
 * contributor runs without reading anything first.
 */
export default defineConfig({
  test: {
    include: ['tests/**/*.test.ts'],
    exclude: ['**/node_modules/**', '**/dist/**', 'tests/**/*.integration.test.ts'],
    setupFiles: ['tests/setup.ts'],
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts'],
      reporter: ['text', 'html'],
      reportsDirectory: 'coverage',
    },
  },
});
