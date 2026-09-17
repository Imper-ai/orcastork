/**
 * PKG — the published manifest is a contract, and this is its executable half.
 *
 * `package.json` decides what a consumer can reach: with an `exports` map in place a deep import
 * of a `dist/` path is refused, so a module that has no subpath here is not part of the package no
 * matter how well it is exported one directory down. The adapters are the case that matters — the
 * root barrel deliberately leaves them out so importing `orcastork` never loads the optional peers
 * (`tests/index.test.ts` pins that), which only works if each adapter family has a subpath of its
 * own to be imported from.
 *
 * Everything here reads text: the manifest, the `LICENSE` file and the source tree. Nothing is
 * built, so the guards run on a clean checkout and fail in `npm test` rather than on someone's
 * `npm install`.
 *
 * @module
 */

import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

/** The fields of `package.json` this file reads. */
interface Manifest {
  readonly name: string;
  readonly version: string;
  readonly description: string;
  readonly keywords: readonly string[];
  readonly license: string;
  readonly homepage: string;
  readonly repository: { readonly type: string; readonly url: string; readonly directory: string };
  readonly bugs: { readonly url: string };
  readonly type: string;
  readonly engines: Readonly<Record<string, string>>;
  readonly sideEffects: boolean;
  readonly main: string;
  readonly types: string;
  readonly exports: Readonly<Record<string, string | Readonly<Record<string, string>>>>;
  readonly bin: Readonly<Record<string, string>>;
  readonly files: readonly string[];
  readonly peerDependencies: Readonly<Record<string, string>>;
}

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repositoryRoot = resolve(packageRoot, '..');
const sourceRoot = join(packageRoot, 'src');

const manifest = JSON.parse(readFileSync(join(packageRoot, 'package.json'), 'utf8')) as Manifest;

/** The conditional subpaths — `./package.json` maps to a plain string and is checked on its own. */
const conditionalEntries = Object.entries(manifest.exports).filter(
  (entry): entry is [string, Readonly<Record<string, string>>] => typeof entry[1] !== 'string',
);

/**
 * The source module a built `dist/` path came from.
 *
 * `tsc` mirrors `src/` into `dist/` one file at a time, so the mapping is the extension and the
 * first path segment: `./dist/orcastork/adapters/memory/index.js` ← `src/orcastork/adapters/memory/index.ts`.
 */
const sourceOf = (distPath: string): string => {
  const withoutPrefix = distPath.replace(/^\.\/dist\//, '');
  const withoutExtension = withoutPrefix.replace(/\.(d\.ts|js)$/, '');
  return join(sourceRoot, `${withoutExtension}.ts`);
};

/** Directories under `src/` holding adapter entry modules, and how an entry is spelled in each. */
const ADAPTER_FAMILIES = [
  { directory: join(sourceRoot, 'orcastork', 'adapters'), entriesAreDirectories: true },
  { directory: join(sourceRoot, 'orcastork_lite', 'adapters'), entriesAreDirectories: false },
] as const;

describe('the published manifest', () => {
  it('declares the identity npm shows on the package page', () => {
    expect(manifest.name).toBe('orcastork');
    expect(manifest.description.length).toBeGreaterThan(0);
    expect(manifest.keywords.length).toBeGreaterThan(0);
    expect(manifest.homepage).toContain('github.com/Imper-ai/orcastork');
    expect(manifest.bugs.url).toContain('github.com/Imper-ai/orcastork');
    // The package lives in a subdirectory of the repository; without this npm links the wrong tree.
    expect(manifest.repository.directory).toBe('typescript');
  });

  it('is ESM on the Node version the toolchain is written against', () => {
    expect(manifest.type).toBe('module');
    expect(manifest.engines.node).toBe('>=22');
  });

  it('keeps registration side effects, so a bundler may not drop a module it thinks is unused', () => {
    // Declaring a DataPoint, Operator or Capability registers it at import time. `sideEffects: false`
    // would license a bundler to elide exactly the module whose import IS the registration.
    expect(manifest.sideEffects).toBe(true);
  });

  it('ships the built tree and nothing else', () => {
    // README and LICENSE are added by npm itself; anything else here would publish sources twice.
    expect(manifest.files).toEqual(['dist']);
  });

  it('carries the licence it claims, verbatim from the repository root', () => {
    expect(manifest.license).toBe('GPL-3.0-or-later');
    const shipped = readFileSync(join(packageRoot, 'LICENSE'), 'utf8');
    expect(shipped).toBe(readFileSync(join(repositoryRoot, 'LICENSE'), 'utf8'));
    expect(shipped).toContain('GNU GENERAL PUBLIC LICENSE');
    expect(shipped).toContain('Version 3, 29 June 2007');
  });

  it('keeps the optional backends optional', () => {
    expect(Object.keys(manifest.peerDependencies).sort()).toEqual(['mongodb', 'redis']);
  });
});

describe('the exports map', () => {
  it('names both entrypoints and every adapter family', () => {
    expect(Object.keys(manifest.exports).sort()).toEqual([
      '.',
      './adapters/memory',
      './adapters/mongo',
      './adapters/redis',
      './lite',
      './lite/adapters/memory',
      './lite/adapters/redis',
      './package.json',
    ]);
  });

  it('resolves every subpath to a module that exists in the source tree', () => {
    for (const [subpath, conditions] of conditionalEntries) {
      for (const target of Object.values(conditions)) {
        expect(target.startsWith('./dist/'), `${subpath} → ${target}`).toBe(true);
        expect(existsSync(sourceOf(target)), `${subpath} → ${sourceOf(target)}`).toBe(true);
      }
    }
  });

  it('puts `types` first, where a TypeScript resolver looks before any runtime condition', () => {
    for (const [subpath, conditions] of conditionalEntries) {
      expect(Object.keys(conditions), subpath).toEqual(['types', 'import']);
      expect(conditions.types, subpath).toMatch(/\.d\.ts$/);
      expect(conditions.import, subpath).toMatch(/\.js$/);
    }
  });

  it('lets a consumer read the manifest, which tooling asks for by subpath', () => {
    expect(manifest.exports['./package.json']).toBe('./package.json');
  });

  it('agrees with the top-level `main`/`types` fallbacks older tooling still reads', () => {
    const root = manifest.exports['.'] as Readonly<Record<string, string>>;
    expect(manifest.main).toBe(root.import);
    expect(manifest.types).toBe(root.types);
  });

  it('gives every adapter entry module in the source tree a subpath to be imported from', () => {
    // An adapter the map forgets is unreachable: the root barrel leaves the adapters out on
    // purpose, and an `exports` map refuses the deep `dist/` import that would otherwise reach it.
    const exported = new Set(
      conditionalEntries.flatMap(([, conditions]) => {
        const target = conditions.import;
        return target === undefined ? [] : [sourceOf(target)];
      }),
    );
    for (const family of ADAPTER_FAMILIES) {
      for (const child of readdirSync(family.directory, { withFileTypes: true })) {
        if (family.entriesAreDirectories) {
          if (!child.isDirectory()) {
            continue;
          }
          expect(exported, child.name).toContain(join(family.directory, child.name, 'index.ts'));
        } else {
          // The family's own barrel re-exports only the dependency-free backends; it is not an entry.
          if (!child.isFile() || child.name === 'index.ts') {
            continue;
          }
          expect(exported, child.name).toContain(join(family.directory, child.name));
        }
      }
    }
  });
});

describe('the console scripts', () => {
  it('point at built CLI modules that exist in the source tree', () => {
    expect(Object.keys(manifest.bin).sort()).toEqual(['orcastork-graph', 'orcastork-lite-graph']);
    for (const target of Object.values(manifest.bin)) {
      expect(existsSync(sourceOf(target)), target).toBe(true);
    }
  });

  it('carry the shebang that makes them runnable once npm links them onto PATH', () => {
    for (const target of Object.values(manifest.bin)) {
      expect(readFileSync(sourceOf(target), 'utf8').startsWith('#!/usr/bin/env node'), target).toBe(true);
    }
  });
});
