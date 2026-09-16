/**
 * HRN — test-harness & architectural-boundary guards.
 *
 * The import-boundary tests are the executable form of the port's hardest rules: the package
 * imports nothing beyond the dependencies it declares, the infrastructure SDKs are confined to
 * `adapters/`, the two packages never reach into each other, and the graph CLIs are never
 * imported by the library. All four scan source **text** — nothing here imports a module under
 * `src/`, so the guards run even when an optional backend is not installed and cannot be defeated
 * by a module that fails to load.
 *
 * The dependency guard reads the allowed set out of `package.json` rather than repeating it, so
 * adding a runtime dependency there is the only way to widen it, and adding an import without
 * declaring it fails here instead of at a consumer's install.
 */

import { readdirSync, readFileSync } from 'node:fs';
import { builtinModules } from 'node:module';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

/** The two packages in the distribution are held to the same boundaries. */
const PACKAGE_DIRECTORIES = ['orcastork', 'orcastork_lite'] as const;

/**
 * Infrastructure SDKs may only be imported under `adapters/` (the ports/adapters boundary).
 *
 * `@opentelemetry/*` is deliberately absent: the OTel API is the framework's built-in telemetry
 * standard (a core dependency that no-ops without an SDK), not a backend behind a port.
 */
const INFRA_SDKS = ['redis', 'mongodb', 'bson'] as const;

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const sourceRoot = join(repositoryRoot, 'src');

/** The fields of `package.json` this file reads. */
interface Manifest {
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly peerDependencies?: Readonly<Record<string, string>>;
}

/** One module specifier, and where it was written. */
interface Import {
  readonly file: string;
  readonly specifier: string;
}

const manifest = JSON.parse(readFileSync(join(repositoryRoot, 'package.json'), 'utf8')) as Manifest;

/**
 * Package roots the source is allowed to reach for: its declared runtime dependencies, its
 * optional peers (only legal under `adapters/`, which the next guard enforces), and Node builtins.
 */
const allowedPackages = new Set<string>([
  ...Object.keys(manifest.dependencies ?? {}),
  ...Object.keys(manifest.peerDependencies ?? {}),
  ...builtinModules,
]);

const sourceFiles = (...segments: string[]): readonly string[] => {
  const root = join(sourceRoot, ...segments);
  return readdirSync(root, { recursive: true, withFileTypes: true })
    .filter((entry) => entry.isFile() && entry.name.endsWith('.ts'))
    .map((entry) => join(entry.parentPath, entry.name))
    .sort();
};

/**
 * Every module specifier in one source file.
 *
 * Text, not the module graph: `import ... from 'x'`, `export ... from 'x'`, a bare side-effect
 * `import 'x'`, a dynamic `import('x')` and a `require('x')` all reach the same table.
 */
const importsOf = (file: string): readonly Import[] => {
  const source = readFileSync(file, 'utf8');
  const patterns = [
    /\bfrom\s*['"]([^'"]+)['"]/g,
    /^\s*import\s*['"]([^'"]+)['"]/gm,
    /\bimport\s*\(\s*['"]([^'"]+)['"]\s*\)/g,
    /\brequire\s*\(\s*['"]([^'"]+)['"]\s*\)/g,
  ];
  const specifiers = new Set<string>();
  for (const pattern of patterns) {
    for (const match of source.matchAll(pattern)) {
      const specifier = match[1];
      if (specifier !== undefined) {
        specifiers.add(specifier);
      }
    }
  }
  return [...specifiers].map((specifier) => ({ file: relative(repositoryRoot, file), specifier }));
};

const allImports = (...segments: string[]): readonly Import[] =>
  sourceFiles(...segments).flatMap((file) => importsOf(file));

const isRelative = (specifier: string): boolean => specifier.startsWith('.') || specifier.startsWith('/');

/** The installable package a bare specifier belongs to: `@scope/name`, or the leading segment. */
const packageOf = (specifier: string): string => {
  const bare = specifier.startsWith('node:') ? specifier.slice('node:'.length) : specifier;
  const segments = bare.split('/');
  if (bare.startsWith('@')) {
    return segments.slice(0, 2).join('/');
  }
  return segments[0] ?? bare;
};

/** Where a relative specifier lands, as a path relative to `src/` and with forward slashes. */
const resolvedWithin = (entry: Import): string =>
  relative(sourceRoot, resolve(repositoryRoot, dirname(entry.file), entry.specifier)).replaceAll('\\', '/');

const describeImport = (entry: Import): string => `${entry.file} -> ${entry.specifier}`;

describe('the harness itself', () => {
  it('finds source to scan, so a broken walk cannot pass every other guard vacuously', () => {
    expect(sourceFiles('orcastork').length).toBeGreaterThan(0);
    expect(sourceFiles('orcastork_lite').length).toBeGreaterThan(0);
    expect(allImports('orcastork').length).toBeGreaterThan(0);
  });
});

describe.each(PACKAGE_DIRECTORIES)('%s', (packageDirectory) => {
  it('imports no package that package.json does not declare', () => {
    const offenders = allImports(packageDirectory)
      .filter((entry) => !isRelative(entry.specifier) && !allowedPackages.has(packageOf(entry.specifier)))
      .map(describeImport);

    expect(offenders).toEqual([]);
  });

  it('confines the infrastructure SDKs to adapters/', () => {
    const infra = new Set<string>(INFRA_SDKS);
    const offenders = allImports(packageDirectory)
      .filter((entry) => infra.has(packageOf(entry.specifier)))
      .filter((entry) => !relative(repositoryRoot, entry.file).startsWith(join('src', packageDirectory, 'adapters')))
      .map(describeImport);

    expect(offenders).toEqual([]);
  });

  it('never imports the other package', () => {
    const other = PACKAGE_DIRECTORIES.find((name) => name !== packageDirectory) as string;
    const offenders = allImports(packageDirectory)
      .filter((entry) => (isRelative(entry.specifier) ? resolvedWithin(entry).startsWith(`${other}/`) : false))
      .map(describeImport);

    expect(offenders).toEqual([]);
  });

  it('never imports the graph CLI under tools/', () => {
    const offenders = allImports(packageDirectory)
      .filter((entry) => {
        const target = isRelative(entry.specifier) ? resolvedWithin(entry) : entry.specifier;
        return target.split('/').includes('tools');
      })
      .map(describeImport);

    expect(offenders).toEqual([]);
  });
});
