#!/usr/bin/env node
/**
 * The `orcastork-graph` console script: argv in, exit code out.
 *
 * Kept apart from `graph.ts` so the tool's logic stays importable (and testable) without a shebang,
 * a process or an exit code in the way — `main` returns the code, this file is the only place that
 * hands it to the process.
 *
 * @module
 */

import process from 'node:process';
import { main } from './graph.js';

// Setting `exitCode` rather than calling `process.exit()`: a pending stdout write to a pipe is
// flushed before the process leaves, so a redirected Mermaid render is never truncated.
process.exitCode = await main(process.argv.slice(2));
