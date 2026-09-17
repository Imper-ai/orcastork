/**
 * The `orcastork-graph` CLI: registry graph build, cycle-policy check, Mermaid rendering.
 *
 * Mirrors Python's `orcastork/tools/__init__.py`, which is empty: nothing in the library imports
 * this subpackage, and a test enforces it. The barrel exists so the tool's own modules have one
 * entry point, not so the library can reach for it.
 *
 * @module
 */

export type { CliOutput, GraphCheck } from './graph.js';
export { checkGraph, main, processOutput, renderMermaid } from './graph.js';
