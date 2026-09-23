/**
 * Adapters — the only layer that may import infrastructure SDKs (`redis`, `mongodb`).
 *
 * Only the dependency-free family is re-exported here. The Redis and Mongo backends live behind
 * optional peer dependencies, so they are imported from their own modules: reaching for this barrel
 * never drags a package a deployment did not install into the module graph.
 *
 * @module
 */

export * from './memory/index.js';
