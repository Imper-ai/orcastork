/**
 * Backends for the ports orcastork_lite declares — currently the `SessionEventSink`.
 *
 * Infrastructure SDKs are imported here and nowhere else; the core depends on the interfaces only.
 *
 * Only the dependency-free backends are re-exported. A backend built on an optional peer —
 * `RedisSessionEventSink` in `./redis.js`, which needs the `redis` package — is imported from its
 * own module, so reaching for this barrel never drags a package a deployment did not install into
 * the module graph.
 *
 * @module
 */

export { InMemorySessionEventSink } from './memory.js';
