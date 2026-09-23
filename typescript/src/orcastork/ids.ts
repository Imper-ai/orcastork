/**
 * Framework-owned identity types.
 *
 * The framework defines its own light identifiers so the core has no domain coupling.
 * Flows and adapters map their own ids onto these.
 *
 * The DataPoint discriminator is an open `string` (each concrete leaf pins its own literal)
 * rather than a closed enum, because the set of DataPoint types is supplied by flows, not by
 * the framework.
 *
 * @module
 */

import { randomUUID } from 'node:crypto';

/**
 * A nominal ("branded") alias of a primitive — the TypeScript stand-in for Python's `NewType`.
 *
 * The brand exists only in the type system: a `SessionId` is a plain `string` at runtime, so it
 * serializes, keys a `Map` and crosses the wire exactly as the Python value does, while the
 * compiler still refuses a bare `string` where an id is required.
 */
export type Brand<T, TBrand extends string> = T & { readonly __brand: TBrand };

/**
 * Discriminator value for a DataPoint leaf.
 *
 * A plain `string` alias (not a brand) so concrete leaves can pin it with a string literal type.
 * The set of values is open and flow-defined.
 */
export type DataPointType = string;

/** Registry key + provenance identity of an operator. */
export type OperatorId = Brand<string, 'OperatorId'>;

/** Brand a raw string as an {@link OperatorId}. */
export const OperatorId = (value: string): OperatorId => value as OperatorId;

/** Registry key + provenance identity of a capability. */
export type CapabilityId = Brand<string, 'CapabilityId'>;

/** Brand a raw string as a {@link CapabilityId}. */
export const CapabilityId = (value: string): CapabilityId => value as CapabilityId;

/** Which operator produced a DataPoint (`DataPoint.retrievedBy`). */
export type OperatorRef = OperatorId;

/** Brand a raw string as an {@link OperatorRef}. */
export const OperatorRef = OperatorId;

/** One run of a flow. A flow maps its own id (a request, a job, a conversation) onto this. */
export type SessionId = Brand<string, 'SessionId'>;

/** Brand a raw string as a {@link SessionId}. */
export const SessionId = (value: string): SessionId => value as SessionId;

/**
 * The scope a session belongs to.
 *
 * The framework never interprets it: it hands the value back to the `CapabilityCatalog` when
 * asking what this scope may run, to the cipher provider when asking which key seals its data,
 * and attaches it to audit records and telemetry. What it partitions — a customer, a team, a
 * region, a deployment — is entirely the embedding flow's choice, and a flow that needs no
 * partitioning can pass one constant.
 */
export type NamespaceId = Brand<string, 'NamespaceId'>;

/** Brand a raw string as a {@link NamespaceId}. */
export const NamespaceId = (value: string): NamespaceId => value as NamespaceId;

/** Fencing token: the value that makes a write correct, not merely permitted. */
export type Epoch = Brand<number, 'Epoch'>;

/** Brand a raw integer as an {@link Epoch}. */
export const Epoch = (value: number): Epoch => value as Epoch;

/** Store change counter, used to compute deltas between two reads of a session. */
export type Revision = Brand<number, 'Revision'>;

/** Brand a raw integer as a {@link Revision}. */
export const Revision = (value: number): Revision => value as Revision;

/** Mint a fresh, opaque session id. */
export const newSessionId = (): SessionId => SessionId(randomUUID());
