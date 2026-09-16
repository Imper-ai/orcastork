/**
 * Framework-owned identity types.
 *
 * Light nominal wrappers over `string` so the core has no domain coupling; the embedding
 * application maps its own ids (a request, a job, a tenant) onto these.
 *
 * @module
 */

/**
 * A nominal ("branded") alias of a primitive — the TypeScript stand-in for Python's `NewType`.
 *
 * The brand exists only in the type system: the value is a plain `string` at runtime, so it keys a
 * `Map` and prints exactly as the Python value does, while the compiler still refuses a bare
 * `string` where an id is required.
 */
export type Brand<T, TBrand extends string> = T & { readonly __brand: TBrand };

/** Identity of an operator; also a DataPoint's provenance (`retrievedBy`). */
export type OperatorId = Brand<string, 'OperatorId'>;

/** Brand a raw string as an {@link OperatorId}. */
export const OperatorId = (value: string): OperatorId => value as OperatorId;

/** Identity of a capability. */
export type CapabilityId = Brand<string, 'CapabilityId'>;

/** Brand a raw string as a {@link CapabilityId}. */
export const CapabilityId = (value: string): CapabilityId => value as CapabilityId;

/** One run of a flow. */
export type SessionId = Brand<string, 'SessionId'>;

/** Brand a raw string as a {@link SessionId}. */
export const SessionId = (value: string): SessionId => value as SessionId;

/**
 * The scope a session belongs to.
 *
 * The framework never interprets it: it hands the value back to the `CapabilityCatalog` when
 * asking what this scope may run. What it partitions — a customer, a team, a region — is the
 * embedding application's choice; one constant is fine.
 */
export type NamespaceId = Brand<string, 'NamespaceId'>;

/** Brand a raw string as a {@link NamespaceId}. */
export const NamespaceId = (value: string): NamespaceId => value as NamespaceId;
