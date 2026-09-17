/**
 * Exception hierarchy — every known error case has a type rooted at {@link OrcastorkLiteError}.
 *
 * @module
 */

/** Base class for every orcastork_lite error. */
export class OrcastorkLiteError extends Error {
  public constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    // Taken from the concrete constructor rather than hard-coded per subclass, so a subclass can
    // never drift from its own name and `error.name` always identifies what was actually thrown.
    this.name = new.target.name;
  }
}

/** Two operators (or two capabilities) handed to one orchestrator share an id. */
export class DuplicateIdError extends OrcastorkLiteError {}

/** A concrete operator definition is missing its scheduling `policy`. */
export class InvalidOperatorError extends OrcastorkLiteError {}

/** The dependency graph contains a cycle whose operators lack a `maxCycles` cap. */
export class UnboundedCycleError extends OrcastorkLiteError {}

/** `require` was called for a capability type with no available provider. */
export class CapabilityUnavailableError extends OrcastorkLiteError {}

/** A DataPoint value could not be reduced to a hashable identity. */
export class UnhashableValueError extends OrcastorkLiteError {}
