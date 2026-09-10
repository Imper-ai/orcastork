"""Exception hierarchy — every known error case has a type rooted at :class:`OrcastorkLiteError`."""


class OrcastorkLiteError(Exception):
    """Base class for every orcastork_lite error."""


class DuplicateIdError(OrcastorkLiteError):
    """Two operators (or two capabilities) handed to one orchestrator share an id."""


class InvalidOperatorError(OrcastorkLiteError):
    """A concrete operator definition is missing its scheduling ``policy``."""


class UnboundedCycleError(OrcastorkLiteError):
    """The dependency graph contains a cycle whose operators lack a ``max_cycles`` cap."""


class CapabilityUnavailableError(OrcastorkLiteError):
    """``require`` was called for a capability type with no available provider."""


class UnhashableValueError(OrcastorkLiteError):
    """A DataPoint value could not be reduced to a hashable identity (see ``datapoints._make_hashable``)."""
