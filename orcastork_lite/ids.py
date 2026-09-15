"""Framework-owned identity types.

Light ``NewType`` wrappers over ``str`` so the core has no domain coupling; the embedding
application maps its own ids (a request, a job, a tenant) onto these.
"""

from typing import NewType

# Identity of an operator / capability; also a DataPoint's provenance (``retrieved_by``).
OperatorId = NewType('OperatorId', str)
CapabilityId = NewType('CapabilityId', str)

# One run of a flow.
SessionId = NewType('SessionId', str)

# The scope a session belongs to. The framework never interprets it: it hands the value back to
# the ``CapabilityCatalog`` when asking what this scope may run. What it partitions — a customer,
# a team, a region — is the embedding application's choice; one constant is fine.
NamespaceId = NewType('NamespaceId', str)
