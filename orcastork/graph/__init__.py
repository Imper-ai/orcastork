"""Static dependency graph: builder, Tarjan cycle detection, runtime circuit-breaker."""

from .builder import build_graph, build_uses_edges, restrict_to_permitted
from .circuit_breaker import CircuitBreaker
from .cycles import find_cycles, validate_acyclic_or_bounded
from .reachability import backward_reachable

__all__ = [
    'CircuitBreaker',
    'backward_reachable',
    'build_graph',
    'build_uses_edges',
    'find_cycles',
    'restrict_to_permitted',
    'validate_acyclic_or_bounded',
]
