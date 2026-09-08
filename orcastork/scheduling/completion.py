"""Declarative session-completion conditions.

A flow that expects mid-session input declares *when the session is complete* as a tiny
AST of frozen dataclasses — :class:`TypePresent` (a DataPoint type is in the session,
subtype-aware like readiness) combined with :class:`AllOf` / :class:`AnyOf` — instead of
a predicate function. Deliberately **no callables**: an AST can be inspected, compared
and later serialized (a parked session resumed on another pod, CI tooling that lints
flow definitions), where an opaque ``Callable`` could only ever be executed.

Empty combinators follow the conventional identities: ``AllOf(())`` is satisfied (the
empty conjunction is true) and ``AnyOf(())`` is not (the empty disjunction is false).

The public ``completes_when`` parameter stays backwards compatible — a bare DataPoint
type is shorthand for :class:`TypePresent` and is normalized in exactly one place,
:func:`normalize_completion`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from ..datapoints import BaseDataPoint, DataPointView
from ..exceptions import InvalidCompletionConditionError


@runtime_checkable
class CompletionCondition(Protocol):
    """The condition contract: a pure, side-effect-free check over the session's view."""

    def is_satisfied(self, view: DataPointView) -> bool:
        """Whether the session's current DataPoints satisfy this condition."""
        ...


@dataclass(frozen=True)
class TypePresent:
    """Satisfied when at least one DataPoint of ``data_point_type`` is present.

    Subtype-aware, consistent with operator readiness: a present leaf satisfies a
    base-type condition.
    """

    data_point_type: type[BaseDataPoint[Any]]

    def is_satisfied(self, view: DataPointView) -> bool:
        return bool(view.of_type(self.data_point_type))

    def describe(self) -> str:
        # The class NAME, never the class object: the frozen-dataclass repr would embed the
        # `<class 'module.X'>` form, which is noisier and easy to confuse with an instance repr.
        return f'TypePresent({self.data_point_type.__qualname__})'


@dataclass(frozen=True)
class AllOf:
    """Satisfied when every child is satisfied; the empty conjunction is satisfied."""

    children: tuple[CompletionCondition, ...]

    def is_satisfied(self, view: DataPointView) -> bool:
        return all(child.is_satisfied(view) for child in self.children)

    def describe(self) -> str:
        return f'AllOf({", ".join(describe_condition(child) for child in self.children)})'


@dataclass(frozen=True)
class AnyOf:
    """Satisfied when at least one child is satisfied; the empty disjunction is not."""

    children: tuple[CompletionCondition, ...]

    def is_satisfied(self, view: DataPointView) -> bool:
        return any(child.is_satisfied(view) for child in self.children)

    def describe(self) -> str:
        return f'AnyOf({", ".join(describe_condition(child) for child in self.children)})'


def describe_condition(condition: CompletionCondition) -> str:
    """A deterministic, process-stable textual form of a condition (flow fingerprinting).

    The AST nodes self-describe via ``describe()``; a custom condition without one falls back
    to its class identity. Both forms are built from qualified names only — never ``id()`` or
    a default object repr — so the text is identical across processes and deploys.
    """
    describe = getattr(condition, 'describe', None)
    if callable(describe):
        return str(describe())
    return f'{type(condition).__module__}.{type(condition).__qualname__}'


def referenced_types(condition: CompletionCondition | None) -> frozenset[type[BaseDataPoint[Any]]] | None:
    """The DataPoint types a completion condition keys on (subtype-aware presence checks).

    Returns ``None`` for an opaque custom ``CompletionCondition`` the AST can't introspect — callers
    that prune the operator graph must treat ``None`` as "completion needs unknown types" and decline
    to prune, so a completion producer is never dropped. ``None`` input (no completion) → empty set.
    """
    if condition is None:
        return frozenset()
    if isinstance(condition, TypePresent):
        return frozenset({condition.data_point_type})
    if isinstance(condition, (AllOf, AnyOf)):
        children = [referenced_types(child) for child in condition.children]
        present = [child for child in children if child is not None]
        if len(present) != len(children):
            return None  # a child was opaque → the whole condition's types are unknown
        return frozenset[type[BaseDataPoint[Any]]]().union(*present)
    return None  # an opaque custom condition — types cannot be determined


def all_of(*items: type[BaseDataPoint[Any]] | CompletionCondition) -> AllOf:
    """Combine conditions conjunctively; a bare DataPoint type is shorthand for ``TypePresent``."""
    return AllOf(tuple(_normalize_item(item) for item in items))


def any_of(*items: type[BaseDataPoint[Any]] | CompletionCondition) -> AnyOf:
    """Combine conditions disjunctively; a bare DataPoint type is shorthand for ``TypePresent``."""
    return AnyOf(tuple(_normalize_item(item) for item in items))


def normalize_completion(
    completes_when: type[BaseDataPoint[Any]] | CompletionCondition | None,
) -> CompletionCondition | None:
    """Normalize the public ``completes_when`` parameter — the one place bare types become AST."""
    return None if completes_when is None else _normalize_item(completes_when)


def _normalize_item(item: type[BaseDataPoint[Any]] | CompletionCondition) -> CompletionCondition:
    # Validated eagerly so a malformed flow definition fails at construction, not deep inside
    # the gather loop when the condition is first evaluated.
    if isinstance(item, type):
        if issubclass(item, BaseDataPoint):
            return TypePresent(item)
        raise InvalidCompletionConditionError(f'{item!r} is not a DataPoint type or CompletionCondition')
    if isinstance(item, CompletionCondition):
        return item
    raise InvalidCompletionConditionError(f'{item!r} is not a DataPoint type or CompletionCondition')
