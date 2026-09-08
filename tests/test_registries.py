"""REG — operator / capability / aggregator auto-registration.

The second half (``test_reg_dp_*``) covers the DataPoint registry: the version-counter-driven
lazy-union cache, the idempotent same-class re-registration branch, the single-leaf and empty
registry boundaries, and the production registration lifecycle (defining leaves accumulates the
union; ``reset_cache`` forces a rebuild) that the autouse isolation fixture normally hides.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

import pytest

from orcastork.capabilities import Capability
from orcastork.datapoints import (
    BaseDataPoint,
    DataPointTypeConfig,
    data_point_adapter,
    parse_data_point,
    registered_leaves,
    registry_version,
)
from orcastork.datapoints import base as dp_base
from orcastork.datapoints import registry as dp_registry
from orcastork.exceptions import DuplicateRegistrationError, UnknownDataPointTypeError
from orcastork.ids import CapabilityId, OperatorId, OperatorRef
from orcastork.operators import Aggregator, Operator, OperatorPolicy

from .doubles.capabilities import make_capability
from .doubles.operators import make_aggregator, make_operator

_T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
_OP: OperatorRef = OperatorId('reg_stub_operator')


def test_reg_01_operator_auto_registers_under_operator_id() -> None:
    operator = make_operator('reg_op')
    assert Operator.registered()[OperatorId('reg_op')] is operator


def test_reg_02_capability_and_aggregator_self_register() -> None:
    capability = make_capability('reg_cap')
    aggregator = make_aggregator('reg_agg')
    assert Capability._registry[CapabilityId('reg_cap')] is capability
    assert Operator.registered()[OperatorId('reg_agg')] is aggregator  # an aggregator is an operator


def test_reg_03_duplicate_operator_id_raises_at_definition() -> None:
    make_operator('reg_dup')
    with pytest.raises(DuplicateRegistrationError):
        make_operator('reg_dup')


def test_reg_04_abstract_base_without_key_is_not_registered() -> None:
    before = set(Operator.registered())

    class AbstractOperator(Operator):  # no operator_id, `run` still abstract → not concrete
        pass

    assert set(Operator.registered()) == before
    # The Aggregator base itself is abstract and unregistered.
    assert Aggregator not in Operator.registered().values()


def test_reg_05_operator_policy_requires_explicit_rerun_choice() -> None:
    with pytest.raises(TypeError):
        OperatorPolicy()  # type: ignore[call-arg]  # rerun_on_new_data has NO default


def test_reg_06_operator_and_capability_registries_are_isolated() -> None:
    make_operator('reg_iso_op')
    assert CapabilityId('reg_iso_op') not in Capability._registry
    make_capability('reg_iso_cap')
    assert OperatorId('reg_iso_cap') not in Operator.registered()


def test_reg_07_importing_handlers_populates_registry() -> None:
    for index in range(3):
        make_operator(f'reg_startup_{index}')
    assert {OperatorId(f'reg_startup_{index}') for index in range(3)} <= set(Operator.registered())


def test_reg_08_aggregator_is_an_operator_subtype() -> None:
    assert issubclass(Aggregator, Operator)
    aggregator = make_aggregator('reg_agg_subtype')
    assert issubclass(aggregator, Operator)
    assert isinstance(aggregator(), Operator)


# --- DataPoint registry: idempotent re-registration, union-cache lifecycle, boundaries ---


def test_reg_dp_01_same_class_reregistration_is_idempotent_and_does_not_raise() -> None:
    # A re-import / reload path re-invokes the post-build hook on the IDENTICAL class. Because
    # the registry already maps the discriminator to that same class, re-registration must be
    # benign (no DuplicateRegistrationError) and must leave the mapping pointing at it.
    class ReimportedLeaf(BaseDataPoint[str]):
        type: Literal['reg_dp_reimported'] = 'reg_dp_reimported'
        config = DataPointTypeConfig(pii=False, ephemeral=False)

    assert dp_base._REGISTRY['reg_dp_reimported'] is ReimportedLeaf
    version_before = registry_version()

    # Re-run the exact registration hook on the same class object (the reload scenario).
    ReimportedLeaf.__pydantic_init_subclass__()

    assert dp_base._REGISTRY['reg_dp_reimported'] is ReimportedLeaf  # still mapped to the same class
    assert registry_version() == version_before + 1  # a harmless re-register still bumps the version


def test_reg_dp_02_distinct_class_on_same_discriminator_still_raises() -> None:
    # The idempotent branch must NOT swallow a genuine collision: a DIFFERENT class claiming an
    # already-registered discriminator is a hard error.
    class FirstClaimant(BaseDataPoint[str]):
        type: Literal['reg_dp_collide'] = 'reg_dp_collide'
        config = DataPointTypeConfig(pii=False, ephemeral=False)

    with pytest.raises(DuplicateRegistrationError):

        class SecondClaimant(BaseDataPoint[str]):
            type: Literal['reg_dp_collide'] = 'reg_dp_collide'
            config = DataPointTypeConfig(pii=False, ephemeral=False)

    assert dp_base._REGISTRY['reg_dp_collide'] is FirstClaimant  # the first registrant is untouched


def test_reg_dp_03_single_leaf_union_round_trips_and_rejects_other_types() -> None:
    # With exactly ONE registered leaf the adapter is built from the bare leaf class (a single-
    # member Union is degenerate). It must still round-trip that leaf and still reject any other.
    class SoleLeaf(BaseDataPoint[str]):
        type: Literal['reg_dp_sole'] = 'reg_dp_sole'
        config = DataPointTypeConfig(pii=False, ephemeral=False)

    # Collapse the registry to just this leaf (the isolation fixture restores it afterwards).
    dp_base._REGISTRY.clear()
    dp_base._REGISTRY['reg_dp_sole'] = SoleLeaf
    dp_base._registry_version += 1
    dp_registry.reset_cache()

    assert len(registered_leaves()) == 1
    # The single-leaf branch builds the adapter from the bare leaf class — a degenerate
    # single-member discriminated Union is avoided, so the core schema is a plain model,
    # not a tagged-union (which is what the multi-leaf path would produce).
    assert data_point_adapter().core_schema['type'] == 'model'
    sole = SoleLeaf(value='x', retrieved_by=_OP, first_retrieved=_T0, last_retrieved=_T0)
    restored = parse_data_point(sole.model_dump())
    assert type(restored) is SoleLeaf
    assert restored == sole

    with pytest.raises(UnknownDataPointTypeError):
        parse_data_point(
            {'type': 'other', 'value': 'x', 'retrieved_by': 'op', 'first_retrieved': _T0, 'last_retrieved': _T0}
        )


def test_reg_dp_04_empty_registry_adapter_raises_clear_error() -> None:
    # The lazy union must fail loudly (not return a degenerate adapter) when nothing is registered.
    dp_base._REGISTRY.clear()
    dp_base._registry_version += 1
    dp_registry.reset_cache()

    with pytest.raises(UnknownDataPointTypeError):
        data_point_adapter()


def test_reg_dp_05_version_bump_then_reset_cache_both_serve_the_current_leaf_set() -> None:
    # The production lifecycle the snapshot/restore harness normally hides: registrations
    # accumulate, the union is rebuilt on a version bump, a reset forces a fresh rebuild, and
    # each rebuild serves exactly the CURRENT leaf set (no stale-cache split-brain).
    dp_base._REGISTRY.clear()
    dp_base._registry_version += 1
    dp_registry.reset_cache()

    class LeafA(BaseDataPoint[str]):
        type: Literal['reg_dp_life_a'] = 'reg_dp_life_a'
        config = DataPointTypeConfig(pii=False, ephemeral=False)

    # First build: only A is registered, so A round-trips and B is unknown.
    adapter_a = data_point_adapter()
    a = LeafA(value='a', retrieved_by=_OP, first_retrieved=_T0, last_retrieved=_T0)
    assert type(adapter_a.validate_python(a.model_dump())) is LeafA

    version_after_a = registry_version()

    class LeafB(BaseDataPoint[str]):
        type: Literal['reg_dp_life_b'] = 'reg_dp_life_b'
        config = DataPointTypeConfig(pii=False, ephemeral=False)

    # Defining B bumps the version; the cache must rebuild and now pick B too.
    assert registry_version() > version_after_a
    b = LeafB(value='b', retrieved_by=_OP, first_retrieved=_T0, last_retrieved=_T0)
    rebuilt = data_point_adapter()
    assert rebuilt is not adapter_a  # a version bump invalidates the prior cached adapter
    assert type(rebuilt.validate_python(b.model_dump())) is LeafB
    assert type(rebuilt.validate_python(a.model_dump())) is LeafA  # A still served

    # A bare reset (no version change) forces a rebuild that still serves the same current set.
    dp_registry.reset_cache()
    after_reset = data_point_adapter()
    assert after_reset is not rebuilt
    assert type(after_reset.validate_python(a.model_dump())) is LeafA
    assert type(after_reset.validate_python(b.model_dump())) is LeafB


def test_reg_dp_06_abstract_flag_does_not_leak_to_concrete_leaves() -> None:
    # An intermediate is abstract only if it sets ``__abstract__`` in its OWN body; the flag must
    # not leak via inheritance, so a leaf below an abstract intermediate is a concrete union member.
    class AbstractMid(BaseDataPoint[str]):
        __abstract__ = True
        config = DataPointTypeConfig(pii=True, ephemeral=False)

    class ConcreteLeaf(AbstractMid):
        type: Literal['reg_dp_leaf_under_abstract'] = 'reg_dp_leaf_under_abstract'

    assert AbstractMid in dp_base._ABSTRACT_TYPES
    assert AbstractMid not in registered_leaves()
    assert ConcreteLeaf in registered_leaves()  # the leaf is concrete despite the abstract parent
    assert dp_base._REGISTRY['reg_dp_leaf_under_abstract'] is ConcreteLeaf
