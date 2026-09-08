"""DP — the DataPoint model: identity, discriminated union, subtype substitution, config."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Literal

import pytest
from pydantic import ValidationError

from orcastork.datapoints import (
    BaseDataPoint,
    DataPointSet,
    DataPointTypeConfig,
    DataPointView,
    data_point_adapter,
    parse_data_point,
    registered_leaves,
    registry_version,
    subtypes_of,
)
from orcastork.datapoints.collection import MergeKind
from orcastork.exceptions import InvalidDataPointError, UnknownDataPointTypeError

from .doubles.datapoints import (
    DEFAULT_OP,
    T0,
    EmailDataPoint,
    GeoDataPoint,
    IpDataPoint,
    PersonalEmailDataPoint,
    TriggerDataPoint,
    WorkEmailDataPoint,
    personal_email,
    work_email,
)

T1 = T0 + timedelta(hours=1)
T2 = T0 + timedelta(hours=2)


def test_dp_01_identity_excludes_timestamps_and_merge_bumps_last_retrieved() -> None:
    # Arrange: same (type, value), different timestamps.
    early = work_email('alice@work.example', first=T0, last=T0)
    later = work_email('alice@work.example', first=T1, last=T2)

    # Assert: equal + hash-equal (timestamps excluded from identity).
    assert early == later
    assert hash(early) == hash(later)

    # Act: re-add to a set.
    points = DataPointSet([early])
    points.add(later)

    # Assert: merged in place — no growth, first kept, last bumped.
    assert len(points) == 1
    merged = points.all()[0]
    assert merged.first_retrieved == T0
    assert merged.last_retrieved == T2


def test_dp_02_discriminated_union_round_trips_to_concrete_class() -> None:
    original = work_email('round@trip.example')
    restored = parse_data_point(original.model_dump())
    assert type(restored) is WorkEmailDataPoint
    assert restored == original


def test_dp_03_latest_returns_newest_with_full_set_queryable() -> None:
    older = work_email('a@work.example', last=T0)
    newer = work_email('b@work.example', last=T2)
    view = DataPointView([older, newer])

    assert view.latest(WorkEmailDataPoint) is newer
    assert set(view.of_type(WorkEmailDataPoint)) == {older, newer}


def test_dp_04_rejects_concrete_leaf_missing_type_literal() -> None:
    with pytest.raises(InvalidDataPointError):

        class MissingType(BaseDataPoint[str]):
            config = DataPointTypeConfig(pii=False, ephemeral=False)


def test_dp_05_rejects_concrete_leaf_missing_config() -> None:
    with pytest.raises(InvalidDataPointError):

        class MissingConfig(BaseDataPoint[str]):
            type: Literal['missing_config'] = 'missing_config'


def test_dp_06_abstract_intermediate_is_not_a_union_member_but_matches_isinstance() -> None:
    assert EmailDataPoint not in registered_leaves()
    assert WorkEmailDataPoint in registered_leaves()
    assert isinstance(work_email(), EmailDataPoint)
    assert set(subtypes_of(EmailDataPoint)) == {WorkEmailDataPoint, PersonalEmailDataPoint}


def test_dp_07_union_assembled_lazily_and_import_order_independent() -> None:
    assert type(parse_data_point(work_email().model_dump())) is WorkEmailDataPoint
    version_before = registry_version()

    # A leaf defined AFTER the union was first built must still be deserializable.
    class LateLeaf(BaseDataPoint[str]):
        type: Literal['late_leaf'] = 'late_leaf'
        config = DataPointTypeConfig(pii=False, ephemeral=False)

    assert registry_version() > version_before
    late = LateLeaf(value='x', retrieved_by=DEFAULT_OP, first_retrieved=T0, last_retrieved=T0)
    assert type(parse_data_point(late.model_dump())) is LateLeaf


def test_dp_08_unknown_discriminator_raises_clear_error() -> None:
    raw = {'type': 'no_such_type', 'value': 'x', 'retrieved_by': 'op', 'first_retrieved': T0, 'last_retrieved': T0}
    with pytest.raises(UnknownDataPointTypeError):
        parse_data_point(raw)


def test_dp_09_extra_fields_ignored_on_read() -> None:
    raw = {
        'type': 'work_email',
        'value': 'a@work.example',
        'retrieved_by': 'op',
        'first_retrieved': T0,
        'last_retrieved': T0,
        'unexpected_field': 'ignored',
    }
    restored = parse_data_point(raw)
    assert type(restored) is WorkEmailDataPoint
    assert not hasattr(restored, 'unexpected_field')


def test_dp_10_is_pii_and_is_ephemeral_derive_from_class_config() -> None:
    # WorkEmail inherits config from the abstract EmailDataPoint intermediate.
    assert work_email().is_pii is True
    assert work_email().is_ephemeral is False
    # Trigger sets ephemeral per leaf.
    trigger = TriggerDataPoint(value='go', retrieved_by=DEFAULT_OP, first_retrieved=T0, last_retrieved=T0)
    assert trigger.is_ephemeral is True
    assert trigger.is_pii is False


def test_dp_12_audit_summary_defaults_none_and_is_overridable() -> None:
    # Default: no override -> None, so the orchestrator redacts a PII value / stringifies a non-PII one.
    assert work_email().audit_summary() is None
    trigger = TriggerDataPoint(value='go', retrieved_by=DEFAULT_OP, first_retrieved=T0, last_retrieved=T0)
    assert trigger.audit_summary() is None

    class TaggedDataPoint(BaseDataPoint[dict[str, Any]]):
        type: Literal['audit_tagged'] = 'audit_tagged'
        config = DataPointTypeConfig(pii=True, ephemeral=True)

        def audit_summary(self) -> str:
            return f'tag={self.value["tag"]}'

    tagged = TaggedDataPoint(
        value={'tag': 'x', 'secret': 's'}, retrieved_by=DEFAULT_OP, first_retrieved=T0, last_retrieved=T0
    )
    # A PII DataPoint can still surface a non-PII summary for the audit (the secret stays out).
    assert tagged.is_pii is True
    assert tagged.audit_summary() == 'tag=x'


def test_dp_11_value_type_binding_enforced() -> None:
    with pytest.raises(ValidationError):
        WorkEmailDataPoint(  # type: ignore[arg-type]
            value=123, retrieved_by=DEFAULT_OP, first_retrieved=T0, last_retrieved=T0
        )


def test_dp_12_unhashable_value_is_hashable_via_make_hashable() -> None:
    geo_a = GeoDataPoint(
        value={'lat': 1.0, 'lon': 2.0}, retrieved_by=DEFAULT_OP, first_retrieved=T0, last_retrieved=T0
    )
    geo_b = GeoDataPoint(
        value={'lon': 2.0, 'lat': 1.0}, retrieved_by=DEFAULT_OP, first_retrieved=T1, last_retrieved=T1
    )
    # Hashing does not raise, and key order in the dict value does not affect identity.
    assert hash(geo_a) == hash(geo_b)
    assert geo_a == geo_b


def test_dp_13_latest_returns_none_when_absent() -> None:
    assert DataPointView([]).latest(WorkEmailDataPoint) is None


def test_dp_14_latest_on_abstract_returns_newest_across_subtypes() -> None:
    work = work_email('w@example', last=T0)
    personal = personal_email('p@example', last=T2)
    view = DataPointView([work, personal])
    assert view.latest(EmailDataPoint) is personal


def test_dp_15_different_types_same_value_are_not_equal() -> None:
    work = WorkEmailDataPoint(value='same', retrieved_by=DEFAULT_OP, first_retrieved=T0, last_retrieved=T0)
    ip = IpDataPoint(value='same', retrieved_by=DEFAULT_OP, first_retrieved=T0, last_retrieved=T0)
    assert work != ip


def test_dp_16_first_retrieved_immutable_across_reobservation() -> None:
    points = DataPointSet([work_email('x@example', first=T0, last=T0)])
    points.add(work_email('x@example', first=T2, last=T2))
    merged = points.all()[0]
    assert merged.first_retrieved == T0  # immutable
    assert merged.last_retrieved == T2  # advanced


# A list-valued leaf and a nested-dict-of-list leaf exercise the ``_make_hashable``
# recursion that the scalar zoo never reaches. Defined at module scope so they register
# into the per-test baseline exactly like the zoo (the isolation fixture snapshots them).
class TagsDataPoint(BaseDataPoint[list[str]]):
    type: Literal['tags'] = 'tags'
    config = DataPointTypeConfig(pii=False, ephemeral=False)


class NestedDataPoint(BaseDataPoint[dict[str, list[str]]]):
    type: Literal['nested'] = 'nested'
    config = DataPointTypeConfig(pii=False, ephemeral=False)


def _tags(value: list[str], *, first: object = T0, last: object = T0) -> TagsDataPoint:
    return TagsDataPoint(value=value, retrieved_by=DEFAULT_OP, first_retrieved=first, last_retrieved=last)


def test_dp_17_list_valued_point_is_hashable_and_order_sensitive() -> None:
    # A list value must be coerced to a tuple so ``hash()`` does not raise, and two
    # element-equal lists must hash-equal and compare equal via the keyed-merge identity.
    a = _tags(['x', 'y'], last=T0)
    b = _tags(['x', 'y'], last=T2)
    assert hash(a) == hash(b)
    assert a == b

    # Order is part of a list's value identity — reordering is a distinct DataPoint.
    reordered = _tags(['y', 'x'])
    assert hash(reordered) != hash(a)
    assert reordered != a


def test_dp_18_nested_dict_of_list_value_is_order_insensitive_at_dict_level() -> None:
    # The dict layer is normalized (sorted keys) while the inner lists keep their order:
    # two emissions differing only in dict key order must dedup to one identity.
    a = NestedDataPoint(
        value={'tags': ['x', 'y'], 'groups': ['a']},
        retrieved_by=DEFAULT_OP,
        first_retrieved=T0,
        last_retrieved=T0,
    )
    b = NestedDataPoint(
        value={'groups': ['a'], 'tags': ['x', 'y']},
        retrieved_by=DEFAULT_OP,
        first_retrieved=T1,
        last_retrieved=T1,
    )
    assert hash(a) == hash(b)
    assert a == b


def test_dp_19_dataset_dedups_two_equal_list_valued_points() -> None:
    # Re-emitting a list-valued DataPoint must merge in place, not insert a second row.
    points = DataPointSet([_tags(['x', 'y'], last=T0)])
    result = points.add(_tags(['x', 'y'], last=T2))
    assert result.kind is MergeKind.UPDATED
    assert len(points) == 1
    assert points.all()[0].last_retrieved == T2


def test_dp_20_reobserved_with_older_time_keeps_the_max() -> None:
    # An out-of-order (older) re-observation must not rewind ``last_retrieved``.
    point = work_email('x@example', first=T0, last=T2)
    rewound = point.reobserved(T0)
    assert rewound.last_retrieved == T2  # clamped to the max — never rewound
    assert rewound.first_retrieved == T0


def test_dp_21_set_merge_does_not_rewind_last_retrieved_on_older_observation() -> None:
    # The collection routes through ``reobserved``; an older add must keep the existing max.
    points = DataPointSet([work_email('x@example', last=T2)])
    result = points.add(work_email('x@example', last=T0))
    assert result.kind is MergeKind.UPDATED
    assert points.all()[0].last_retrieved == T2


def test_dp_22_latest_tie_break_is_first_maximal_by_input_order() -> None:
    # Two distinct points with identical ``last_retrieved``: ``max`` keeps the first
    # maximal in iteration order, so the result is deterministic given a fixed order.
    a = work_email('a@work.example', last=T1)
    b = work_email('b@work.example', last=T1)
    assert DataPointView([a, b]).latest(WorkEmailDataPoint) is a
    assert DataPointView([b, a]).latest(WorkEmailDataPoint) is b


def test_dp_23_contains_is_timestamp_insensitive_identity_membership() -> None:
    points = DataPointSet([work_email('x@example', last=T0)])
    # Present by identity even though the timestamp differs.
    assert work_email('x@example', last=T2) in points
    # Absent identity.
    assert work_email('y@example') not in points


def test_dp_24_contains_returns_false_for_non_datapoint_without_raising() -> None:
    points = DataPointSet([work_email('x@example')])
    # The isinstance guard must short-circuit, not let ``identity_key`` blow up.
    assert 'work_email' not in points
    assert None not in points


def test_dp_25_emit_on_abstract_intermediate_raises() -> None:
    # An abstract intermediate has no discriminator and is not a union member, so it
    # cannot be finalized into a concrete DataPoint.
    with pytest.raises(InvalidDataPointError):
        EmailDataPoint.emit('a@e.example')


def test_dp_26_emit_on_unparametrized_base_raises() -> None:
    with pytest.raises(InvalidDataPointError):
        BaseDataPoint.emit('x')


# --- S1: keyed-merge & union round-trip laws (example-based; no property library here) ---

# Adversarial JSON-native value shapes paired with a leaf that accepts them. Each pair is
# (leaf_class, value) — covering scalar str/float, empty/non-empty lists, single- and
# multi-key dict-of-list (the dict layer is order-normalized), and an empty dict. Every
# shape must agree across hash/eq and (below) round-trip through the union adapter.
_VALUE_SHAPES: list[tuple[type[BaseDataPoint[Any]], Any]] = [
    (WorkEmailDataPoint, 'scalar@e.example'),
    (TagsDataPoint, ['x', 'y', 'z']),
    (TagsDataPoint, []),
    (NestedDataPoint, {'tags': ['x', 'y'], 'groups': []}),
    (NestedDataPoint, {'groups': [], 'tags': ['x', 'y']}),  # same identity under dict-order normalization
    (NestedDataPoint, {}),
    (NestedDataPoint, {'outer': ['p', 'q']}),
]


@pytest.mark.parametrize(('leaf', 'value'), _VALUE_SHAPES)
def test_dp_27_hash_and_eq_agree_for_every_value_shape(leaf: type[BaseDataPoint[Any]], value: Any) -> None:
    # The keyed-merge law: equal values hash-equal and compare equal regardless of shape,
    # so a re-observation never splits into two store rows.
    # mypy sees only the abstract base (whose ``type`` has no default); leaves pin it.
    left = leaf(value=value, retrieved_by=DEFAULT_OP, first_retrieved=T0, last_retrieved=T0)  # type: ignore[call-arg]
    right = leaf(value=value, retrieved_by=DEFAULT_OP, first_retrieved=T2, last_retrieved=T2)  # type: ignore[call-arg]
    assert (left == right) is (hash(left) == hash(right))
    assert left == right
    assert hash(left) == hash(right)


@pytest.mark.parametrize('order', [[0, 1, 2], [2, 1, 0], [1, 2, 0], [0, 0, 2, 1], [2, 1, 1, 0, 2]])
def test_dp_28_merge_collapses_to_one_identity_with_max_last_retrieved(order: list[int]) -> None:
    # Merging a multiset of re-observations in any order collapses to one identity whose
    # value and last_retrieved (= the latest observed) are permutation-invariant. The
    # documented contract keeps the FIRST-inserted first_retrieved (immutable), so that
    # field is intentionally order-dependent — not the min — and is asserted separately.
    observations = [
        work_email('alice@work.example', first=T0, last=T0),
        work_email('alice@work.example', first=T1, last=T1),
        work_email('alice@work.example', first=T2, last=T2),
    ]
    points = DataPointSet()
    for index in order:
        points.add(observations[index])
    assert len(points) == 1
    merged = points.all()[0]
    assert merged.value == 'alice@work.example'
    assert merged.last_retrieved == T2  # latest advanced, regardless of arrival order
    # first_retrieved is whatever the first arrival carried (kept, never recomputed).
    assert merged.first_retrieved == observations[order[0]].first_retrieved


def _sample_value_for(leaf: type[BaseDataPoint[Any]]) -> Any:
    # A constructible value for each value-type family in the zoo, so no leaf is skipped.
    annotation = leaf.model_fields['value'].annotation
    if annotation is str:
        return 'probe@e.example'
    if annotation is float:
        return 0.25
    if annotation == dict[str, float]:
        return {'lat': 1.0, 'lon': 2.0}
    if annotation == list[str]:
        return ['x', 'y']
    if annotation == dict[str, list[str]]:
        return {'tags': ['x']}
    raise AssertionError(f'no sample value defined for leaf value type {annotation!r}')


def test_dp_29_union_adapter_round_trips_every_registered_leaf() -> None:
    # For every registered leaf, encode -> decode through the union adapter is identity —
    # the union round-trip law over every value shape, not just a hand-picked few.
    adapter = data_point_adapter()
    leaves = registered_leaves()
    assert leaves  # guard: an empty registry would make this vacuous
    for leaf in leaves:
        # mypy sees only the abstract base (whose ``type`` has no default); leaves pin it.
        sample = leaf(  # type: ignore[call-arg]
            value=_sample_value_for(leaf), retrieved_by=DEFAULT_OP, first_retrieved=T0, last_retrieved=T0
        )
        restored = adapter.validate_python(sample.model_dump())
        assert type(restored) is leaf
        assert restored == sample
