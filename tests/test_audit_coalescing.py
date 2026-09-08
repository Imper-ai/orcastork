"""High-volume DataPoint types can opt out of a row per emission.

A per-frame debugger probe and a page-view stream were ~78% of one session's audit trail. Every row is a
durable write on the session's hot path, so the volume lands on the database as it is produced. Opting
out trades per-emission detail for one counted row.

No leaf is declared here on purpose: leaves self-register into a process-global discriminated union, so
a test-only type leaks into every other test's view of the registry.
"""

from orcastork.audit.models import AuditKind
from orcastork.datapoints import DataPointTypeConfig


def test_the_default_is_to_audit_every_emission() -> None:
    # The opt-out must be explicit: a new type silently losing its trail would be a bad default.
    assert DataPointTypeConfig(pii=False, ephemeral=False).audit_every_emission is True


def test_a_high_volume_type_can_opt_out() -> None:
    assert DataPointTypeConfig(pii=False, ephemeral=False, audit_every_emission=False).audit_every_emission is False


def test_opting_out_is_independent_of_pii_and_ephemeral() -> None:
    # The two real opt-outs are pii+ephemeral leaves; the flag must not disturb either classification,
    # since those drive encryption and durable-store writes.
    config = DataPointTypeConfig(pii=True, ephemeral=True, audit_every_emission=False)

    assert (config.pii, config.ephemeral) == (True, True)


def test_the_coalesced_kind_exists_for_the_summary_row() -> None:
    assert AuditKind.DATA_POINTS_COALESCED.value == 'data_points_coalesced'
