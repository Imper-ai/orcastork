"""The ``ArchivedDataPoint`` model — one durable document per ``(session, type, value)``.

Pure pydantic and framework-owned; the Mongo adapter binds it to the
``orcastork-datapoints`` collection. Identity mirrors the keyed-merge
(timestamps excluded, like the live store's identity): ``(session_id, type, value_hash)``. PII values
are encrypted at rest by the adapter via the injected :class:`ValueCipher`; the model
itself always carries the plaintext value.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar, Self

from pydantic import BaseModel, ConfigDict

from ..datapoints import BaseDataPoint
from ..ids import DataPointType, Epoch, NamespaceId, OperatorRef, SessionId


class ArchivedDataPoint(BaseModel):
    model_config = ConfigDict(frozen=True)

    # The durable destination is decided here, by the model — the adapter routes by it rather than
    # hardcoding a collection. A consuming flow may subclass with its own `__table_name__`.
    __table_name__: ClassVar[str] = 'orcastork-datapoints'

    session_id: SessionId
    namespace_id: NamespaceId
    type: DataPointType  # discriminator (part of the upsert key)
    # Derived by the archive adapter (which holds the cipher key): a keyed MAC of the value for PII,
    # a plain SHA-256 otherwise. Left empty on construction; never the plaintext value for PII.
    value_hash: str = ''
    value: Any  # encrypted at rest by the adapter when ``is_pii`` (the model holds plaintext)
    retrieved_by: OperatorRef
    first_retrieved: datetime  # set on first observation, immutable
    last_retrieved: datetime  # bumped on every re-observation (keyed-upsert)
    is_pii: bool
    epoch: Epoch  # fencing token of the writer
    schema_version: int = 1

    @classmethod
    def from_data_point(
        cls,
        data_point: BaseDataPoint[Any],
        *,
        session_id: SessionId,
        namespace_id: NamespaceId,
        epoch: Epoch,
    ) -> Self:
        """Project a live DataPoint into its archive document (provenance supplied by the orchestrator)."""
        return cls(
            session_id=session_id,
            namespace_id=namespace_id,
            type=data_point.type,
            value=data_point.model_dump(mode='json')['value'],
            retrieved_by=data_point.retrieved_by,
            first_retrieved=data_point.first_retrieved,
            last_retrieved=data_point.last_retrieved,
            is_pii=data_point.is_pii,
            epoch=epoch,
        )
