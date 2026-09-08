"""The DataPoint type zoo used across the suite (HRN-02).

Includes an abstract intermediate (``EmailDataPoint``), two leaves sharing that
substitution group (``WorkEmailDataPoint`` / ``PersonalEmailDataPoint``), a leaf with an
unhashable ``dict`` value (``GeoDataPoint``, exercising ``_make_hashable``), and an
ephemeral leaf (``TriggerDataPoint``). Importing this module self-registers the zoo.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from orcastork.datapoints import BaseDataPoint, DataPointTypeConfig
from orcastork.ids import OperatorId, OperatorRef

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
DEFAULT_OP = OperatorId('stub_operator')


# --- abstract intermediate + its leaves (a substitution group) -----------------------
class EmailDataPoint(BaseDataPoint[str]):
    __abstract__ = True
    config = DataPointTypeConfig(pii=True, ephemeral=False)


class WorkEmailDataPoint(EmailDataPoint):
    type: Literal['work_email'] = 'work_email'


class PersonalEmailDataPoint(EmailDataPoint):
    type: Literal['personal_email'] = 'personal_email'


# --- standalone leaves ----------------------------------------------------------------
class IpDataPoint(BaseDataPoint[str]):
    type: Literal['ip'] = 'ip'
    config = DataPointTypeConfig(pii=True, ephemeral=False)


class GeoDataPoint(BaseDataPoint[dict[str, float]]):
    # Unhashable (dict) value — exercises identity hashing via ``_make_hashable`` (DP-12).
    type: Literal['geo'] = 'geo'
    config = DataPointTypeConfig(pii=False, ephemeral=False)


class RiskDataPoint(BaseDataPoint[float]):
    type: Literal['risk'] = 'risk'
    config = DataPointTypeConfig(pii=False, ephemeral=False)


class ChatAnswerDataPoint(BaseDataPoint[str]):
    type: Literal['chat_answer'] = 'chat_answer'
    config = DataPointTypeConfig(pii=True, ephemeral=False)


class TriggerDataPoint(BaseDataPoint[str]):
    # Ephemeral: emitted only to trigger another operator; never persisted.
    type: Literal['trigger'] = 'trigger'
    config = DataPointTypeConfig(pii=False, ephemeral=True)


def work_email(
    value: str = 'alice@work.example',
    *,
    first: datetime = T0,
    last: datetime = T0,
    by: OperatorRef = DEFAULT_OP,
) -> WorkEmailDataPoint:
    return WorkEmailDataPoint(value=value, retrieved_by=by, first_retrieved=first, last_retrieved=last)


def personal_email(
    value: str = 'alice@personal.example',
    *,
    first: datetime = T0,
    last: datetime = T0,
    by: OperatorRef = DEFAULT_OP,
) -> PersonalEmailDataPoint:
    return PersonalEmailDataPoint(value=value, retrieved_by=by, first_retrieved=first, last_retrieved=last)


def risk(
    value: float = 0.5,
    *,
    first: datetime = T0,
    last: datetime = T0,
    by: OperatorRef = DEFAULT_OP,
) -> RiskDataPoint:
    return RiskDataPoint(value=value, retrieved_by=by, first_retrieved=first, last_retrieved=last)


def ip(
    value: str = '203.0.113.7', *, first: datetime = T0, last: datetime = T0, by: OperatorRef = DEFAULT_OP
) -> IpDataPoint:
    return IpDataPoint(value=value, retrieved_by=by, first_retrieved=first, last_retrieved=last)


def chat_answer(
    value: str = 'answer', *, first: datetime = T0, last: datetime = T0, by: OperatorRef = DEFAULT_OP
) -> ChatAnswerDataPoint:
    return ChatAnswerDataPoint(value=value, retrieved_by=by, first_retrieved=first, last_retrieved=last)
