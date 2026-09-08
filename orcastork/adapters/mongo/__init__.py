"""Mongo adapters (extra ``mongo``) — OCC durable store + buffered audit sink.

Each adapter takes an async Mongo database (``pymongo``'s ``AsyncMongoClient[...]`` in
production); the framework owns this code directly and does not depend on ``common``.
"""

from .audit_sink import MongoAuditSink
from .datapoint_archive import MongoDataPointArchive
from .durable_store import MongoDurableStore

__all__ = ['MongoAuditSink', 'MongoDataPointArchive', 'MongoDurableStore']
