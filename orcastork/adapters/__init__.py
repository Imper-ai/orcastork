"""Adapters — the only layer that may import infrastructure SDKs (redis, pymongo).

``memory`` is always available; ``redis`` and ``mongo`` require the corresponding extras.
"""
