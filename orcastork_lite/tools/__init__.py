"""Standalone tooling around orcastork_lite — the ``orcastork-lite-graph`` CLI.

Deliberately separate from the library: this package imports the core, the core never
imports this package, and nothing here runs inside a session.
"""
