## What this changes

<!-- And why it needed to. Link an issue if there is one. -->

## How it was verified

<!--
`make lint && make test` is what CI reports; what a reviewer cannot get from CI is which case
you actually reproduced, or which invariant you convinced yourself you did not break. If you
touched one of the documented invariants in CLAUDE.md, the reasoning belongs here.
-->

## Checklist

- [ ] `make lint` and `make test` pass locally
- [ ] Tests cover the new behaviour, and drive time through `FakeClock` rather than sleeping
- [ ] A new adapter passes the conformance suite in `tests/doubles/conformance.py`
- [ ] No new import outside what `pyproject.toml` declares, and no infrastructure SDK outside `adapters/`
- [ ] `CHANGELOG.md` updated under `[Unreleased]` if this is user-visible
