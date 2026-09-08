# Contributing to orcastork

Thanks for considering it. This page covers getting set up, what the checks expect, and how
changes get reviewed.

## Getting set up

You need **Python 3.13+** and [Poetry](https://python-poetry.org/). Docker is optional — only
the integration tests use it.

```bash
git clone https://github.com/Imper-ai/orcastork
cd orcastork
make deps          # poetry install --all-extras --with dev
make test          # in-memory adapters + fakeredis + mongomock; no Docker
make lint          # poetry check + ruff + ruff format --check + mypy
```

`--all-extras` matters: it installs the `redis` and `mongo` backends, without which mypy and
the adapter tests have nothing to check.

```bash
make fix_lint            # ruff format + ruff check --fix
make test_integration    # needs Docker: starts a real MongoDB via pytest-mock-resources
make test_all            # both suites
poetry run pytest tests/test_orchestrator.py -x     # one file, stop on first failure
poetry run pytest --cov=orcastork --cov-report=term-missing
```

## Before you open a pull request

`make lint && make test` must pass. CI runs exactly those, plus the integration suite, so
there are no surprises waiting for you.

A few things the checks cannot tell you, in rough order of how often they come up in review:

- **Read [CLAUDE.md](CLAUDE.md) first.** It is the map of which file owns which decision, and
  it documents a handful of invariants that look like bugs until you know why they are there —
  the bounded completion tail, `final` being terminal for an aggregator's output, when a
  coalescing window may be abandoned. Several of them have been "fixed" before and reverted.
  If your change touches one, the reasoning belongs in the pull request.
- **Time is injected.** Never call `datetime.now()` or `asyncio.sleep()` in library code; take
  the `Clock` port. Tests drive `FakeClock`, which is what makes them fast and deterministic.
- **New backends go under `adapters/`.** Core modules depend only on `ports/` protocols plus
  `ids` and `clock`. `tests/test_harness.py` enforces both that boundary and the rule that the
  package imports nothing `pyproject.toml` does not declare.
- **A new adapter must pass the conformance suite.** `tests/doubles/conformance.py` is the
  executable port contract; run the mixin against your backend rather than writing a fresh set
  of tests, so every family stays behaviourally interchangeable.
- **Tests assert behaviour, not implementation.** Arrange-act-assert, mock at the boundary,
  and no dependence on absolute dates, the local clock or timezone, test ordering, or a sleep.

## Style

`make fix_lint` settles formatting (119 columns, single quotes, 4-space indent). The rest is
in the House style section of [CLAUDE.md](CLAUDE.md); the short version:

- Type hints on every function, parameters **and** return, `-> None` included.
- Custom exceptions from `exceptions.py`; never a bare `Exception`.
- `logger.info('Session started', session_id=sid)` — kwargs, never an f-string.
- Comments explain **why**, and stay true a year from now. No ticket references, no "fixes the
  above", nothing that only makes sense next to the change that introduced it.

## Commits and pull requests

Write the commit message for someone who will read it in two years with no other context: what
changed and why it needed to. One logical change per commit; keep a rename separate from the
behaviour change that motivated it, so both stay reviewable.

In the pull request, say what you verified and how. "Tests pass" is what CI says; what a
reviewer needs is which case you reproduced, or which invariant you convinced yourself you did
not break.

## Reporting bugs and asking for features

Use the [issue templates](https://github.com/Imper-ai/orcastork/issues/new/choose). For a bug,
the single most useful thing you can include is a failing test — the in-memory runtime plus
`FakeClock` means most bugs reproduce in a few lines with no infrastructure at all.

For security vulnerabilities, do **not** open an issue: see [SECURITY.md](SECURITY.md).

## Cutting a release

Releases publish to PyPI from `.github/workflows/release.yml`, which runs when a **GitHub
Release is published** — not on a tag push, so cutting one is always a deliberate act with a
changelog attached.

To release:

1. Move the `[Unreleased]` entries in [CHANGELOG.md](CHANGELOG.md) under the new version, and
   add the comparison links at the bottom.
2. `poetry version <major|minor|patch>` (or set it explicitly) and commit.
3. Tag and push: `git tag -a vX.Y.Z -m "orcastork X.Y.Z" && git push origin vX.Y.Z`.
4. Publish a GitHub Release for that tag. The workflow verifies the tag matches
   `pyproject.toml`'s version, builds an sdist and wheel, runs `twine check --strict`, and
   uploads.

The version check in step 4 is not ceremony: **PyPI never allows a version number to be
reused**, even after a yank. A mismatch caught in CI is free; a wrong version on PyPI is
permanent.

Note that a **draft** release does not start the workflow — the trigger is `release:
published`, so the run begins when you publish, whether that is immediately or later from a
draft.

### Rehearsing it without publishing

Actions → **Release** → *Run workflow* builds the sdist and wheel and runs
`twine check --strict`, then stops: the publish job is gated on `github.event_name ==
'release'`, so a manual run cannot upload anything. Use it to confirm the build is healthy
before you cut a real release. The tag/version check is skipped there, having no tag to
compare.

### One-time setup (before the first release)

The workflow uses [PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/), so no
API token is stored in this repository. Someone with the PyPI account has to register the
publisher once, at https://pypi.org/manage/account/publishing/, as a *pending* publisher (the
project does not exist on PyPI yet):

| Field | Value |
|---|---|
| PyPI project name | `orcastork` |
| Owner | `Imper-ai` |
| Repository name | `orcastork` |
| Workflow name | `release.yml` |
| Environment name | `pypi` |

Then create a GitHub environment named `pypi` in this repository's settings. Adding a required
reviewer to it makes every upload need a human approval, which is worth it for an irreversible
action.

## Licence

orcastork is GPL-3.0-or-later. By contributing you agree that your contribution is licensed
under the same terms.
