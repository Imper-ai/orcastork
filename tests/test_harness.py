"""HRN — test-harness & architectural-boundary guards.

The import-boundary tests are the executable form of the framework's two hardest rules:
the package imports nothing beyond the dependencies it declares, and the infrastructure
SDKs (``redis``/``pymongo``) are confined to ``adapters/``. Both scan source with the
``ast`` module — they never import the modules — so they are safe to run even when the
optional adapter backends are not installed.

The dependency guard reads the allowed set out of ``pyproject.toml`` rather than
repeating it, so adding a runtime dependency there is the only way to widen it, and
adding an import without declaring it fails here instead of at a consumer's install.
"""

import ast
import re
import sys
import tomllib
from pathlib import Path

import pytest

import orcastork
import orcastork_lite

# Both packages in the distribution are held to the same boundaries.
PACKAGE_ROOTS = [Path(orcastork.__file__).parent, Path(orcastork_lite.__file__).parent]
PYPROJECT = PACKAGE_ROOTS[0].parent / 'pyproject.toml'

# Distributions that do not import under their own name, or that ship more than one root.
# `pydantic_core` is pydantic's compiled core, pinned by pydantic itself and the only place
# `PydanticUndefined` is exported from — depending on it separately would pin it twice.
_IMPORT_ROOTS = {
    'opentelemetry-api': frozenset({'opentelemetry'}),
    'pydantic': frozenset({'pydantic', 'pydantic_core'}),
}


def _declared_dependencies() -> frozenset[str]:
    """Import roots the package is allowed to reach for: its declared deps, plus itself."""
    project = tomllib.loads(PYPROJECT.read_text())['project']
    requirements = list(project['dependencies'])
    for extra in project.get('optional-dependencies', {}).values():
        requirements.extend(extra)
    allowed = {'orcastork', 'orcastork_lite'}
    for requirement in requirements:
        # A requirement is `name`, `name>=1.2`, or `name (>=1.2,<2)`; the name is the leading token.
        leading_name = re.match(r'[A-Za-z0-9._-]+', requirement)
        assert leading_name is not None, f'unparseable requirement in pyproject.toml: {requirement!r}'
        name = leading_name.group(0).lower()
        allowed |= _IMPORT_ROOTS.get(name, frozenset({name.replace('-', '_')}))
    return frozenset(allowed)


# Infrastructure SDKs may only be imported under `adapters/` (ports/adapters boundary).
# `opentelemetry` is deliberately NOT here: the OTel API is the framework's built-in telemetry
# standard (a core dependency that no-ops without an SDK), not a backend behind a port.
INFRA_SDKS = frozenset({'redis', 'pymongo', 'motor', 'bson'})


def _iter_source_files(package_root: Path) -> list[Path]:
    return sorted(package_root.rglob('*.py'))


def _imported_roots(source: str) -> set[str]:
    """Top-level package of every ``import x`` / ``from x import y`` in the source."""
    roots: set[str] = set()
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split('.', 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.add(node.module.split('.', 1)[0])
    return roots


@pytest.mark.parametrize('package_root', PACKAGE_ROOTS, ids=lambda root: root.name)
def test_hrn_no_module_imports_an_undeclared_dependency(package_root: Path) -> None:
    allowed = _declared_dependencies() | sys.stdlib_module_names
    offenders = {
        file.relative_to(package_root).as_posix(): sorted(roots - allowed)
        for file in _iter_source_files(package_root)
        if (roots := _imported_roots(file.read_text())) - allowed
    }
    assert not offenders, f'These modules import packages pyproject.toml does not declare: {offenders}'


@pytest.mark.parametrize('package_root', PACKAGE_ROOTS, ids=lambda root: root.name)
def test_hrn_infra_sdks_confined_to_adapters(package_root: Path) -> None:
    offenders = {
        rel.as_posix(): sorted(roots & INFRA_SDKS)
        for file in _iter_source_files(package_root)
        if not (rel := file.relative_to(package_root)).as_posix().startswith('adapters/')
        and (roots := _imported_roots(file.read_text())) & INFRA_SDKS
    }
    assert not offenders, f'infrastructure SDKs may only be imported under adapters/: {offenders}'
