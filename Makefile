.PHONY: deps lint fix_lint test test_integration test_all clean_venv

# `--all-extras` pulls the redis + mongo adapter backends so mypy and the adapter
# tests have them available.
deps:
	poetry install --all-extras --with dev

lint: deps
	poetry check
	poetry run ruff check .
	poetry run ruff format --check .
	poetry run mypy .

fix_lint: deps
	poetry run ruff format .
	poetry run ruff check . --fix

# In-memory adapters only — no Docker required.
test: deps
	poetry run pytest

# Needs Docker: the `real_mongo_database` fixture starts a real MongoDB server.
test_integration: deps
	poetry run pytest -m integration

test_all: test test_integration

clean_venv:
	rm -rf .venv
