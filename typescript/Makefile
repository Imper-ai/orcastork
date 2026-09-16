.PHONY: deps lint fix_lint test test_integration test_all build clean

# `npm ci` and not `npm install`: the lock file is the dependency list, and a build that
# quietly resolves something else is not the build CI ran.
deps:
	npm ci

lint: deps
	npm run lint

fix_lint: deps
	npm run fix_lint

# Needs no service it cannot start itself: the Redis suite spawns `redis-server` on a free
# port and the Mongo suite uses an ephemeral server binary.
test: deps
	npm test

test_integration: deps
	npm run test:integration

test_all: test test_integration

build: deps
	npm run build

clean:
	rm -rf dist coverage node_modules
