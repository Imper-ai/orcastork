.PHONY: deps lint fix_lint test test_integration test_all build pack clean

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

# The tarball a consumer actually installs, so what the `exports` map promises can be checked
# against what is in the package. `npm pack` rebuilds through the `prepack` script first, so a
# stale `dist` can never be published.
pack: deps
	npm pack

clean:
	rm -rf dist coverage node_modules *.tgz
