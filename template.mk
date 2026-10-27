.PHONY: setup
setup: deps/node

.PHONY: deps/node
deps/node:
	pnpm install --frozen-lockfile

.PHONY: commit
commit:
	pnpm czg
