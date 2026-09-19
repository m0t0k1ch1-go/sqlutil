.PHONY: setup
setup: deps/node

.PHONY: deps
deps:
	go mod download
	go mod verify

.PHONY: deps/node
deps/node:
	pnpm install --frozen-lockfile

.PHONY: commit
commit:
	pnpm czg

.PHONY: lint
lint:
	go vet ./...
	go tool staticcheck ./...

.PHONY: test
test:
	go test $(APP_TEST_FLAGS) ./...
