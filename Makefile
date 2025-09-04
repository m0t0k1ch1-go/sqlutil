.PHONY: setup
setup: deps-dev

.PHONY: deps-dev
deps-dev:
	pnpm install

.PHONY: commit
commit:
	pnpm cz

.PHONY: deps
deps:
	go mod download
	go mod verify

.PHONY: lint
lint:
	go vet ./...
	go tool staticcheck ./...

.PHONY: test
test:
	go test -v ./...

.PHONY: test-race
test-race:
	go test -race -v ./...

.PHONY: test-race-cover
test-race-cover:
	go test -race -v ./... -coverprofile=coverage.txt
