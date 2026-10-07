include conventions.mk

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
	go test $(APP_TEST_FLAGS) ./...
