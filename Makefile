.PHONY: build proto test integration fmt fmt-check vet vulncheck commit-policy postgres-up postgres-down postgres-logs

build:
	@echo "watchd skeleton: no build targets implemented"

# Regenerates the committed Go code for the v1 API. Requires protoc,
# protoc-gen-go, and protoc-gen-go-grpc on PATH.
proto:
	protoc --proto_path=. \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/watch/v1/watch.proto

test:
	go test -race -covermode=atomic -coverprofile=coverage.out ./...

integration:
	go test -race -p=1 -tags=integration ./...

commit-policy:
	@test -n "$(RANGE)" || (echo "usage: make commit-policy RANGE=<git-revision-range>" >&2; exit 2)
	./scripts/check-commit-messages.sh "$(RANGE)"

fmt:
	gofmt -l -w .

fmt-check:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		echo "the following files are not gofmt-formatted:" >&2; \
		echo "$$unformatted" >&2; \
		exit 1; \
	fi

vet:
	go vet ./...

vulncheck:
	govulncheck ./...

postgres-up:
	docker compose up --detach --wait postgres

postgres-down:
	docker compose down --volumes

postgres-logs:
	docker compose logs --follow postgres
