.PHONY: build test integration fmt fmt-check vet vulncheck commit-policy postgres-up postgres-down postgres-logs

build:
	@echo "watchd skeleton: no build targets implemented"

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
