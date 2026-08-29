.PHONY: build test fmt integration commit-policy postgres-up postgres-down postgres-logs

build:
	@echo "watchd skeleton: no build targets implemented"

test:
	go test ./...

integration:
	go test -tags=integration ./tests/integration

commit-policy:
	@test -n "$(RANGE)" || (echo "usage: make commit-policy RANGE=<git-revision-range>" >&2; exit 2)
	./scripts/check-commit-messages.sh "$(RANGE)"

fmt:
	@echo "watchd skeleton: no formatting targets implemented"

postgres-up:
	docker compose up --detach --wait postgres

postgres-down:
	docker compose down --volumes

postgres-logs:
	docker compose logs --follow postgres
