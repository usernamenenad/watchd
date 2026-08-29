.PHONY: build test fmt integration postgres-up postgres-down postgres-logs

build:
	@echo "watchd skeleton: no build targets implemented"

test:
	go test ./...

integration:
	go test -tags=integration ./tests/integration

fmt:
	@echo "watchd skeleton: no formatting targets implemented"

postgres-up:
	docker compose up --detach --wait postgres

postgres-down:
	docker compose down --volumes

postgres-logs:
	docker compose logs --follow postgres
