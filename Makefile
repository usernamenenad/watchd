.PHONY: build test fmt postgres-up postgres-down postgres-logs

build:
	@echo "watchd skeleton: no build targets implemented"

test:
	@echo "watchd skeleton: no tests implemented"

fmt:
	@echo "watchd skeleton: no formatting targets implemented"

postgres-up:
	docker compose up --detach --wait postgres

postgres-down:
	docker compose down --volumes

postgres-logs:
	docker compose logs --follow postgres
