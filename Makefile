.PHONY: build test fmt vet tidy up down logs demo

build:
	go build ./...

test:
	go test ./... -v

fmt:
	gofmt -l .

vet:
	go vet ./...

tidy:
	go mod tidy

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f consumer notifier

# demo posts three sample orders to the running producer: one small enough
# to skip the bulk discount, one large enough to trigger it, and one
# that's deliberately invalid -- watch `make logs` in another terminal to
# see all three show up on "orders.processed" via cmd/notifier, including
# the rejected one.
demo:
	curl -s -X POST localhost:8080/orders \
		-H 'Content-Type: application/json' \
		-d '{"customer_id":"cust-1","items":[{"sku":"widget","quantity":2,"unit_price_cents":500}]}'
	@echo
	curl -s -X POST localhost:8080/orders \
		-H 'Content-Type: application/json' \
		-d '{"customer_id":"cust-2","items":[{"sku":"widget","quantity":5,"unit_price_cents":5000}]}'
	@echo
	curl -s -X POST localhost:8080/orders \
		-H 'Content-Type: application/json' \
		-d '{"customer_id":"cust-3","items":[]}'
	@echo
