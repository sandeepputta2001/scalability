.PHONY: up down tidy build seed logs

up:
	docker compose up -d --build

down:
	docker compose down -v

tidy:
	go mod tidy

build:
	go build ./...

# Seed sample products into MongoDB
seed:
	@curl -s -X POST http://localhost:8080/api/v1/auth/register \
		-H "Content-Type: application/json" \
		-d '{"email":"admin@example.com","password":"password123","name":"Admin"}' | jq .
	@TOKEN=$$(curl -s -X POST http://localhost:8080/api/v1/auth/login \
		-H "Content-Type: application/json" \
		-d '{"email":"admin@example.com","password":"password123"}' | jq -r .token); \
	curl -s -X POST http://localhost:8080/api/v1/products \
		-H "Authorization: Bearer $$TOKEN" \
		-H "Content-Type: application/json" \
		-d '{"name":"MacBook Pro","description":"Apple laptop","price":2499.99,"stock":50,"category":"electronics","tags":["laptop","apple"]}' | jq .; \
	curl -s -X POST http://localhost:8080/api/v1/products \
		-H "Authorization: Bearer $$TOKEN" \
		-H "Content-Type: application/json" \
		-d '{"name":"Sony WH-1000XM5","description":"Noise cancelling headphones","price":349.99,"stock":200,"category":"electronics","tags":["headphones","sony"]}' | jq .

logs:
	docker compose logs -f api

health:
	curl -s http://localhost:8080/health | jq .
