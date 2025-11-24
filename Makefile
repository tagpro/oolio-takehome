# Variables
PROMOCODES_DIR ?= promocodes
OUTPUT ?= valid_codes.txt
DB_PATH ?= ./food_ordering.db

.PHONY: setup
setup: precompute db/setup
	@echo "Setup complete."

.PHONY: precompute
precompute:
	@echo "Generating valid promo codes..."
	@echo "Input directory: $(PROMOCODES_DIR)"
	@echo "Output file: $(OUTPUT)"
	go run cmd/precompute/main.go --input $(PROMOCODES_DIR) --output $(OUTPUT)
	@echo "Done."

.PHONY: db/setup
db/setup:
	@echo "Setting up database..."
	DB_PATH=$(DB_PATH) go run cmd/db/main.go
	@echo "Done."

.PHONY: run
run:
	@echo "Starting API server..."
	DB_PATH=$(DB_PATH) go run cmd/server/main.go -promocodes $(OUTPUT)
	@echo "Done."

.PHONY: build
build:
	@echo "Building binaries..."
	go build -o bin/precompute ./cmd/precompute
	go build -o bin/dbsetup ./cmd/db
	go build -o bin/server ./cmd/server
	@echo "Done."

.PHONY: test
test:
	@echo "Running tests..."
	go test -race -covermode=atomic -coverprofile=cover.out -v ./...
	@echo "Done."

.PHONY: test/coverage
test/coverage: test
	@echo "Generating coverage report..."
	go tool cover -html=cover.out
	@echo "Done."

.PHONY: clean
clean:
	@echo "Cleaning up..."
	rm -rf bin/
	rm -f food.db
	rm -f valid_codes.txt
	rm -f cover.out
	@echo "Done."
