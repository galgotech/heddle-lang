# Heddle Build System

# Variables
BINARY_DIR=bin
GO=go
PROTOC=protoc
PROTO_DIR=sdk/go/proto
PROTO_FILES=$(PROTO_DIR)/worker.proto $(PROTO_DIR)/locality.proto

# Services (Main build target)
SERVICES=control-plane worker client lsp debug-adapter
RUST_SERVICES=relational-worker
EXAMPLES=calculator-example

.PHONY: all build clean test $(SERVICES) $(RUST_SERVICES) $(EXAMPLES) run-server submit proto

# Default target
all: build

proto:
	$(PROTOC) --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)

# Create bin directory
$(BINARY_DIR):
	mkdir -p $(BINARY_DIR)

# Build all services and examples
build: $(BINARY_DIR) $(SERVICES) $(RUST_SERVICES) $(EXAMPLES)
	@echo "All build targets complete."

# Individual Service Targets
control-plane: $(BINARY_DIR)
	@echo "Building Control Plane..."
	$(GO) build -o $(BINARY_DIR)/heddle-cp ./services/control-plane/cmd/control-plane

worker: $(BINARY_DIR)
	@echo "Building Worker Service..."
	$(GO) build -o $(BINARY_DIR)/heddle-worker ./services/worker/cmd/heddle-worker

client: $(BINARY_DIR)
	@echo "Building Client CLI..."
	$(GO) build -o $(BINARY_DIR)/heddle-client ./services/client/cmd/heddle-client

lsp: $(BINARY_DIR)
	@echo "Building LSP Server..."
	$(GO) build -o $(BINARY_DIR)/heddle-lsp ./services/lsp/cmd/heddle-lsp

debug-adapter: $(BINARY_DIR)
	@echo "Building Debug Adapter..."
	$(GO) build -o $(BINARY_DIR)/heddle-dap ./services/debug-adapter/cmd/heddle-dap

# Rust Service Targets
relational-worker: $(BINARY_DIR)
	@echo "Building Relational Worker (Rust)..."
	cd services/relational-worker && cargo build --release
	cp services/relational-worker/target/release/relational-worker $(BINARY_DIR)/heddle-relational-worker

# Individual Example Targets
calculator-example: $(BINARY_DIR)
	@echo "Building Calculator Example..."
	$(GO) build -o $(BINARY_DIR)/example-calculator ./sdk-examples/go/calculator

test-calculator:
	@echo "Testing Calculator Example..."
	$(GO) test -v ./sdk-examples/go/calculator

# Run Helpers
run-server: control-plane
	@echo "Starting Control Plane..."
	./$(BINARY_DIR)/heddle-cp

submit: client
	@if [ -z "$(FILE)" ]; then echo "Error: FILE variable is required. Usage: make submit FILE=path/to/file.he"; exit 1; fi
	@echo "Submitting $(FILE)..."
	./$(BINARY_DIR)/heddle-client submit $(FILE)

# Run all tests
test:
	@echo "Running tests across workspace..."
	$(GO) test ./...

# Clean build artifacts
clean:
	@echo "Cleaning up..."
	rm -rf $(BINARY_DIR)
	@echo "Done."

# Help target
help:
	@echo "Heddle Build Targets:"
	@echo "  make all           - Build all services"
	@echo "  make build         - Alias for all"
	@echo "  make control-plane - Build only the control plane"
	@echo "  make worker        - Build only the worker service"
	@echo "  make client        - Build only the client CLI"
	@echo "  make relational-worker - Build only the Rust relational worker"
	@echo "  make calculator-example - Build only the calculator example"
	@echo "  make test-calculator   - Run tests for the calculator example"
	@echo "  make run-server    - Build and start the control plane"
	@echo "  make submit FILE=f.he - Build and submit a heddle file"
	@echo "  make test          - Run all tests"
	@echo "  make clean         - Remove build artifacts"
