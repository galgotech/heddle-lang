# Heddle Build System

# Variables
BINARY_DIR=bin
GO=go

# Services (Main build target)
SERVICES=control-plane worker client
EXAMPLES=calculator-example

.PHONY: all build clean test $(SERVICES) $(EXAMPLES) run-server submit

# Default target
all: build

# Create bin directory
$(BINARY_DIR):
	mkdir -p $(BINARY_DIR)

# Build all services and examples
build: $(BINARY_DIR) $(SERVICES) $(EXAMPLES)
	@echo "All build targets complete."

# Individual Service Targets
control-plane: $(BINARY_DIR)
	@echo "Building Control Plane..."
	$(GO) build -o $(BINARY_DIR)/heddle-cp ./services/control-plane

worker: $(BINARY_DIR)
	@echo "Building Worker Service..."
	$(GO) build -o $(BINARY_DIR)/heddle-worker ./services/worker

client: $(BINARY_DIR)
	@echo "Building Client CLI..."
	$(GO) build -o $(BINARY_DIR)/heddle-client ./services/client

# Individual Example Targets
calculator-example: $(BINARY_DIR)
	@echo "Building Calculator Example..."
	$(GO) build -o $(BINARY_DIR)/example-calculator ./sdk-examples/go/calculator

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
	@echo "  make calculator-example - Build only the calculator example"
	@echo "  make run-server    - Build and start the control plane"
	@echo "  make submit FILE=f.he - Build and submit a heddle file"
	@echo "  make test          - Run all tests"
	@echo "  make clean         - Remove build artifacts"
