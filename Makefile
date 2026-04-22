# Heddle Build System

# Variables
BINARY_DIR=bin
GO=go

# Services (Main build target)
SERVICES=control-plane worker
EXAMPLES=calculator-example

.PHONY: all build clean test $(SERVICES) $(EXAMPLES)

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

# Individual Example Targets
calculator-example: $(BINARY_DIR)
	@echo "Building Calculator Example..."
	$(GO) build -o $(BINARY_DIR)/example-calculator ./sdk-examples/go/calculator

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
	@echo "  make calculator-example - Build only the calculator example"
	@echo "  make test          - Run all tests"
	@echo "  make clean         - Remove build artifacts"
