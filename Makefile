# Heddle Build System

# Variables
BINARY_DIR=bin
GO=go

# Services (Main build target)
SERVICES=heddle heddle-plugin-go
RUST_SERVICES=relational-worker
EXAMPLES=calculator-example

.PHONY: all build clean test $(SERVICES) $(RUST_SERVICES) $(EXAMPLES) run-server submit docs-serve docs-build

# Default target
all: build

# Create bin directory
$(BINARY_DIR):
	mkdir -p $(BINARY_DIR)

# Build all services and examples
build: $(BINARY_DIR) $(SERVICES) $(RUST_SERVICES) $(EXAMPLES)
	@echo "All build targets complete."

# Consolidated Heddle CLI
heddle: $(BINARY_DIR)
	@echo "Building Heddle CLI..."
	$(GO) build -o $(BINARY_DIR)/heddle ./cmd

# Heddle standard library (go)
heddle-plugin-std: $(BINARY_DIR)
	@echo "Building Go SDK Plugin..."
	$(GO) build -o $(BINARY_DIR)/heddle-plugin-std ./sdk/go/cmd

# Individual Example Targets
calculator-example: $(BINARY_DIR)
	@echo "Building Calculator Example..."
	$(GO) build -o $(BINARY_DIR)/example-calculator ./sdk-examples/go/calculator

test-calculator:
	@echo "Testing Calculator Example..."
	$(GO) test -v ./sdk-examples/go/calculator

# Run Helpers
run-server: heddle
	@echo "Starting Control Plane..."
	./$(BINARY_DIR)/heddle cp

submit: heddle
	@if [ -z "$(FILE)" ]; then echo "Error: FILE variable is required. Usage: make submit FILE=path/to/file.he"; exit 1; fi
	@echo "Submitting $(FILE)..."
	./$(BINARY_DIR)/heddle client submit $(FILE)

# Run all tests
test:
	@echo "Running tests across workspace..."
	$(GO) test ./...

# Documentation
docs-serve:
	@echo "Starting MkDocs server..."
	mkdocs serve

docs-build:
	@echo "Building documentation..."
	mkdocs build

# Clean build artifacts
clean:
	@echo "Cleaning up..."
	rm -rf $(BINARY_DIR)
	rm -rf site
	@echo "Done."

# Help target
help:
	@echo "Heddle Build Targets:"
	@echo "  make all           	 - Build all services"
	@echo "  make build         	 - Alias for all"
	@echo "  make relational-worker  - Build only the Rust relational worker"
	@echo "  make calculator-example - Build only the calculator example"
	@echo "  make test-calculator    - Run tests for the calculator example"
	@echo "  make run-server         - Build and start the control plane"
	@echo "  make submit FILE=f.he   - Build and submit a heddle file"
	@echo "  make test               - Run all tests"
	@echo "  make docs-serve         - Start MkDocs development server"
	@echo "  make docs-build         - Build MkDocs static documentation"
	@echo "  make clean              - Remove build artifacts"
