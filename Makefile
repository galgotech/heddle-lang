# Heddle Build System

# Variables
BINARY_DIR=bin
GO=go
NPM=npm

.PHONY: all build clean test heddle run-server submit docs-serve docs-build vscode\:prepublish vsce\:package vsce\:publish

# Default target
all: build

# Create bin directory
$(BINARY_DIR):
	mkdir -p $(BINARY_DIR)

# Build all services and examples
build: $(BINARY_DIR) heddle
	@echo "All build targets complete."

# Consolidated Heddle CLI (Local)
heddle: $(BINARY_DIR)
	@echo "Building Heddle CLI..."
	$(GO) build -o $(BINARY_DIR)/heddle ./cmd

# Run all tests
test:
	@echo "Testing Heddle Core..."
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
	@echo "  make heddle             - Build only the heddle cli"
	@echo "  make test               - Run all tests across workspace"
	@echo "  make docs-serve         - Start MkDocs development server"
	@echo "  make docs-build         - Build MkDocs static documentation"
	@echo "  make clean              - Remove build artifacts everywhere"
