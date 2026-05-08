# Hugo Site Makefile

# Variables
IMAGE_NAME=heddle-site
CONTAINER_NAME=heddle-site-srv
PORT=1313

.PHONY: help build run docker-build docker-run docker-stop docker-clean clean

help:
	@echo "Available targets:"
	@echo "  make build         - Build the site locally"
	@echo "  make run           - Run the site locally"
	@echo "  make docker-build  - Build the Docker image"
	@echo "  make docker-run    - Run the site in a Docker container"
	@echo "  make docker-stop   - Stop the Docker container"
	@echo "  make docker-clean  - Remove the Docker image"
	@echo "  make clean         - Clean local build artifacts"

build:
	npm run build

run:
	npm run dev

docker-build:
	docker build -t $(IMAGE_NAME) .

docker-run:
	@echo "Starting Hugo site in Docker at http://localhost:$(PORT)"
	docker run -it --rm \
		-p $(PORT):$(PORT) \
		--name $(CONTAINER_NAME) \
		$(IMAGE_NAME)

docker-stop:
	docker stop $(CONTAINER_NAME) || true

docker-clean:
	docker rmi $(IMAGE_NAME) || true

clean:
	rm -rf public resources
