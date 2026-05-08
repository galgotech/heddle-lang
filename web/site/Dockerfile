FROM node:22-bookworm

# Install Go 1.24
RUN curl -OL https://go.dev/dl/go1.24.0.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.24.0.linux-amd64.tar.gz && \
    rm go1.24.0.linux-amd64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin

# Install Hugo Extended (v0.161.1)
RUN curl -L https://github.com/gohugoio/hugo/releases/download/v0.161.1/hugo_extended_0.161.1_linux-amd64.deb -o hugo.deb && \
    apt-get update && \
    apt-get install -y ./hugo.deb && \
    rm hugo.deb && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency files first
COPY package.json package-lock.json ./
RUN npm install

# Copy Go module files
COPY go.mod go.sum ./
RUN go mod download || true

# Copy the rest of the application
COPY . .

# Expose Hugo port
EXPOSE 1313

# Start the development server (binding to all interfaces for Docker)
CMD ["npm", "run", "dev:docker"]
