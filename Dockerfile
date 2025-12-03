# Multi-stage Dockerfile for OmniDaemon with all agent support
FROM python:3.11-slim as base

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Go
RUN curl -L https://go.dev/dl/go1.23.3.linux-amd64.tar.gz | tar -C /usr/local -xzf - && \
    mkdir -p /go/bin /go/src /go/pkg

# Set Go environment
ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOPATH=/go
ENV GOROOT=/usr/local/go

# Install Node.js (for MCP tools)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    corepack enable

# Install uv (fast Python package manager)
RUN pip install --no-cache-dir uv

# Set working directory
WORKDIR /app

# Copy all files including .git (needed for uv-dynamic-versioning)
# We copy everything first so Git history is available for version detection
COPY . .

# Install Python dependencies
RUN uv pip install --system -e .

# Build Go callback adapter (core infrastructure, not an agent)
# This is part of the OmniDaemon system, not a user agent
WORKDIR /app/src/omnidaemon/agent_runner/go_callback_adapter
RUN go mod download && (go build -o /usr/local/bin/go_callback_adapter . || echo "Go adapter build failed - will build at runtime")

# Build TypeScript callback adapter (core infrastructure, not an agent)
# This is part of the OmniDaemon system, not a user agent
WORKDIR /app/src/omnidaemon/agent_runner/ts_callback_adapter
RUN if [ -f "package.json" ]; then \
        npm install && \
        (npm run build && echo "TypeScript adapter built successfully" || echo "TypeScript adapter build failed - will build at runtime"); \
    else \
        echo "ts_callback_adapter package.json not found - skipping build"; \
    fi

# NOTE: Individual agents (Go, TypeScript, Python) are NOT built here.
# They are built/installed at runtime by the supervisor in their own isolated directories.
# This ensures:
# - Each agent has its own dependencies (go.mod, package.json, requirements.txt)
# - Agents are isolated from each other
# - Agents can be added/removed without rebuilding the Docker image

# Set back to app root
WORKDIR /app

# Expose API port
EXPOSE 8765

# Default command (can be overridden)
CMD ["python", "-m", "examples.omnicoreagent_dir.supervised_agent_runner"]

