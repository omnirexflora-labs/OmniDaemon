# Docker Setup for OmniDaemon

This guide explains how to run OmniDaemon with all agents (Python OmniCore, Google ADK, and Go) using Docker.

## Prerequisites

- Docker and Docker Compose installed
- API keys for your agents (OpenAI, Google, etc.)

## Quick Start

### 1. Set Up Environment Variables

Copy the example environment file and add your API keys:

```bash
cp .env.example .env
# Edit .env and add your API keys
```

### 2. Build and Start Services

```bash
# Build and start all services
docker-compose up --build

# Or run in detached mode
docker-compose up -d --build
```

### 3. Check Logs

```bash
# View all logs
docker-compose logs -f

# View only OmniDaemon logs
docker-compose logs -f omnidaemon

# View only Redis logs
docker-compose logs -f redis
```

### 4. Test the Agents

Once the services are running, you can test the agents:

```bash
# Test OmniCore agent
docker-compose exec omnidaemon python -m examples.omnicoreagent_dir.filesystem_publisher "List files"

# Test Google ADK agent
docker-compose exec omnidaemon python -m examples.omnicoreagent_dir.google_adk_publisher "List files"

# Test Go agent
docker-compose exec omnidaemon python -m examples.omnicoreagent_dir.go_agent_publisher "Hello from Docker!"
```

## Services

### Redis
- **Port**: 6379
- **Purpose**: Event bus for OmniDaemon
- **Health Check**: Enabled

### OmniDaemon Agent Runner
- **Port**: 8765 (API)
- **Purpose**: Main agent runner with all three agents
- **Agents**:
  - OmniCore (Python)
  - Google ADK (Python)
  - Go Agent

### Redis Commander (Optional)
- **Port**: 8081
- **Purpose**: Web UI for Redis debugging
- **Start**: `docker-compose --profile debug up redis-commander`

## Development

### Rebuild After Code Changes

```bash
# Rebuild and restart
docker-compose up --build

# Or just restart (if no code changes)
docker-compose restart omnidaemon
```

### Access Container Shell

```bash
# Get shell in OmniDaemon container
docker-compose exec omnidaemon bash

# Test Go installation
docker-compose exec omnidaemon go version

# Test Python
docker-compose exec omnidaemon python --version
```

### View Go Agent Build

```bash
# Check if Go agent built successfully
docker-compose exec omnidaemon ls -la /tmp/go_agent

# Manually build Go agent
docker-compose exec omnidaemon bash -c "cd /app/examples/omnicoreagent_dir/agents/go_agent && go build ."
```

## Troubleshooting

### Go Agent Not Building

If the Go agent fails to build, check:

```bash
# Check Go environment
docker-compose exec omnidaemon go env

# Check Go agent directory
docker-compose exec omnidaemon ls -la /app/examples/omnicoreagent_dir/agents/go_agent/

# Try manual build
docker-compose exec omnidaemon bash -c "cd /app/examples/omnicoreagent_dir/agents/go_agent && go mod tidy && go build ."
```

### Redis Connection Issues

```bash
# Check Redis is running
docker-compose ps redis

# Test Redis connection
docker-compose exec omnidaemon redis-cli -h redis ping
```

### API Not Accessible

```bash
# Check if API is running
curl http://localhost:8765/health

# Check container logs
docker-compose logs omnidaemon | grep API
```

## Clean Up

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clears all data)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```

## Production Considerations

For production use:

1. **Use .env file** for secrets (never commit it)
2. **Set up proper networking** (not bridge mode)
3. **Add health checks** for all services
4. **Use Docker secrets** for API keys
5. **Set resource limits** in docker-compose.yml
6. **Use a reverse proxy** (nginx/traefik) for the API

## Architecture

```
┌─────────────────┐
│   OmniDaemon    │
│  Agent Runner   │
│                 │
│  ┌───────────┐  │
│  │ OmniCore  │  │
│  │  (Python) │  │
│  └───────────┘  │
│  ┌───────────┐  │
│  │Google ADK │  │
│  │  (Python) │  │
│  └───────────┘  │
│  ┌───────────┐  │
│  │ Go Agent  │  │
│  └───────────┘  │
└────────┬────────┘
         │
         ▼
    ┌────────┐
    │ Redis  │
    │ Stream │
    └────────┘
```

