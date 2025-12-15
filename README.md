# 🌐 OmniDaemon

<div align="center">

### **Universal Event-Driven Runtime Engine for AI Agents**

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Redis](https://img.shields.io/badge/redis-6.0+-red.svg)](https://redis.io/)
[![Coverage](https://img.shields.io/badge/coverage-84.59%25-green)](https://github.com/omnirexflora-labs/OmniDaemon)

**Run any AI agent. Any framework. One event-driven control plane.**

*Created by [Abiola Adeshina](https://github.com/Abiorh001) • From the team behind [OmniCore Agent](https://github.com/omnirexflora-labs/omnicoreagent)*

[📚 Docs](https://docs-omnidaemon.omnirexfloralabs.com/docs) • [💡 Examples](examples/) • [🏗️ Architecture](docs/architecture/) • [🚀 Quick Start](#-quick-start)

</div>

---

## 🎯 What is OmniDaemon?

> **"Kubernetes for AI Agents"** - A production-ready runtime that makes AI agents autonomous, observable, and fault-tolerant.

OmniDaemon runs **each agent in its own isolated process** (like containers), managed by an Agent Supervisor. This means your agents get production-grade reliability out of the box: process isolation, auto-restart, crash protection, and event-driven communication.

**In 5 seconds:**
- 🤖 **Run any AI agent** (OmniCoreAgent, Google ADK, LangChain, CrewAI, AutoGen, custom)
- 📨 **Event-driven** (agents react to events, not HTTP requests)
- 🔒 **Process isolation** (one agent crash won't kill others)
- 🔄 **Auto-recovery** (supervisors restart crashed agents)
- 🚀 **Production-ready** (retries, DLQ, metrics, scaling built-in)

> 💡 **Why OmniDaemon exists:** Most AI frameworks run everything in a single Python process. One crash kills your entire system. OmniDaemon solves this with process isolation and event-driven architecture. [Read the full story →](docs/index.mdx)

---

## 🏭 Agent Supervisor: Production-Grade Process Isolation

The **Agent Supervisor** is OmniDaemon's core innovation. It manages each agent in an isolated process with automatic lifecycle management, error recovery, and health monitoring.

### Benefits

| Feature | What You Get |
|---------|-------------|
| 🔒 **Fault Isolation** | Agent A crashes? Agent B keeps running. |
| 🔄 **Auto-Recovery** | Crashed agents restart automatically. |
| 💾 **Resource Safety** | Clean memory/CPU boundaries per agent. |
| 🎯 **Process Per Agent** | Like Kubernetes pods for AI agents. |
| ❤️ **Health Monitoring** | Heartbeat checks, timeout handling. |
| 📊 **Observability** | Metrics, logs, state tracking built-in. |

### Lifecycle State Machine

```mermaid
stateDiagram-v2
    [*] --> IDLE: Created
    IDLE --> STARTING: start()
    STARTING --> RUNNING: Process ready
    STARTING --> CRASHED: Start failed
    RUNNING --> STOPPING: stop()
    RUNNING --> CRASHED: Process died
    STOPPING --> STOPPED: Graceful shutdown
    CRASHED --> STARTING: Auto-restart
    CRASHED --> STOPPED: Max retries exceeded
    STOPPED --> [*]
```

> 📚 **Deep dive:** [Agent Supervisor Architecture →](docs/architecture/agent-supervisor.mdx)

---

## 🚀 Quick Start

Get OmniDaemon running in **5 minutes** with production-ready process isolation:

### 1. Install Redis (Event Bus)

```bash
# Using Docker (easiest)
docker run -d -p 6379:6379 --name redis redis:latest

# Verify
redis-cli ping  # Should return: PONG
```

### 2. Install OmniDaemon

```bash
# Using uv (recommended - 10-100x faster)
uv add omnidaemon

# Or using pip
pip install omnidaemon
```

### 3. Create Your Agent

```bash
# Create agent directory
mkdir my_first_agent
touch my_first_agent/__init__.py
```

**Create agent** (`my_first_agent/agent.py`):
```python
def greeter_callback(message: dict):
    """Your agent runs here - in an isolated process!"""
    name = message.get("content", {}).get("name", "stranger")
    return {"reply": f"Hello, {name}! 👋"}
```

**Create runner** (`agent_runner.py`):
```python
import asyncio
from omnidaemon import OmniDaemonSDK, AgentConfig
from omnidaemon.supervisor import create_supervisor_from_directory

sdk = OmniDaemonSDK()

async def main():
    # Create supervisor (runs agent in separate process)
    supervisor = await create_supervisor_from_directory(
        agent_name="greeter",
        agent_dir="./my_first_agent",
        callback_function="agent.greeter_callback"
    )
    
    await sdk.register_agent(
        agent_config=AgentConfig(topic="greet.user"),
        callback=supervisor.handle_event
    )
    
    await sdk.start()
    print("🎧 Agent running. Press Ctrl+C to stop.")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await sdk.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```

### 4. Run It!

```bash
python agent_runner.py
```

**🎉 Your AI agent is now running in an isolated process with auto-restart!**

> 📚 **Next steps:** [Full Tutorial →](docs/getting-started/quick-start.mdx) | [Complete Examples →](examples/)

---

## 💡 Examples

See production-ready implementations in the [`examples/`](examples/) directory:

### Featured Examples

1. **[Multiple Agents with Supervisors](examples/agents_with_supervisors/)** - Run multiple supervised agents
2. **[Google ADK Integration](examples/google_adk_with_supervisor/)** - Full ADK agent with MCP tools
3. **[Content Moderation](examples/content_moderation/)** - Multi-agent pipeline
4. **[FastAPI Integration](examples/fast_api_impl.py)** - HTTP + event-driven hybrid

Each example shows real-world patterns you'll use in production.

---

## 📚 Documentation

### Core Concepts
- [Why OmniDaemon?](docs/core-concepts/vs-other-frameworks.mdx) - The single process trap
- [Event-Driven Architecture](docs/index.mdx) - Why EDA for AI agents
- [Key Features](docs/core-concepts/) - Deep dive into capabilities

### Architecture
- [Agent Supervisor](docs/architecture/agent-supervisor.mdx) - Process isolation & lifecycle
- [System Overview](docs/architecture/system-overview.mdx) - How everything fits together

### How-To Guides
- [Common Patterns](docs/how-to-guides/common-patterns.mdx) - Production recipes
- [Monitoring & Metrics](docs/how-to-guides/monitoring.mdx) - Observability
- [Multi-Agent Systems](docs/how-to-guides/) - Coordination patterns

### API Reference
- [Python SDK](docs/api-reference/sdk-api.mdx) - Complete SDK docs
- [CLI Reference](docs/api-reference/cli-reference.mdx) - Command-line tools

---

## 🎯 When to Use OmniDaemon

### ✅ Perfect For
- **Background AI Agents** - Autonomous agents reacting to events
- **Event-Driven Workflows** - Multi-step pipelines coordinated through events
- **Distributed Multi-Agent Systems** - Agents across different servers/runtimes
- **Long-Running AI Tasks** - Workloads that shouldn't block client requests
- **Enterprise AI Ops** - Production systems with retries, logs, monitoring

### ❌ Overkill For
- **Simple HTTP APIs** - Use FastAPI/Flask instead
- **Pure Real-Time Chat Only** - WebSockets/SSE alone are simpler
- **Single-Shot Scripts** - A basic Python script is sufficient

### 🆚 vs Alternatives

| Tool | Use Case | vs OmniDaemon |
|------|----------|---------------|
| **Celery** | Task queues | ❌ Not AI-first, complex setup, no agent abstraction |
| **AWS Lambda** | Serverless functions | ❌ Cold starts, time limits, vendor lock-in |
| **Temporal** | Workflow engine | ❌ Heavy, complex, not AI-optimized |
| **Airflow** | DAG orchestration | ❌ Batch-oriented, not real-time events |
| **OmniDaemon** | AI Agent Runtime | ✅ AI-first, event-driven, any framework, production-ready |

---

## 🔌 Pluggable Architecture

Switch backends by changing environment variables - **zero code changes!**

```bash
# Event Bus
EVENT_BUS_TYPE=redis_stream + REDIS_URL=redis://localhost:6379     # Current ✅
EVENT_BUS_TYPE=kafka + KAFKA_SERVERS=localhost:9092                 # Coming soon 🚧
EVENT_BUS_TYPE=rabbitmq + RABBITMQ_URL=amqp://localhost             # Coming soon 🚧

# Storage
STORAGE_BACKEND=redis + REDIS_URL=redis://localhost:6379            # Production ✅
STORAGE_BACKEND=json + JSON_STORAGE_DIR=./data                      # Development ✅
STORAGE_BACKEND=postgresql + POSTGRES_URL=postgresql://...          # Coming soon 🚧
STORAGE_BACKEND=mongodb + MONGODB_URI=mongodb://...                 # Coming soon 🚧
```

---

## 🌟 Resources

- 📖 [**Full Documentation**](https://docs-omnidaemon.omnirexfloralabs.com/docs) - Complete guides and API reference
- 💡 [**Examples**](examples/) - Production-ready code samples
- 🏗️ [**Architecture**](docs/architecture/) - Deep dive into system design
- 🤝 [**Community**](docs/community/support.mdx) - Get help and contribute
- 🐛 [**Issues**](https://github.com/omnirexflora-labs/OmniDaemon/issues) - Report bugs or request features

---

## 📊 Features

| Feature | Status |
|---------|--------|
| 🔒 Process Isolation (Agent Supervisor) | ✅ Production |
| 📨 Event-Driven Architecture | ✅ Production |
| 🔄 Auto-Retry & DLQ | ✅ Production |
| 📊 Metrics & Monitoring | ✅ Production |
| 🎛️ CLI & HTTP API | ✅ Production |
| 🔌 Redis Event Bus | ✅ Production |
| 💾 Redis Storage | ✅ Production |
| 📁 JSON Storage | ✅ Production |
| ⚡ Kafka Event Bus | 🚧 Coming Soon |
| 🐰 RabbitMQ Event Bus | 🚧 Coming Soon |
| 🗄️ PostgreSQL Storage | 🚧 Coming Soon |
| 🍃 MongoDB Storage | 🚧 Coming Soon |

---

## 👨‍💻 About

**Created by [Abiola Adeshina](https://github.com/Abiorh001) and the OmniDaemon Team**

*From the creators of [OmniCore Agent](https://github.com/omnirexflora-labs/omnicoreagent) — building the future of event-driven AI systems*

---

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details

---

[⭐ Star on GitHub](https://github.com/omnirexflora-labs/OmniDaemon) • [🐛 Report Bug](https://github.com/omnirexflora-labs/OmniDaemon/issues) • [💡 Request Feature](https://github.com/omnirexflora-labs/OmniDaemon/issues)
