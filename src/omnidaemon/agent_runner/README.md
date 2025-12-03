# Agent Runner Architecture

This document explains how the OmniDaemon agent runner system works, including the supervisor pattern, callback adapters, and the complete event flow.

## Overview

The agent runner system enables **language-agnostic, process-isolated agent execution** within OmniDaemon's event-driven architecture. Agents run in separate processes, managed by supervisors, and communicate via stdio using a JSON protocol.

## Architecture Components

### 1. **BaseAgentRunner** (`runner.py`)
The core event-driven runner that:
- Subscribes to event bus topics
- Routes events to registered agent callbacks
- Tracks metrics and handles responses
- Manages webhook delivery and reply-to topics

### 2. **AgentSupervisor** (`agent_supervisor.py`)
Manages the lifecycle of a single agent process:
- **Launches** agent processes as subprocesses
- **Communicates** via stdio (stdin/stdout) using JSON protocol
- **Monitors** process health and automatically restarts on failure
- **Handles** request/response correlation using unique IDs
- **Manages** timeouts and error handling

### 3. **Python Callback Adapter** (`python_callback_adapter.py`)
A generic adapter that wraps any Python callback function:
- Dynamically imports Python modules
- Loads callback functions by name
- Runs in a separate process
- Communicates via stdio JSON protocol
- Works with any existing Python callback without code modification

### 3b. **TypeScript Callback Adapter** (`ts_callback_adapter`)
For Node ecosystems (TypeScript or JavaScript):
- Runs as a Node process that implements the same stdio JSON protocol
- Dynamically loads your compiled JavaScript module (built from TypeScript or plain JS)
- Invokes the exported callback you specify and streams responses back to the supervisor
- Emits structured logs to stderr so they appear in the OmniDaemon logs

> ℹ️ **Build requirement**: the adapter executes JavaScript files. Compile your `.ts` sources (e.g., `tsc` → `dist/index.js`) or configure `package.json` to point at an existing build artifact. The supervisor raises a clear error if it only finds `.ts` files.

### 4. **Directory-Based Discovery** (`agent_supervisor.py`)
Factory functions that automatically discover agents:
- **Detects language** from directory structure (Python, Go, etc.)
- **Finds callback functions** by searching through agent directories
- **Converts paths** to importable module paths
- **Creates supervisors** automatically

## Complete Event Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. Event Published to OmniDaemon Event Bus                     │
│    Topic: "file_system.tasks"                                   │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. BaseAgentRunner receives event                              │
│    - Enriches message with topic/agent metadata                 │
│    - Tracks "task_received" metric                              │
│    - Calls registered callback function                         │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. Callback Function (in agent runner)                         │
│    async def call_file_system_agent(message):                   │
│        supervisor = await create_supervisor_from_directory(...) │
│        return await supervisor.handle_event(message)            │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. AgentSupervisor                                              │
│    - Checks if agent process is running                         │
│    - Creates JSON envelope with unique request ID               │
│    - Writes to process stdin                                    │
│    - Waits for response on stdout                               │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. Python Callback Adapter Process                             │
│    - Reads JSON envelope from stdin                             │
│    - Dynamically imports the agent module                       │
│    - Calls the callback function with message payload           │
│    - Writes response JSON to stdout                             │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│ 6. Agent Implementation (in separate process)                   │
│    async def call_file_system_agent(message):                   │
│        # Your agent code here                                   │
│        result = await agent.run(message.get("content"))        │
│        return {"status": "success", "data": result}             │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│ 7. Response flows back through the chain                        │
│    Adapter → Supervisor → Callback → BaseAgentRunner           │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│ 8. BaseAgentRunner handles response                             │
│    - Saves result to storage                                    │
│    - Sends webhook (if configured)                             │
│    - Publishes to reply_to topic (if configured)               │
│    - Tracks "task_processed" metric                             │
└─────────────────────────────────────────────────────────────────┘
```

## Directory-Based Agent Discovery

The system supports **directory-based agent discovery**, where you only need to specify:
1. **Agent directory path** (e.g., `"agents/my_agent"`)
2. **Callback function name** (e.g., `"handle_request"`)

The system automatically:
- Detects the language (Python, Go, etc.)
- Searches the directory for the callback function
- Converts file paths to importable module paths
- Creates and starts the supervisor

### Example Agent Directory Structure

```
agents/
└── omnicore_agent/
    ├── __init__.py
    ├── utils.py              # Helper functions
    ├── agent_impl.py         # Agent implementation class
    └── callback.py           # Entry point callback function
        └── call_file_system_agent()  # ← This is what you specify
```

### How Discovery Works

1. **Language Detection**: Checks for `.py` files, `__init__.py`, or Go files
2. **Module Path Conversion**: Converts `agents/omnicore_agent` → `agents.omnicore_agent`
3. **Callback Search**: Recursively searches all `.py` files in the directory
4. **Function Validation**: Imports each module and checks if the callback exists
5. **Supervisor Creation**: Creates `AgentSupervisor` with the found module/function

## Usage Examples

### Basic Usage: Directory-Based Discovery (Lazy Initialization)

```python
from omnidaemon import OmniDaemonSDK, AgentConfig
from omnidaemon.agent_runner.agent_supervisor import (
    create_supervisor_from_directory,
    shutdown_all_supervisors,
)

sdk = OmniDaemonSDK()

async def my_agent_callback(message: dict):
    """Callback registered with OmniDaemon"""
    # Create/get supervisor for this agent (lazy - on first event)
    supervisor = await create_supervisor_from_directory(
        agent_name="my_agent",
        agent_dir="agents/my_agent",           # Directory path
        callback_function="handle_request",    # Function name
    )
    # Forward event to supervised agent process
    return await supervisor.handle_event(message)

# Register with OmniDaemon
await sdk.register_agent(
    agent_config=AgentConfig(
        topic="my.topic",
        callback=my_agent_callback,
    )
)
```

**Note**: With lazy initialization, the first event will experience a delay as:
- Supervisor is created
- Agent process is started
- Agent initializes

### Recommended: Eager Initialization (Pre-start Agents)

To avoid first-request delays, pre-initialize supervisors **before** registering with OmniDaemon:

```python
from omnidaemon import OmniDaemonSDK, AgentConfig
from omnidaemon.agent_runner.agent_supervisor import (
    create_supervisor_from_directory,
    shutdown_all_supervisors,
)

sdk = OmniDaemonSDK()

# Pre-initialize supervisor (agent process starts immediately)
_my_agent_supervisor = None

async def _get_my_agent_supervisor():
    global _my_agent_supervisor
    if _my_agent_supervisor is None:
        _my_agent_supervisor = await create_supervisor_from_directory(
            agent_name="my_agent",
            agent_dir="agents/my_agent",
            callback_function="handle_request",
        )
    return _my_agent_supervisor

async def my_agent_callback(message: dict):
    """Callback registered with OmniDaemon"""
    # Supervisor already exists - just get it
    supervisor = await _get_my_agent_supervisor()
    return await supervisor.handle_event(message)

async def main():
    # Pre-initialize BEFORE registering
    logger.info("Pre-initializing agent supervisors...")
    await _get_my_agent_supervisor()  # Process starts here
    logger.info("Agent processes are running and ready")
    
    # Now register - agents are already running
    await sdk.register_agent(
        agent_config=AgentConfig(
            topic="my.topic",
            callback=my_agent_callback,
        )
    )
    
    await sdk.start()
    # ... rest of your code
```

### Advanced Usage: Manual Supervisor Configuration

```python
from omnidaemon.agent_runner.agent_supervisor import (
    AgentSupervisor,
    AgentProcessConfig,
)

# Create supervisor with custom configuration
config = AgentProcessConfig(
    name="my_agent",
    command="python",
    args=["-m", "omnidaemon.agent_runner.python_callback_adapter",
          "--module", "my_module",
          "--function", "my_callback"],
    request_timeout=60.0,
    restart_on_exit=True,
    max_restart_attempts=3,
)

supervisor = AgentSupervisor(config)
await supervisor.start()

# Use supervisor
result = await supervisor.handle_event({"content": "Hello"})
```

## Agent Implementation Requirements

### Python Agents

Your agent must have a callback function that:
- Is `async def`
- Takes one parameter: `message: Dict[str, Any]`
- Returns a `Dict[str, Any]` (or `None`)
- Include a `requirements.txt` or `pyproject.toml` in the agent directory—OmniDaemon will automatically `pip install` those dependencies into an isolated folder before launching the agent process.

```python
# agents/my_agent/callback.py
async def handle_request(message: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point callback function"""
    content = message.get("content", "")
    # Your agent logic here
    result = process(content)
    return {"status": "success", "data": result}
```

### Directory Structure

```
agents/
└── my_agent/
    ├── __init__.py          # Package marker
    ├── callback.py          # Entry point (contains callback function)
    ├── agent_impl.py        # Agent implementation
    └── utils.py             # Helper functions
```

### TypeScript Agents (JavaScript-only projects are not supported)

- Your agent code must be written in TypeScript (`.ts`/`.tsx`) and include a `tsconfig.json`.
- Export an async (Promise-returning) function that receives the JSON payload the supervisor forwards.
- Compile your TypeScript sources to JavaScript (for example: `npm run build` → `dist/index.js`). The adapter loads the compiled file.
- The supervisor looks for an entry file in this order:
  1. `package.json` fields: `omnidaemonEntry`, `module`, `main`, `exports`
  2. Common build artifacts such as `dist/index.js`, `build/callback.js`, `index.js`
- Ship your `package.json` + lockfile alongside the callback—OmniDaemon installs dependencies into an isolated folder before startup, so you don’t need to commit `node_modules/`.
- Provide the exported callback name when using `create_supervisor_from_directory`. The adapter accepts named or default exports.
- OmniDaemon automatically runs `npm/yarn/pnpm install` (based on your lock file) and your build step (`npm run build` or `tsc`) before launching the agent. Plain JavaScript projects (without TypeScript sources) will be rejected.

## Communication Protocol

The supervisor and agent process communicate via **stdio JSON protocol**:

### Request Format (Supervisor → Agent)

```json
{
  "id": "unique-request-id",
  "type": "task",
  "payload": {
    "content": "...",
    "webhook": "...",
    "reply_to": "...",
    ...
  }
}
```

### Response Format (Agent → Supervisor)

```json
{
  "id": "unique-request-id",
  "status": "ok",
  "result": {
    "status": "success",
    "data": "..."
  }
}
```

### Error Response

```json
{
  "id": "unique-request-id",
  "status": "error",
  "error": "Error message"
}
```

## Supervisor Features

### Automatic Restart
- Monitors process health
- Automatically restarts on unexpected exit
- Configurable max restart attempts
- Exponential backoff between restarts

### Request Correlation
- Each request gets a unique ID
- Responses are matched to requests
- Pending requests are rejected if process restarts

### Timeout Handling
- Configurable per-request timeout
- Automatic cleanup of timed-out requests
- Error propagation to caller

### Process Isolation
- Each agent runs in its own process
- No shared memory or state
- Clean shutdown on supervisor stop

## Language Support

### Currently Supported
- **Python**: via `python_callback_adapter`
- **Go**: via `go_callback_adapter`
- **TypeScript / JavaScript**: via `ts_callback_adapter`

### Extending
- Implement a stdio adapter for any additional language and register it inside `create_supervisor_from_directory`.

## Key Benefits

1. **Language Agnostic**: Agents can be written in any language
2. **Process Isolation**: Agents run in separate processes for stability
3. **No Code Modification**: Existing callbacks work without changes
4. **Automatic Discovery**: Just provide directory + function name
5. **Automatic Recovery**: Supervisors restart failed agents
6. **Event-Driven**: Fully integrated with OmniDaemon's event bus

## Error Handling

- **Process Crashes**: Automatically restarted (if configured)
- **Request Timeouts**: Propagated to caller with timeout error
- **Invalid Responses**: Logged and error returned to caller
- **Import Errors**: Raised during supervisor creation
- **Missing Callbacks**: Raised during directory discovery

## Shutdown

Always call `shutdown_all_supervisors()` during cleanup:

```python
try:
    # ... your code ...
finally:
    await sdk.shutdown()
    await shutdown_all_supervisors()  # Clean shutdown of all agents
```

This ensures:
- All agent processes are terminated cleanly
- Pending requests are handled
- Resources are freed

## Summary

The agent runner system provides a robust, language-agnostic way to run agents in OmniDaemon:

1. **Register callbacks** with OmniDaemon (as usual)
2. **Callbacks delegate** to supervisors
3. **Supervisors manage** agent processes
4. **Adapters bridge** the communication gap
5. **Agents run** in isolated processes
6. **Responses flow** back through the chain

All of this happens automatically - you just provide the directory path and callback function name!

