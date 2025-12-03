# Complete Agent Process Flow

This document details the complete flow from OmniDaemon event to agent response, including dependency management, process startup, and JSON communication.

## Process Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Agent Runner Process (PID: 12345)                          │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  OmniDaemon SDK                                       │  │
│  │  - Event Bus (Redis)                                  │  │
│  │  - Agent Registration                                 │  │
│  └───────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Registered Callback                                  │  │
│  │  async def call_omnicore_agent(message):              │  │
│  │      supervisor = await get_supervisor()               │  │
│  │      return await supervisor.handle_event(message)     │  │
│  └───────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  AgentSupervisor                                       │  │
│  │  - Manages subprocess lifecycle                       │  │
│  │  - Handles stdio communication                         │  │
│  │  - Restarts on crash                                   │  │
│  └───────────────────────────────────────────────────────┘  │
│                    │                                        │
│                    │ stdin/stdout/stderr pipes              │
│                    ▼                                        │
└─────────────────────────────────────────────────────────────┘
                    │
                    │ asyncio.create_subprocess_exec()
                    │ (Separate Process!)
                    ▼
┌─────────────────────────────────────────────────────────────┐
│  Agent Process (PID: 12346) - SEPARATE PROCESS              │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Python Callback Adapter                              │  │
│  │  - Reads JSON from stdin                              │  │
│  │  - Imports callback module                            │  │
│  │  - Calls callback function                            │  │
│  │  - Writes JSON to stdout                              │  │
│  └───────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Agent Code (callback.py)                             │  │
│  │  - Has access to dependencies via PYTHONPATH            │  │
│  │  - Runs in isolated process                            │  │
│  │  - Can crash without affecting runner                  │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**Key Points:**
- ✅ **Different PIDs**: Runner (12345) vs Agent (12346) - completely separate processes
- ✅ **stdio Pipes**: Communication via stdin/stdout/stderr (no network, no shared memory)
- ✅ **Isolation**: Agent crashes don't affect runner, memory is separate
- ✅ **Adapter Pattern**: The adapter bridges stdio JSON ↔ Python callback function

## Complete Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    participant EB as Event Bus<br/>(Redis)
    participant Runner as OmniDaemon<br/>Agent Runner
    participant CB as Registered<br/>Callback
    participant Sup as AgentSupervisor
    participant Proc as Agent Process<br/>(Python Adapter)
    participant Agent as Agent Code<br/>(callback.py)

    Note over Sup,Agent: Initialization Phase (First Time)
    Sup->>Sup: create_supervisor_from_directory()
    Sup->>Sup: Detect language (python/go/typescript)
    Sup->>Sup: Install dependencies:<br/>- Python: pip install --target .omnidaemon_pydeps<br/>- Go: go mod tidy + go build<br/>- TS: npm install + tsc
    Sup->>Sup: Build PYTHONPATH:<br/>[.omnidaemon_pydeps/, agent_dir/, existing]
    Sup->>Sup: Find callback.py in agent_dir
    Sup->>Sup: Create AgentProcessConfig:<br/>- command: python -m adapter<br/>- args: --module X --function Y<br/>- env: PYTHONPATH=...<br/>- cwd: agent_dir
    Sup->>Sup: Register supervisor in global registry

    Note over Sup,Agent: Process Startup
    Sup->>Proc: start() → asyncio.create_subprocess_exec()
    Note right of Proc: Subprocess launched with:<br/>- stdin/stdout/stderr pipes<br/>- PYTHONPATH in environment<br/>- cwd = agent_dir
    Proc->>Proc: Python initializes sys.path from PYTHONPATH
    Proc->>Agent: importlib.import_module(module_path)
    Note right of Agent: Dependencies available via sys.path<br/>(.omnidaemon_pydeps/ is first)
    Agent-->>Proc: Callback function loaded
    Proc->>Proc: Enter stdio loop (read stdin)

    Note over EB,Agent: Event Processing Phase
    EB->>Runner: Event published (topic + payload)
    Runner->>CB: Invoke callback(message)
    CB->>Sup: supervisor.handle_event(message)
    
    Sup->>Sup: Check if process running<br/>(restart if needed)
    Sup->>Sup: Generate request_id (UUID)
    Sup->>Sup: Create JSON envelope:<br/>{"id": "...", "type": "task", "payload": {...}}
    
    Sup->>Proc: Write JSON + newline to stdin
    Note right of Proc: stdin.readline() receives JSON
    Proc->>Proc: json.loads(line) → parse envelope
    Proc->>Agent: Call callback(envelope["payload"])
    Note right of Agent: Agent code executes:<br/>- Imports work (deps in sys.path)<br/>- Processes message<br/>- Returns result
    Agent-->>Proc: Return result dict
    Proc->>Proc: Create response:<br/>{"id": "...", "status": "ok", "result": {...}}
    Proc->>Sup: Write JSON + newline to stdout
    
    Note over Sup: stdout_task reads line
    Sup->>Sup: json.loads(line) → parse response
    Sup->>Sup: Match response.id to pending[request_id]
    Sup->>Sup: future.set_result(response["result"])
    Sup-->>CB: Return result dict
    CB-->>Runner: Return agent response
    Runner->>EB: Save result, publish reply/webhook
```

## Detailed Component Breakdown

### 1. Supervisor Initialization (`create_supervisor_from_directory`)

**For Python Agents:**
```python
# Step 1: Install dependencies
python_env = _ensure_python_dependencies(agent_path)
# Returns: {"PYTHONPATH": ".omnidaemon_pydeps/:agent_dir/:existing", "PYTHONNOUSERSITE": "1"}

# Step 2: Find callback
module_path, func_name = _find_callback_in_directory(
    agent_dir, callback_function, "python", python_env=python_env
)
# Returns: ("examples.omnicoreagent_dir.agents.omnicore_agent.callback", "call_file_system_agent")

# Step 3: Create config
config = AgentProcessConfig(
    command="python",
    args=["-m", "omnidaemon.agent_runner.python_callback_adapter", 
          "--module", module_path, "--function", func_name],
    env=python_env,  # Includes PYTHONPATH
    cwd=str(agent_path),  # Working directory
)
```

### 2. Process Launch (`supervisor.start()`)

```python
# Launches subprocess with:
process = await asyncio.create_subprocess_exec(
    "python",
    "-m", "omnidaemon.agent_runner.python_callback_adapter",
    "--module", "examples.omnicoreagent_dir.agents.omnicore_agent.callback",
    "--function", "call_file_system_agent",
    stdin=asyncio.subprocess.PIPE,
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
    cwd="/app/examples/omnicoreagent_dir/agents/omnicore_agent",
    env={"PYTHONPATH": "/app/.../.omnidaemon_pydeps:/app/.../omnicore_agent:..."}
)
```

### 3. Python Callback Adapter (Subprocess)

**Initialization:**
```python
# Adapter starts, Python initializes sys.path from PYTHONPATH
# sys.path = [
#     "/app/.../.omnidaemon_pydeps",  # Dependencies first!
#     "/app/.../omnicore_agent",      # Agent code
#     ...existing paths...
# ]

# Load callback module
module = importlib.import_module("examples.omnicoreagent_dir.agents.omnicore_agent.callback")
callback = getattr(module, "call_file_system_agent")
# ✅ All imports in callback.py work because deps are in sys.path
```

**Event Loop:**
```python
while True:
    line = await loop.run_in_executor(None, sys.stdin.readline)
    envelope = json.loads(line)
    # {"id": "abc-123", "type": "task", "payload": {...}}
    
    result = await callback(envelope["payload"])
    # Agent code executes, returns dict
    
    response = {
        "id": envelope["id"],
        "status": "ok",
        "result": result
    }
    print(json.dumps(response))  # stdout
```

### 4. Communication Protocol

**Request Format (Supervisor → Adapter):**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "type": "task",
  "payload": {
    "content": "List all files",
    "topic": "file_system.omnicore.tasks",
    "task_id": "...",
    ...
  }
}
```

**Response Format (Adapter → Supervisor):**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "ok",
  "result": {
    "status": "success",
    "data": "File listing: ..."
  }
}
```

**Error Response:**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "error",
  "error": "Error message here"
}
```

## Key Points

1. **Process Isolation**: Each agent runs in a **separate process (different PID)** from the agent runner. The supervisor uses `asyncio.create_subprocess_exec()` to spawn the agent process, which means:
   - Agent runner PID: e.g., `12345` (main OmniDaemon process)
   - Agent process PID: e.g., `12346` (separate Python subprocess)
   - Complete isolation: Agent crashes don't affect the runner, and vice versa
   - Memory isolation: Each agent has its own memory space

2. **Communication via stdio**: The agent process communicates with the supervisor through:
   - **stdin**: Supervisor writes JSON requests → Agent reads
   - **stdout**: Agent writes JSON responses → Supervisor reads
   - **stderr**: Agent logs → Supervisor streams to logger
   - No network sockets, no shared memory - just standard pipes

3. **Dependency Isolation**: Each agent's dependencies are installed in `.omnidaemon_pydeps/` within the agent directory, isolated from other agents and the main process.

4. **PYTHONPATH Setup**: The supervisor builds PYTHONPATH with dependencies first, then agent directory, ensuring imports resolve correctly.

5. **Process Persistence**: The agent process runs continuously, handling multiple events without restarting (unless it crashes).

6. **JSON over stdio**: All communication uses newline-delimited JSON, making it language-agnostic and simple.

7. **Request/Response Correlation**: Each request has a unique `id` that matches the response, allowing async handling of multiple concurrent requests.

8. **Error Handling**: If the process crashes, the supervisor can restart it (if `restart_on_exit=True`).

## Testing the Flow

```bash
# Start the agent runner
uv run examples/omnicoreagent_dir/supervised_agent_runner.py

# In another terminal, publish a test event
uv run examples/omnicoreagent_dir/filesystem_publisher.py "List all files"

# Watch the logs to see:
# 1. Supervisor installing dependencies (first time)
# 2. Process starting
# 3. JSON messages flowing through stdio
# 4. Agent processing and responding
```

