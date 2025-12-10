
The Agent Supervisor provides robust process management for OmniDaemon agents with comprehensive lifecycle management, health monitoring, and graceful error handling.


✅ **State Management** - Structured state tracking (IDLE, STARTING, RUNNING, STOPPED, CRASHED, RESTARTING)  
✅ **Graceful Shutdown** - 3-phase shutdown (stdin message → SIGTERM → SIGKILL)  
✅ **Auto-Restart** - Circuit breaker with exponential backoff  
✅ **Health Monitoring** - Heartbeat protocol with CPU/memory tracking  
✅ **Storage Integration** - Metrics persistence with graceful degradation  
✅ **Process Isolation** - Stdio-based communication for safety  



```python
async def handle_request(message: dict) -> dict:
    """Your agent logic here."""
    return {"result": "processed"}
```


```python
from omnidaemon.agent_runner.agent_supervisor_runner import create_supervisor_from_directory

supervisor = await create_supervisor_from_directory(
    agent_name="my-agent",
    agent_dir="agents/my_agent",
    callback_function="handle_request"
)
```


```python
response = await supervisor.handle_event({
    "type": "data",
    "payload": {"task": "process this"}
})
```


```python
from omnidaemon.agent_runner.types import AgentProcessConfig

config = AgentProcessConfig(
    name="my-agent",
    command="python",
    args=["-m", "omnidaemon.agent_runner.python_callback_adapter", ...],
    
    request_timeout=60.0,
    graceful_timeout_sec=5.0,
    sigterm_timeout_sec=5.0,
    
    restart_on_exit=True,
    max_restart_attempts=3,
    restart_backoff_seconds=5.0,
    
    heartbeat_interval_seconds=30.0,
    
    env={"CUSTOM_VAR": "value"},
    cwd="/path/to/agent"
)
```

> [!CAUTION]
> **`request_timeout` is critical for long-running agents!**
>
> The `request_timeout` (default: 60-120 seconds depending on how you create the supervisor) defines how long the supervisor waits for your agent to respond to a single request.
>
> **If your agent takes longer than this timeout:**
> - The request will be marked as failed
> - The pending future will be cancelled
> - If using Redis Streams (event bus), the message may be **retried** — causing duplicate processing
>
> **Examples:**
> | Agent Type | Recommended Timeout |
> |------------|---------------------|
> | Fast API calls | 30-60 seconds |
> | LLM inference | 120-300 seconds |
> | File processing | 300-600 seconds |
> | Long-running ML tasks | 600+ seconds |
>
> ```python
>
> config = AgentProcessConfig(
>     name="file-processor",
>     request_timeout=600.0,
>     ...
> )
> ```



```
IDLE → STARTING → RUNNING
  ↓       ↓          ↓
STOPPED ← STOPPING ← CRASHED → RESTARTING → STARTING
```


States are automatically persisted to storage (if configured):

```python
from omnidaemon.storage.base import BaseStore

supervisor = AgentSupervisor(config, store=my_store)
```



The supervisor sends periodic pings to check agent health:

```
Supervisor → Agent: {"type": "ping", "id": "..."}
Agent → Supervisor: {
    "id": "...",
    "status": "ok",
    "result": {
        "health": {
            "uptime_seconds": 120.5,
            "total_requests": 42,
            "memory_mb": 125.3,
            "cpu_percent": 2.1
        }
    }
}
```


```python
import time
import psutil

start_time = time.time()
request_count = 0

async def handle_request(message: dict) -> dict:
    global request_count
    
    if message.get("type") == "ping":
        return {
            "health": {
                "uptime_seconds": time.time() - start_time,
                "total_requests": request_count,
                "memory_mb": psutil.Process().memory_info().rss / (1024 * 1024),
                "cpu_percent": psutil.Process().cpu_percent()
            }
        }
    
    request_count += 1
    return {"result": "done"}
```


The supervisor uses 3-phase shutdown for maximum reliability:

```python
await supervisor.stop()
```

```python
```

```python
```



Restarts use exponential backoff with jitter:

```python
delay = min(max_backoff, base_delay * (2 ** attempt))
final_delay = delay + random(0, delay * 0.1)
```

Example progression (base=5s):
- Attempt 1: ~5s
- Attempt 2: ~10s  
- Attempt 3: ~20s
- Circuit breaker opens after max_restart_attempts


```python
config = AgentProcessConfig(
    name="my-agent",
    max_restart_attempts=3,
    restart_backoff_seconds=5.0
)
```

When circuit opens (max attempts reached):
- Agent remains in CRASHED state
- No more automatic restarts
- Manual intervention required



The supervisor automatically saves metrics (when storage configured):

**State Changes:**
```json
{
    "event": "agent_state_change",
    "agent_name": "my-agent",
    "old_state": "STARTING",
    "new_state": "RUNNING",
    "timestamp": 1701234567.89
}
```

**Health Checks:**
```json
{
    "event": "supervisor_health_check",
    "agent_name": "my-agent",
    "state": "RUNNING",
    "cpu_percent": 2.1,
    "memory_mb": 125.3,
    "latency_ms": 15.2,
    "restart_count": 0,
    "timestamp": 1701234567.89
}
```


Storage failures don't crash the supervisor:

```python
supervisor = AgentSupervisor(config, store=unreliable_store)
```



All exceptions include actionable remediation steps:

```python
from omnidaemon.agent_runner.exceptions import AgentStartupError

try:
    await supervisor.start()
except AgentStartupError as e:
    print(e.message)
    print(e.remediation)
```


| Exception | Cause | Remediation |
|-----------|-------|-------------|
| `AgentStartupError` | Agent fails to start | Check command exists, verify permissions, review logs |
| `AgentCallbackNotFoundError` | Callback not found | Verify function name spelling, check available functions |
| `AgentDependencyError` | Dependency install failed | Check requirements.txt syntax, verify PyPI packages exist |
| `AgentTimeoutError` | Request timeout | Increase timeout, check agent responsiveness, review logs |
| `AgentCrashError` | Repeated crashes | Check logs, test agent standalone, verify dependencies |



**Symptoms:** Agent immediately crashes or never reaches RUNNING state

**Diagnosis:**
```bash
cd agents/my_agent
python callback.py

python -c "from callback import handle_request"

pip install -r requirements.txt
```

**Common Fixes:**
- Missing dependencies → Install requirements.txt
- Import errors → Fix Python path or package structure
- Permission denied → `chmod +x` on scripts


**Symptoms:** Restart loop, circuit breaker opens

**Diagnosis:**
```python
print(supervisor._restart_attempts)
print(supervisor._state)

```

**Common Fixes:**
- Systematic bug → Fix code and restart supervisor
- Resource exhaustion → Increase memory limits
- External dependency down → Check database/API availability


**Symptoms:** Health check warnings in logs

**Diagnosis:**
```python
print(supervisor._metadata.last_health_check)

response = await supervisor.handle_event({"type": "ping"})
```

**Common Fixes:**
- Agent blocked → Check for long-running synchronous operations
- Agent crashed → Supervisor will auto-restart
- Network issues → Verify localhost connectivity


**Symptoms:** Storage timeout/error warnings in logs

**Impact:** Metrics not persisted, but supervisor continues operating

**Diagnosis:**
```python
await store.save_metric({"test": "data"})

```

**Common Fixes:**
- Storage down → Restart storage backend
- Slow storage → Increase timeout (currently 5s)
- No storage needed → Don't pass `store` parameter



✅ **DO:**
- Implement ping/pong health protocol
- Use async for I/O operations
- Handle shutdown gracefully
- Log errors with context
- Keep agents stateless when possible

❌ **DON'T:**
- Block event loop with sync operations
- Ignore shutdown signals
- Store critical state only in memory
- Use globals for request state
- Perform long-running tasks synchronously


```python
config = AgentProcessConfig(
    name="prod-agent",
    request_timeout=120.0,
    heartbeat_interval_seconds=30.0,
    max_restart_attempts=5,
    restart_backoff_seconds=10.0,
    graceful_timeout_sec=30.0
)

config = AgentProcessConfig(
    name="dev-agent",
    request_timeout=10.0,
    heartbeat_interval_seconds=5.0,
    max_restart_attempts=1,
    restart_backoff_seconds=1.0
)
```


```python
from unittest.mock import AsyncMock, patch

@patch('asyncio.create_subprocess_exec')
async def test_supervisor_start(mock_subprocess):
    mock_subprocess.return_value = AsyncMock()
    supervisor = AgentSupervisor(config)
    await supervisor.start()
    assert supervisor._state == AgentState.RUNNING

async def test_real_agent():
    supervisor = await create_supervisor_from_directory(
        agent_name="test",
        agent_dir="test_agents/simple"
    )
    response = await supervisor.handle_event({"test": "data"})
    await supervisor.stop()
```


**Overhead (typical Python agent):**
- Heartbeat: <1% CPU
- Memory: ~10MB supervisor overhead
- Latency: <50ms ping/pong roundtrip

**Scalability:**
- Each agent runs in isolated process
- Supervisor can manage multiple agents
- Limited by system resources (CPU, memory, file descriptors)


```
┌─────────────────────────────────────┐
│     BaseAgentRunner (SDK)           │
│  - Event routing                    │
│  - Agent registration               │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│     AgentSupervisor                 │
│  - Process lifecycle                │
│  - State management                 │
│  - Health monitoring                │
│  - Restart logic                    │
│  - Storage integration              │
└──────────────┬──────────────────────┘
               │ stdio (JSON)
               ▼
┌─────────────────────────────────────┐
│  Python Callback Adapter (subprocess)│
│  - Loads user callback              │
│  - Handles ping/pong                │
│  - Processes tasks                  │
└──────────────┬──────────────────────┘
               │
               ▼
┌──────────────────────────────────── ┐
│    User Agent Code                  │
│  async def handle_request(msg):     │
│
└─────────────────────────────────────┘
```


- TypeScript/JavaScript adapter support
- Go adapter support  
- Memory limit enforcement
- CPU limit enforcement
- Multi-agent coordination
- Admin API for supervisor control
- Performance dashboard


- [Agent Process Types](types.py) - Configuration dataclasses
- [Python Callback Adapter](python_callback_adapter.py) - Subprocess adapter implementation
- [Custom Exceptions](exceptions.py) - Error types with remediation
- [Integration Tests](../../tests/integration/agent_runner/) - Real subprocess tests
