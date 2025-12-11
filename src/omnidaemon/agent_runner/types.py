from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict


@dataclass
class AgentProcessConfig:
    """Configuration used to launch and supervise an agent process."""

    name: str
    command: str
    args: list[str] = field(default_factory=list)
    env: Optional[Dict[str, str]] = None
    cwd: Optional[str] = None
    request_timeout: float = 300.0
    restart_on_exit: bool = True
    max_restart_attempts: int = 3
    restart_backoff_seconds: float = 5.0
    graceful_timeout_sec: float = 5.0
    sigterm_timeout_sec: float = 5.0
    heartbeat_interval_seconds: float = 60.0


class AgentState(Enum):
    """Lifecycle states for an agent process."""

    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    CRASHED = "crashed"
    RESTARTING = "restarting"


@dataclass
class AgentMetadata:
    """Runtime metadata and metrics for an agent."""

    version: str = "unknown"
    start_time: float = 0.0
    restart_count: int = 0
    last_health_check: float = 0.0
    cpu_percent: float = 0.0
    memory_mb: float = 0.0
    total_requests: int = 0
    failed_requests: int = 0
