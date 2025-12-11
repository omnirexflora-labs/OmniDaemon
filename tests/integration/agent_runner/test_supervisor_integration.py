"""Integration tests for Agent Supervisor with real subprocesses.

These tests use actual subprocesses to validate the supervisor's
behavior in real-world scenarios including:
- Process lifecycle management
- Crash detection and recovery
- Heartbeat timeouts and health monitoring
- Process cleanup and resource management
"""

import pytest
import asyncio
import tempfile
import psutil
from pathlib import Path
from unittest.mock import AsyncMock

from omnidaemon.agent_runner.agent_supervisor import AgentSupervisor
from omnidaemon.agent_runner.types import AgentProcessConfig, AgentState


@pytest.fixture
async def test_agent_directory():
    """Create a temporary directory with a test Python agent."""
    with tempfile.TemporaryDirectory() as tmpdir:
        agent_dir = Path(tmpdir)

        callback_file = agent_dir / "callback.py"
        callback_file.write_text("""
import json
import sys
import time

start_time = time.time()
request_count = 0

def handle_message(msg):
    global request_count
    msg_type = msg.get("type")
    
    if msg_type == "ping":
        return {
            "status": "ok",
            "result": {
                "health": {
                    "uptime_seconds": time.time() - start_time,
                    "total_requests": request_count,
                    "memory_mb": 10.0,
                    "cpu_percent": 1.5
                }
            }
        }
    elif msg_type == "task":
        request_count += 1
        return {
            "status": "ok",
            "result": msg.get("payload", {})
        }
    elif msg_type == "shutdown":
        sys.exit(0)
    
    return {"status": "error", "error": "Unknown message type"}

if __name__ == "__main__":
    for line in sys.stdin:
        try:
            msg = json.loads(line.strip())
            response = handle_message(msg)
            response["id"] = msg.get("id")
            print(json.dumps(response), flush=True)
        except Exception as e:
            print(json.dumps({
                "id": msg.get("id") if 'msg' in locals() else "error",
                "status": "error",
                "error": str(e)
            }), flush=True)
""")

        yield agent_dir


@pytest.mark.integration
@pytest.mark.asyncio
class TestSupervisorRealProcess:
    """Integration tests with real subprocess."""

    async def test_real_python_agent_lifecycle(self, test_agent_directory):
        """Test complete lifecycle with real Python agent."""
        import sys

        config = AgentProcessConfig(
            name="test-real-agent",
            command=sys.executable,
            args=[str(test_agent_directory / "callback.py")],
            cwd=str(test_agent_directory),
            heartbeat_interval_seconds=2.0,
            graceful_timeout_sec=2.0,
        )

        supervisor = AgentSupervisor(config, store=AsyncMock())

        try:
            await supervisor.start()
            assert supervisor._state == AgentState.RUNNING
            assert supervisor._process is not None
            assert supervisor._process.returncode is None

            response = await supervisor.handle_event({"test": "message"})

            assert response == {"test": "message"}

            assert supervisor._process.returncode is None

        finally:
            await supervisor.stop()
            assert supervisor._state == AgentState.STOPPED
            assert (
                supervisor._process is None
                or supervisor._process.returncode is not None
            )

    async def test_crash_recovery_real_process(self, test_agent_directory):
        """Test agent crashes and restarts correctly."""
        import sys

        crashy_agent = test_agent_directory / "crashy.py"
        crashy_agent.write_text("""
import sys
import time
sys.exit(1)
""")

        config = AgentProcessConfig(
            name="test-crashy-agent",
            command=sys.executable,
            args=[str(crashy_agent)],
            cwd=str(test_agent_directory),
            restart_on_exit=True,
            max_restart_attempts=2,
            restart_backoff_seconds=0.5,
        )

        supervisor = AgentSupervisor(config, store=AsyncMock())

        try:
            await supervisor.start()

            await asyncio.sleep(3.0)

            assert supervisor._restart_attempts > 0
            assert supervisor._state == AgentState.CRASHED

        finally:
            supervisor._stopping = True
            if supervisor._process:
                try:
                    supervisor._process.kill()
                    await supervisor._process.wait()
                except Exception:
                    pass

    async def test_heartbeat_with_real_process(self, test_agent_directory):
        """Test heartbeat monitoring with real process."""
        import sys

        config = AgentProcessConfig(
            name="test-heartbeat-agent",
            command=sys.executable,
            args=[str(test_agent_directory / "callback.py")],
            cwd=str(test_agent_directory),
            heartbeat_interval_seconds=1.0,
        )

        mock_store = AsyncMock()
        supervisor = AgentSupervisor(config, store=mock_store)

        try:
            await supervisor.start()
            assert supervisor._state == AgentState.RUNNING

            await asyncio.sleep(7.0)

            assert supervisor._metadata.last_health_check > 0
            assert supervisor._metadata.total_requests >= 0

        finally:
            await supervisor.stop()

    async def test_process_cleanup_verification(self, test_agent_directory):
        """Test that all child processes are properly cleaned up."""
        import sys

        config = AgentProcessConfig(
            name="test-cleanup-agent",
            command=sys.executable,
            args=[str(test_agent_directory / "callback.py")],
            cwd=str(test_agent_directory),
        )

        supervisor = AgentSupervisor(config, store=AsyncMock())

        try:
            await supervisor.start()
            agent_pid = supervisor._process.pid

            assert psutil.pid_exists(agent_pid)

            await supervisor.stop()

            await asyncio.sleep(0.5)

            assert not psutil.pid_exists(agent_pid)

        except Exception:
            if supervisor._process and supervisor._process.returncode is None:
                try:
                    supervisor._process.kill()
                    await supervisor._process.wait()
                except Exception:
                    pass
            raise

    async def test_sigterm_then_sigkill_escalation(self, test_agent_directory):
        """Test 3-phase shutdown escalation (stdin -> SIGTERM -> SIGKILL)."""
        import sys

        ignore_shutdown = test_agent_directory / "ignore_shutdown.py"
        ignore_shutdown.write_text("""
import json
import sys
import signal
import time

signal.signal(signal.SIGTERM, signal.SIG_IGN)

for line in sys.stdin:
    msg = json.loads(line.strip())
    if msg.get("type") == "shutdown":
        print(json.dumps({"id": msg.get("id"), "status": "ok"}), flush=True)
        continue
    
    response = {"id": msg.get("id"), "status": "ok", "result": {}}
    print(json.dumps(response), flush=True)
""")

        config = AgentProcessConfig(
            name="test-stubborn-agent",
            command=sys.executable,
            args=[str(ignore_shutdown)],
            cwd=str(test_agent_directory),
            graceful_timeout_sec=1.0,
            sigterm_timeout_sec=1.0,
        )

        supervisor = AgentSupervisor(config, store=AsyncMock())

        try:
            await supervisor.start()
            agent_pid = supervisor._process.pid

            await supervisor.stop()

            assert supervisor._state == AgentState.STOPPED
            assert not psutil.pid_exists(agent_pid)

        except Exception:
            if supervisor._process:
                try:
                    supervisor._process.kill()
                    await supervisor._process.wait()
                except Exception:
                    pass
            raise


@pytest.mark.integration
@pytest.mark.asyncio
class TestStorageFailureScenarios:
    """Test supervisor behavior when storage fails."""

    async def test_storage_timeout_doesnt_crash_supervisor(self, test_agent_directory):
        """Test that storage timeouts don't crash the supervisor."""
        import sys

        class TimeoutStore:
            async def save_metric(self, metric):
                await asyncio.sleep(10.0)

            async def save_config(self, key, value):
                await asyncio.sleep(10.0)

        config = AgentProcessConfig(
            name="test-timeout-agent",
            command=sys.executable,
            args=[str(test_agent_directory / "callback.py")],
            cwd=str(test_agent_directory),
            heartbeat_interval_seconds=1.0,
        )

        supervisor = AgentSupervisor(config, store=TimeoutStore())

        try:
            await supervisor.start()
            assert supervisor._state == AgentState.RUNNING

            response = await supervisor.handle_event({"test": "data"})
            assert response == {"test": "data"}

            await asyncio.sleep(3.0)

            assert supervisor._state == AgentState.RUNNING

        finally:
            await supervisor.stop()

    async def test_storage_error_doesnt_crash_supervisor(self, test_agent_directory):
        """Test that storage exceptions don't crash the supervisor."""
        import sys

        class ErrorStore:
            async def save_metric(self, metric):
                raise Exception("Storage error!")

            async def save_config(self, key, value):
                raise Exception("Storage error!")

        config = AgentProcessConfig(
            name="test-error-agent",
            command=sys.executable,
            args=[str(test_agent_directory / "callback.py")],
            cwd=str(test_agent_directory),
        )

        supervisor = AgentSupervisor(config, store=ErrorStore())

        try:
            await supervisor.start()
            assert supervisor._state == AgentState.RUNNING

            await supervisor.stop()
            assert supervisor._state == AgentState.STOPPED

        except Exception:
            if supervisor._process:
                try:
                    supervisor._process.kill()
                    await supervisor._process.wait()
                except Exception:
                    pass
            raise
