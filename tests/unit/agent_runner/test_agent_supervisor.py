"""Unit tests for AgentSupervisor."""

import pytest
import asyncio
import time
import json
from unittest.mock import AsyncMock, patch, Mock

from omnidaemon.agent_runner.agent_supervisor import (
    AgentSupervisor,
    AgentState,
    AgentMetadata,
    AgentProcessConfig,
)


class TestAgentStateAndMetadata:
    """Test suite for AgentState enum and AgentMetadata dataclass."""

    def test_agent_state_values(self):
        """Test AgentState enum has all expected values."""
        expected_states = {
            "idle",
            "starting",
            "running",
            "stopping",
            "stopped",
            "crashed",
            "restarting",
        }
        actual_states = {state.value for state in AgentState}
        assert actual_states == expected_states

    def test_agent_metadata_initialization(self):
        """Test AgentMetadata initializes with correct defaults."""
        metadata = AgentMetadata()
        assert metadata.version == "unknown"
        assert metadata.start_time == 0.0
        assert metadata.restart_count == 0
        assert metadata.last_health_check == 0.0
        assert metadata.cpu_percent == 0.0
        assert metadata.memory_mb == 0.0
        assert metadata.total_requests == 0
        assert metadata.failed_requests == 0

    def test_agent_metadata_custom_values(self):
        """Test AgentMetadata accepts custom values."""
        metadata = AgentMetadata(
            version="v1.0.0",
            start_time=12345.0,
            restart_count=2,
            cpu_percent=25.5,
            memory_mb=128.0,
        )
        assert metadata.version == "v1.0.0"
        assert metadata.start_time == 12345.0
        assert metadata.restart_count == 2
        assert metadata.cpu_percent == 25.5
        assert metadata.memory_mb == 128.0


class TestAgentSupervisorInitialization:
    """Test suite for AgentSupervisor initialization."""

    def test_supervisor_init_with_store_check(self):
        """Test supervisor initializes with storage (required)."""
        config = AgentProcessConfig(
            name="test-agent",
            command="python",
            args=["-m", "test"],
        )
        mock_store = AsyncMock()
        supervisor = AgentSupervisor(config, store=mock_store)

        assert supervisor.config == config
        assert supervisor.store is mock_store
        assert supervisor._state == AgentState.IDLE
        assert isinstance(supervisor._metadata, AgentMetadata)
        assert supervisor._process is None
        assert supervisor._stopping is False

    def test_supervisor_init_with_store(self):
        """Test supervisor initializes with storage."""
        config = AgentProcessConfig(name="test-agent", command="python")
        mock_store = AsyncMock()

        supervisor = AgentSupervisor(config, store=mock_store)

        assert supervisor.store is mock_store

    def test_supervisor_init_config_defaults(self):
        """Test supervisor config uses default values."""
        config = AgentProcessConfig(name="test-agent", command="python")

        assert config.request_timeout == 300.0
        assert config.restart_on_exit is True
        assert config.max_restart_attempts == 3
        assert config.restart_backoff_seconds == 5.0
        assert config.graceful_timeout_sec == 5.0
        assert config.sigterm_timeout_sec == 5.0
        assert config.heartbeat_interval_seconds == 60.0


class TestAgentSupervisorStateTransitions:
    """Test suite for state transition logic."""

    @pytest.mark.asyncio
    async def test_transition_to_changes_state(self):
        """Test _transition_to changes state."""
        config = AgentProcessConfig(name="test-agent", command="python")
        supervisor = AgentSupervisor(config, store=AsyncMock())

        assert supervisor._state == AgentState.IDLE

        await supervisor._transition_to(AgentState.STARTING)
        assert supervisor._state == AgentState.STARTING

    @pytest.mark.asyncio
    async def test_transition_to_persists_to_storage(self):
        """Test _transition_to saves metrics to storage."""
        config = AgentProcessConfig(name="test-agent", command="python")
        mock_store = AsyncMock()
        supervisor = AgentSupervisor(config, store=mock_store)

        await supervisor._transition_to(AgentState.STARTING)

        mock_store.save_metric.assert_called_once()
        call_args = mock_store.save_metric.call_args[0][0]
        assert call_args["event"] == "agent_state_change"
        assert call_args["agent_name"] == "test-agent"
        assert call_args["old_state"] == "idle"
        assert call_args["new_state"] == "starting"

    @pytest.mark.asyncio
    async def test_transition_to_handles_storage_error(self):
        """Test _transition_to handles storage errors gracefully."""
        config = AgentProcessConfig(name="test-agent", command="python")
        mock_store = AsyncMock()
        mock_store.save_metric.side_effect = Exception("Storage error")
        supervisor = AgentSupervisor(config, store=mock_store)

        await supervisor._transition_to(AgentState.STARTING)
        assert supervisor._state == AgentState.STARTING


class TestAgentSupervisorStartupAndShutdown:
    """Test suite for agent startup and shutdown."""

    @pytest.mark.asyncio
    async def test_start_creates_process(self):
        """Test start() creates subprocess."""
        config = AgentProcessConfig(
            name="test-agent",
            command="python",
            args=["-c", "import time; time.sleep(10)"],
        )
        supervisor = AgentSupervisor(config, store=AsyncMock())

        with patch("asyncio.create_subprocess_exec") as mock_create:
            mock_process = AsyncMock()
            mock_process.returncode = None
            mock_process.stdin = AsyncMock()
            mock_process.stdout = AsyncMock()
            mock_process.stderr = AsyncMock()
            mock_create.return_value = mock_process

            await supervisor.start()

            assert supervisor._state == AgentState.RUNNING
            assert supervisor._process is not None
            mock_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_transitions_to_running(self):
        """Test start() transitions to RUNNING state."""
        config = AgentProcessConfig(name="test-agent", command="python")
        supervisor = AgentSupervisor(config, store=AsyncMock())

        with patch("asyncio.create_subprocess_exec") as mock_create:
            mock_process = AsyncMock()
            mock_process.returncode = None
            mock_process.stdin = AsyncMock()
            mock_process.stdout = AsyncMock()
            mock_process.stderr = AsyncMock()
            mock_create.return_value = mock_process

            await supervisor.start()
            assert supervisor._state == AgentState.RUNNING

    @pytest.mark.asyncio
    async def test_start_sets_metadata(self):
        """Test start() sets metadata fields."""
        config = AgentProcessConfig(name="test-agent", command="python")
        supervisor = AgentSupervisor(config, store=AsyncMock())

        with patch("asyncio.create_subprocess_exec") as mock_create:
            mock_process = AsyncMock()
            mock_process.returncode = None
            mock_process.stdin = AsyncMock()
            mock_process.stdout = AsyncMock()
            mock_process.stderr = AsyncMock()
            mock_create.return_value = mock_process

            before_time = time.time()
            await supervisor.start()
            after_time = time.time()

            assert before_time <= supervisor._metadata.start_time <= after_time
            assert supervisor._metadata.restart_count == 0

    @pytest.mark.asyncio
    async def test_stop_graceful_shutdown(self):
        """Test stop() performs graceful shutdown."""
        config = AgentProcessConfig(
            name="test-agent",
            command="python",
            graceful_timeout_sec=0.1,
        )
        supervisor = AgentSupervisor(config, store=AsyncMock())

        mock_process = AsyncMock()
        mock_process.returncode = None
        mock_process.stdin = Mock()
        mock_process.stdin.write = Mock()
        mock_process.stdin.drain = AsyncMock()

        async def wait_mock():
            mock_process.returncode = 0

        mock_process.wait = AsyncMock(side_effect=wait_mock)

        supervisor._process = mock_process
        supervisor._state = AgentState.RUNNING

        await supervisor.stop()

        mock_process.stdin.write.assert_called()
        written_data = mock_process.stdin.write.call_args[0][0]
        assert b'"type": "shutdown"' in written_data
        assert supervisor._state == AgentState.STOPPED

    @pytest.mark.asyncio
    async def test_stop_uses_sigterm_on_timeout(self):
        """Test stop() uses SIGTERM if graceful timeout expires."""
        config = AgentProcessConfig(
            name="test-agent",
            command="python",
            graceful_timeout_sec=0.05,
            sigterm_timeout_sec=0.05,
        )
        supervisor = AgentSupervisor(config, store=AsyncMock())

        mock_process = AsyncMock()
        mock_process.returncode = None
        mock_process.stdin = Mock()
        mock_process.stdin.write = Mock()
        mock_process.stdin.drain = AsyncMock()

        async def wait_timeout():
            await asyncio.sleep(10)

        mock_process.wait = AsyncMock(side_effect=wait_timeout)
        mock_process.terminate = Mock()
        mock_process.kill = Mock()

        supervisor._process = mock_process
        supervisor._state = AgentState.RUNNING

        await supervisor.stop()

        mock_process.terminate.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_cleanup_tasks(self):
        """Test stop() cleans up background tasks."""
        config = AgentProcessConfig(name="test-agent", command="python")
        supervisor = AgentSupervisor(config, store=AsyncMock())

        async def long_task():
            await asyncio.sleep(100)

        task1 = asyncio.create_task(long_task())
        task2 = asyncio.create_task(long_task())
        task3 = asyncio.create_task(long_task())

        supervisor._stdout_task = task1
        supervisor._stderr_task = task2
        supervisor._heartbeat_task = task3

        await asyncio.sleep(0.01)
        assert not task1.done()
        assert not task2.done()
        assert not task3.done()

        mock_process = AsyncMock()
        mock_process.returncode = 0
        mock_process.stdin = Mock()
        mock_process.stdin.write = Mock()
        mock_process.stdin.drain = AsyncMock()

        async def wait_mock():
            pass

        mock_process.wait = AsyncMock(side_effect=wait_mock)

        supervisor._process = mock_process
        supervisor._state = AgentState.RUNNING

        await supervisor.stop()

        assert task1.done()
        assert task2.done()
        assert task3.done()


class TestAgentSupervisorRestartLogic:
    """Test suite for restart and circuit breaker logic."""

    @pytest.mark.asyncio
    async def test_restart_uses_exponential_backoff(self):
        """Test restart uses exponential backoff with jitter."""
        config = AgentProcessConfig(
            name="test-agent",
            command="python",
            restart_backoff_seconds=1.0,
            max_restart_attempts=3,
        )
        supervisor = AgentSupervisor(config, store=AsyncMock())
        supervisor._stopping = False

        with (
            patch("asyncio.sleep") as mock_sleep,
            patch.object(supervisor, "start", new_callable=AsyncMock),
        ):
            supervisor._restart_attempts = 0
            await supervisor._restart_if_needed()

            mock_sleep.assert_called_once()
            delay = mock_sleep.call_args[0][0]
            assert 1.0 <= delay <= 1.1

    @pytest.mark.asyncio
    async def test_restart_circuit_breaker(self):
        """Test circuit breaker stops restarts after max attempts."""
        config = AgentProcessConfig(
            name="test-agent",
            command="python",
            max_restart_attempts=3,
        )
        supervisor = AgentSupervisor(config, store=AsyncMock())
        supervisor._stopping = False
        supervisor._restart_attempts = 3

        with (
            patch("asyncio.sleep") as mock_sleep,
            patch.object(supervisor, "start", new_callable=AsyncMock) as mock_start,
        ):
            await supervisor._restart_if_needed()

            mock_sleep.assert_not_called()
            mock_start.assert_not_called()
            assert supervisor._state == AgentState.CRASHED

    @pytest.mark.asyncio
    async def test_restart_transitions_states(self):
        """Test restart transitions through correct states."""
        config = AgentProcessConfig(
            name="test-agent",
            command="python",
            restart_backoff_seconds=0.01,
            max_restart_attempts=3,
        )
        supervisor = AgentSupervisor(config, store=AsyncMock())
        supervisor._stopping = False

        with (
            patch("asyncio.sleep"),
            patch.object(supervisor, "start", new_callable=AsyncMock),
        ):
            await supervisor._restart_if_needed()

            assert supervisor._state in (AgentState.RESTARTING, AgentState.RUNNING)


@pytest.mark.asyncio
class TestAgentSupervisorHeartbeat:
    """Test suite for heartbeat system."""

    @pytest.mark.asyncio
    async def test_heartbeat_sends_ping(self):
        """Test heartbeat sends ping via direct method call."""
        config = AgentProcessConfig(
            name="test-agent",
            command="python",
            heartbeat_interval_seconds=0.1,
        )
        mock_store = AsyncMock()
        supervisor = AgentSupervisor(config, store=mock_store)

        writes = []

        mock_stdin = Mock()
        mock_stdin.write = Mock(side_effect=lambda data: writes.append(data))
        mock_stdin.drain = AsyncMock()

        mock_process = AsyncMock()
        mock_process.returncode = None
        mock_process.stdin = mock_stdin
        supervisor._process = mock_process
        supervisor._stopping = False

        ping_id = "test-ping"
        async with supervisor._write_lock:
            if supervisor._process and supervisor._process.stdin:
                ping_envelope = {"id": ping_id, "type": "ping", "payload": {}}
                message = json.dumps(ping_envelope).encode() + b"\n"
                supervisor._process.stdin.write(message)
                await supervisor._process.stdin.drain()

        assert len(writes) > 0
        assert b'"type"' in writes[0] and b"ping" in writes[0]

    @pytest.mark.asyncio
    async def test_heartbeat_updates_metadata(self):
        """Test heartbeat updates metadata from pong response."""
        config = AgentProcessConfig(name="test-agent", command="python")
        mock_store = AsyncMock()
        supervisor = AgentSupervisor(config, store=mock_store)

        pong_response = {
            "id": "ping-123",
            "status": "ok",
            "result": {
                "type": "pong",
                "health": {
                    "cpu_percent": 15.5,
                    "memory_mb": 128.0,
                    "total_requests": 42,
                },
            },
        }

        supervisor._metadata.cpu_percent = pong_response["result"]["health"][
            "cpu_percent"
        ]
        supervisor._metadata.memory_mb = pong_response["result"]["health"]["memory_mb"]
        supervisor._metadata.total_requests = pong_response["result"]["health"][
            "total_requests"
        ]

        assert supervisor._metadata.cpu_percent == 15.5
        assert supervisor._metadata.memory_mb == 128.0
        assert supervisor._metadata.total_requests == 42


class TestAgentSupervisorRequestHandling:
    """Test suite for handle_event request/response."""

    @pytest.mark.asyncio
    async def test_handle_event_sends_request(self):
        """Test handle_event sends request to agent."""
        config = AgentProcessConfig(name="test-agent", command="python")
        supervisor = AgentSupervisor(config, store=AsyncMock())

        mock_process = AsyncMock()
        mock_process.returncode = None
        mock_process.stdin = Mock()
        mock_process.stdin.write = Mock()
        mock_process.stdin.drain = AsyncMock()
        supervisor._process = mock_process
        supervisor._state = AgentState.RUNNING

        async def mock_wait_for(future, timeout):
            return {"status": "ok", "result": {"data": "test result"}}

        with patch("asyncio.wait_for", side_effect=mock_wait_for):
            await supervisor.handle_event({"test": "payload"})

            mock_process.stdin.write.assert_called_once()
            written = mock_process.stdin.write.call_args[0][0]
            assert b'"type": "task"' in written

    @pytest.mark.asyncio
    async def test_handle_event_timeout(self):
        """Test handle_event raises on timeout."""
        config = AgentProcessConfig(
            name="test-agent",
            command="python",
            request_timeout=0.1,
        )
        supervisor = AgentSupervisor(config, store=AsyncMock())

        mock_process = AsyncMock()
        mock_process.returncode = None
        mock_process.stdin = Mock()
        mock_process.stdin.write = Mock()
        mock_process.stdin.drain = AsyncMock()
        supervisor._process = mock_process
        supervisor._state = AgentState.RUNNING

        async def mock_wait_for(future, timeout):
            await asyncio.sleep(1.0)

        with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
            with pytest.raises(TimeoutError, match="Timeout waiting for response"):
                await supervisor.handle_event({"test": "payload"})
