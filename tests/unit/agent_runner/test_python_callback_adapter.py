"""Unit tests for PythonCallbackAdapter."""

import pytest
import json
import sys
import io
from unittest.mock import patch, Mock
from omnidaemon.agent_runner.python_callback_adapter import PythonCallbackAdapter


class TestPythonCallbackAdapterInitialization:
    """Test suite for adapter initialization."""

    def test_adapter_init(self):
        """Test adapter initializes with correct attributes."""
        adapter = PythonCallbackAdapter("test.module", "test_function")

        assert adapter.module_path == "test.module"
        assert adapter.function_name == "test_function"
        assert adapter.callback is None
        assert adapter.total_requests == 0
        assert adapter.failed_requests == 0
        assert adapter.start_time > 0

    def test_adapter_init_different_values(self):
        """Test adapter with different module/function combinations."""
        adapter = PythonCallbackAdapter("mymodule.submodule", "my_callback")

        assert adapter.module_path == "mymodule.submodule"
        assert adapter.function_name == "my_callback"


class TestPythonCallbackAdapterLoading:
    """Test suite for callback loading."""

    def test_load_callback_success(self):
        """Test successful callback loading."""
        adapter = PythonCallbackAdapter("omnidaemon.agent_runner.types", "AgentState")

        adapter._load_callback()

        assert adapter.callback is not None
        assert callable(adapter.callback)

    def test_load_callback_import_error(self):
        """Test callback loading with invalid module."""
        adapter = PythonCallbackAdapter("nonexistent.module", "function")

        with pytest.raises(RuntimeError, match="Failed to import module"):
            adapter._load_callback()

    def test_load_callback_attribute_error(self):
        """Test callback loading with nonexistent function."""
        adapter = PythonCallbackAdapter(
            "omnidaemon.agent_runner.types", "nonexistent_function"
        )

        with pytest.raises(RuntimeError, match="Function.*not found"):
            adapter._load_callback()

    def test_load_callback_not_callable(self):
        """Test callback loading when attribute is not callable."""
        with patch("importlib.import_module") as mock_import:
            mock_module = Mock()
            mock_module.test_attr = "not_callable"
            mock_import.return_value = mock_module

            adapter = PythonCallbackAdapter("test.module", "test_attr")

            with pytest.raises(ValueError, match="is not callable"):
                adapter._load_callback()


class TestPythonCallbackAdapterPingHandling:
    """Test suite for ping/pong health check protocol."""

    @pytest.mark.asyncio
    async def test_handle_ping_basic(self):
        """Test basic ping handling."""
        adapter = PythonCallbackAdapter("test.module", "test_function")
        adapter.total_requests = 10
        adapter.failed_requests = 2

        responses = []

        async def mock_send_response(response):
            responses.append(response)

        adapter._send_response = mock_send_response

        await adapter._handle_ping("ping-123")

        assert len(responses) == 1
        response = responses[0]
        assert response["id"] == "ping-123"
        assert response["status"] == "ok"
        assert response["result"]["type"] == "pong"
        assert response["result"]["health"]["total_requests"] == 10
        assert response["result"]["health"]["failed_requests"] == 2
        assert "uptime_seconds" in response["result"]["health"]
        assert "memory_mb" in response["result"]["health"]
        assert "cpu_percent" in response["result"]["health"]

    @pytest.mark.asyncio
    async def test_handle_ping_with_psutil(self):
        """Test ping handling with psutil available."""
        adapter = PythonCallbackAdapter("test.module", "test_function")

        responses = []

        async def mock_send_response(response):
            responses.append(response)

        adapter._send_response = mock_send_response

        mock_process = Mock()
        mock_process.memory_info.return_value = Mock(rss=104857600)
        mock_process.cpu_percent.return_value = 25.5

        mock_psutil = Mock()
        mock_psutil.Process.return_value = mock_process

        with patch.dict("sys.modules", {"psutil": mock_psutil}):
            import importlib

            importlib.reload(
                sys.modules["omnidaemon.agent_runner.python_callback_adapter"]
            )
            await adapter._handle_ping("ping-456")

        assert len(responses) == 1
        response = responses[0]
        assert response["result"]["health"]["memory_mb"] > 0
        assert response["result"]["health"]["cpu_percent"] >= 0

    @pytest.mark.asyncio
    async def test_handle_ping_error(self):
        """Test ping handling with error."""
        adapter = PythonCallbackAdapter("test.module", "test_function")

        error_responses = []

        async def mock_error_response(request_id, error):
            error_responses.append({"id": request_id, "error": error})

        adapter._send_error_response = mock_error_response

        adapter.start_time = "invalid"

        await adapter._handle_ping("ping-error")

        assert len(error_responses) == 1


class TestPythonCallbackAdapterTaskHandling:
    """Test suite for task processing."""

    @pytest.mark.asyncio
    async def test_handle_task_async_callback(self):
        """Test handling task with async callback."""

        async def test_callback(message):
            return {"result": "success", "data": message.get("input")}

        adapter = PythonCallbackAdapter("test.module", "test_function")
        adapter.callback = test_callback

        responses = []

        async def mock_send_response(response):
            responses.append(response)

        adapter._send_response = mock_send_response

        envelope = {"id": "task-123", "type": "task", "payload": {"input": "test data"}}

        await adapter._handle_task(envelope)

        assert adapter.total_requests == 1
        assert adapter.failed_requests == 0
        assert len(responses) == 1
        assert responses[0]["id"] == "task-123"
        assert responses[0]["status"] == "ok"
        assert responses[0]["result"]["result"] == "success"

    @pytest.mark.asyncio
    async def test_handle_task_sync_callback(self):
        """Test handling task with sync callback."""

        def test_callback(message):
            return {"processed": True}

        adapter = PythonCallbackAdapter("test.module", "test_function")
        adapter.callback = test_callback

        responses = []

        async def mock_send_response(response):
            responses.append(response)

        adapter._send_response = mock_send_response

        envelope = {"id": "task-456", "type": "task", "payload": {}}

        await adapter._handle_task(envelope)

        assert adapter.total_requests == 1
        assert responses[0]["result"]["processed"] is True

    @pytest.mark.asyncio
    async def test_handle_task_none_result(self):
        """Test handling task when callback returns None."""

        async def test_callback(message):
            return None

        adapter = PythonCallbackAdapter("test.module", "test_function")
        adapter.callback = test_callback

        responses = []

        async def mock_send_response(response):
            responses.append(response)

        adapter._send_response = mock_send_response

        envelope = {"id": "task-789", "type": "task", "payload": {}}

        await adapter._handle_task(envelope)

        assert responses[0]["result"]["status"] == "completed"

    @pytest.mark.asyncio
    async def test_handle_task_non_dict_result(self):
        """Test handling task when callback returns non-dict value."""

        async def test_callback(message):
            return "string result"

        adapter = PythonCallbackAdapter("test.module", "test_function")
        adapter.callback = test_callback

        responses = []

        async def mock_send_response(response):
            responses.append(response)

        adapter._send_response = mock_send_response

        envelope = {"id": "task-999", "type": "task", "payload": {}}

        await adapter._handle_task(envelope)

        assert responses[0]["result"]["status"] == "completed"
        assert responses[0]["result"]["data"] == "string result"

    @pytest.mark.asyncio
    async def test_handle_task_exception(self):
        """Test handling task when callback raises exception."""

        async def test_callback(message):
            raise ValueError("Test error")

        adapter = PythonCallbackAdapter("test.module", "test_function")
        adapter.callback = test_callback

        error_responses = []

        async def mock_error_response(request_id, error):
            error_responses.append({"id": request_id, "error": error})

        adapter._send_error_response = mock_error_response

        envelope = {"id": "task-error", "type": "task", "payload": {}}

        await adapter._handle_task(envelope)

        assert adapter.failed_requests == 1
        assert len(error_responses) == 1
        assert "Test error" in error_responses[0]["error"]


class TestPythonCallbackAdapterResponseSending:
    """Test suite for response sending."""

    @pytest.mark.asyncio
    async def test_send_response(self):
        """Test sending JSON response to stdout."""
        adapter = PythonCallbackAdapter("test.module", "test_function")

        captured_output = io.StringIO()
        with patch("sys.stdout", captured_output):
            await adapter._send_response({"id": "123", "status": "ok"})

        output = captured_output.getvalue()
        assert '"id": "123"' in output
        assert '"status": "ok"' in output
        assert output.endswith("\n")

    @pytest.mark.asyncio
    async def test_send_error_response(self):
        """Test sending error response."""
        adapter = PythonCallbackAdapter("test.module", "test_function")

        responses = []

        async def mock_send_response(response):
            responses.append(response)

        adapter._send_response = mock_send_response

        await adapter._send_error_response("error-123", "Something went wrong")

        assert len(responses) == 1
        assert responses[0]["id"] == "error-123"
        assert responses[0]["status"] == "error"
        assert responses[0]["error"] == "Something went wrong"


class TestPythonCallbackAdapterMaybeAwait:
    """Test suite for _maybe_await utility."""

    @pytest.mark.asyncio
    async def test_maybe_await_coroutine(self):
        """Test _maybe_await with coroutine."""

        async def async_fn():
            return "async result"

        result = await PythonCallbackAdapter._maybe_await(async_fn())
        assert result == "async result"

    @pytest.mark.asyncio
    async def test_maybe_await_non_coroutine(self):
        """Test _maybe_await with regular value."""
        result = await PythonCallbackAdapter._maybe_await("regular value")
        assert result == "regular value"

    @pytest.mark.asyncio
    async def test_maybe_await_dict(self):
        """Test _maybe_await with dict."""
        result = await PythonCallbackAdapter._maybe_await({"key": "value"})
        assert result == {"key": "value"}


class TestPythonCallbackAdapterRun:
    """Test suite for main run loop."""

    @pytest.mark.asyncio
    async def test_run_shutdown_message(self):
        """Test run loop handles shutdown message."""
        adapter = PythonCallbackAdapter("omnidaemon.agent_runner.types", "AgentState")

        shutdown_msg = json.dumps({"id": "1", "type": "shutdown"}) + "\n"
        mock_stdin = io.StringIO(shutdown_msg)

        responses = []

        async def mock_send_response(response):
            responses.append(response)

        adapter._send_response = mock_send_response

        with patch("sys.stdin", mock_stdin):
            await adapter.run()

        assert len(responses) == 1
        assert responses[0]["result"]["message"] == "shutdown acknowledged"

    @pytest.mark.asyncio
    async def test_run_ping_message(self):
        """Test run loop handles ping message."""
        adapter = PythonCallbackAdapter("omnidaemon.agent_runner.types", "AgentState")

        messages = [
            json.dumps({"id": "1", "type": "ping"}),
            json.dumps({"id": "2", "type": "shutdown"}),
        ]
        mock_stdin = io.StringIO("\n".join(messages) + "\n")

        responses = []

        async def mock_send_response(response):
            responses.append(response)

        adapter._send_response = mock_send_response

        with patch("sys.stdin", mock_stdin):
            await adapter.run()

        assert len(responses) >= 2
        assert responses[0]["result"]["type"] == "pong"

    @pytest.mark.asyncio
    async def test_run_invalid_json(self):
        """Test run loop handles invalid JSON."""
        adapter = PythonCallbackAdapter("omnidaemon.agent_runner.types", "AgentState")

        messages = ["invalid json{{{", json.dumps({"id": "2", "type": "shutdown"})]
        mock_stdin = io.StringIO("\n".join(messages) + "\n")

        error_responses = []

        async def mock_error_response(request_id, error):
            error_responses.append({"id": request_id, "error": error})

        adapter._send_error_response = mock_error_response

        responses = []

        async def mock_send_response(response):
            responses.append(response)

        adapter._send_response = mock_send_response

        with patch("sys.stdin", mock_stdin):
            await adapter.run()

        assert len(error_responses) >= 1
        assert "Invalid JSON" in error_responses[0]["error"]

    @pytest.mark.asyncio
    async def test_run_unknown_message_type(self):
        """Test run loop handles unknown message type."""
        adapter = PythonCallbackAdapter("omnidaemon.agent_runner.types", "AgentState")

        messages = [
            json.dumps({"id": "1", "type": "unknown_type"}),
            json.dumps({"id": "2", "type": "shutdown"}),
        ]
        mock_stdin = io.StringIO("\n".join(messages) + "\n")

        error_responses = []

        async def mock_error_response(request_id, error):
            error_responses.append({"id": request_id, "error": error})

        adapter._send_error_response = mock_error_response

        responses = []

        async def mock_send_response(response):
            responses.append(response)

        adapter._send_response = mock_send_response

        with patch("sys.stdin", mock_stdin):
            await adapter.run()

        assert len(error_responses) >= 1
        assert "Unknown message type" in error_responses[0]["error"]
