"""
Python Callback Adapter for OmniDaemon Agent Supervisor.

This module provides a stdio-based adapter that allows Python functions to be
run as OmniDaemon agents managed by the AgentSupervisor. The adapter handles:
- JSON message parsing from stdin
- Task execution via user-provided callback
- Health monitoring and heartbeat responses
- Process metrics collection (CPU, memory)
- Graceful shutdown handling

The adapter follows a request-response protocol over stdio, where each message
contains an 'id' for correlation, a 'type' (task, ping, shutdown), and a 'payload'.

Usage:
    python -m omnidaemon.agent_runner.python_callback_adapter \\
        --module examples.omnicoreagent_dir.agent_runner \\
        --function call_file_system_agent
"""

import asyncio
import json
import logging
import os
import sys
import time
import importlib
from typing import Callable, Dict, Any, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)


class PythonCallbackAdapter:
    """
    Generic adapter that wraps any Python async callback function
    and makes it accessible via stdio JSON protocol.
    """

    def __init__(self, module_path: str, function_name: str) -> None:
        """
        Initialize the adapter.

        Args:
            module_path: Dot-separated module path (e.g., "examples.omnicoreagent_dir.agent_runner")
            function_name: Name of the callback function to call
        """
        self.module_path = module_path
        self.function_name = function_name
        self.callback: Optional[Callable] = None
        self.start_time = time.time()
        self.total_requests = 0
        self.failed_requests = 0

        try:
            import psutil

            self._process = psutil.Process(os.getpid())
            self._process.cpu_percent(interval=None)
            logger.debug("CPU measurement baseline established")
        except ImportError:
            self._process = None
            logger.warning("psutil not available, CPU metrics will be 0")

    def _load_callback(self) -> None:
        """Dynamically import the module and get the callback function."""
        try:
            module = importlib.import_module(self.module_path)
            self.callback = getattr(module, self.function_name)
            if not callable(self.callback):
                raise ValueError(
                    f"'{self.function_name}' in module '{self.module_path}' is not callable"
                )
            logger.debug(
                f"Agent listening on stdin for messages (callback: {self.callback.__name__})"
            )
        except ImportError as e:
            raise RuntimeError(
                f"Failed to import module '{self.module_path}': {e}"
            ) from e
        except AttributeError as e:
            raise RuntimeError(
                f"Function '{self.function_name}' not found in module '{self.module_path}': {e}"
            ) from e

    async def run(self) -> None:
        """Run the stdio loop, processing tasks and calling the callback."""
        self._load_callback()
        assert self.callback is not None

        loop = asyncio.get_running_loop()

        while True:
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                break

            line = line.strip()
            if not line:
                continue

            try:
                envelope = json.loads(line)
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON received: {line[:100]}... Error: {e}")
                await self._send_error_response(None, f"Invalid JSON: {str(e)}")
                continue

            message_type = envelope.get("type")
            request_id = envelope.get("id")

            if message_type == "shutdown":
                logger.info("Received shutdown signal")
                await self._send_response(
                    {
                        "id": request_id,
                        "status": "ok",
                        "result": {"message": "shutdown acknowledged"},
                    }
                )
                break

            if message_type == "ping":
                await self._handle_ping(request_id)
                continue

            if message_type != "task":
                logger.warning(f"Unknown message type: {message_type}")
                await self._send_error_response(
                    request_id, f"Unknown message type: {message_type}"
                )
                continue

            asyncio.create_task(self._handle_task(envelope))

    async def _handle_ping(self, request_id: Optional[str]) -> None:
        """Handle a ping health check request."""
        try:
            if self._process is not None:
                try:
                    memory_info = self._process.memory_info()
                    memory_mb = memory_info.rss / (1024 * 1024)

                    cpu_percent = self._process.cpu_percent(interval=0.1)

                    logger.debug(
                        f"Process metrics: CPU={cpu_percent}%, Memory={memory_mb}MB"
                    )
                except Exception as e:
                    logger.warning(f"Error getting process metrics: {e}")
                    memory_mb = 0.0
                    cpu_percent = 0.0
            else:
                memory_mb = 0.0
                cpu_percent = 0.0

            uptime = time.time() - self.start_time

            health_data = {
                "process_id": os.getpid(),
                "uptime_seconds": uptime,
                "total_requests": self.total_requests,
                "failed_requests": self.failed_requests,
                "memory_mb": memory_mb,
                "cpu_percent": cpu_percent,
            }

            await self._send_response(
                {
                    "id": request_id,
                    "status": "ok",
                    "result": {
                        "type": "pong",
                        "health": health_data,
                    },
                }
            )
        except Exception as exc:
            logger.exception(f"Error handling ping: {exc}")
            await self._send_error_response(request_id, str(exc))

    async def _handle_task(self, envelope: Dict[str, Any]) -> None:
        """Handle a task by calling the wrapped callback function."""
        assert self.callback is not None

        self.total_requests += 1

        request_id = envelope.get("id")
        message = envelope.get("payload") or {}

        try:
            result = await self._maybe_await(self.callback(message))

            if result is None:
                result = {"status": "completed"}
            elif not isinstance(result, dict):
                result = {"status": "completed", "data": result}

            await self._send_response(
                {
                    "id": request_id,
                    "status": "ok",
                    "result": result,
                }
            )
        except Exception as exc:
            self.failed_requests += 1
            logger.exception(f"Error calling callback '{self.function_name}': {exc}")
            await self._send_error_response(request_id, str(exc))

    async def _send_response(self, response: Dict[str, Any]) -> None:
        """Send a JSON response to stdout."""
        sys.stdout.write(json.dumps(response) + "\n")
        sys.stdout.flush()

    async def _send_error_response(self, request_id: Optional[str], error: str) -> None:
        """Send an error response to stdout."""
        await self._send_response(
            {
                "id": request_id,
                "status": "error",
                "error": error,
            }
        )

    @staticmethod
    async def _maybe_await(result: Any) -> Any:
        """Await coroutine results automatically."""
        if asyncio.iscoroutine(result):
            return await result
        return result


def main() -> None:
    """Entry point for the adapter."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Python Callback Adapter - Run any Python callback via stdio"
    )
    parser.add_argument(
        "--module",
        required=True,
        help="Dot-separated module path (e.g., 'examples.omnicoreagent_dir.agent_runner')",
    )
    parser.add_argument(
        "--function",
        required=True,
        help="Name of the callback function to call",
    )

    args = parser.parse_args()

    adapter = PythonCallbackAdapter(args.module, args.function)
    try:
        asyncio.run(adapter.run())
    except KeyboardInterrupt:
        logger.info("Adapter interrupted by user")
    except Exception as e:
        logger.exception(f"Adapter error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
