"""
Generic Python Callback Adapter

This adapter allows any existing Python callback function to run in a separate process
without modifying the original agent code. It communicates via stdio using JSON.

Usage:
    python -m omnidaemon.agent_runner.python_callback_adapter \
        --module examples.omnicoreagent_dir.agent_runner \
        --function call_file_system_agent
"""

import asyncio
import json
import logging
import sys
import importlib
from typing import Any, Callable, Dict, Optional

# Configure logging to use stderr so stdout is only for JSON responses
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,  # Logs go to stderr, not stdout
)
logger = logging.getLogger("PythonCallbackAdapter")


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

    def _load_callback(self) -> None:
        """Dynamically import the module and get the callback function."""
        try:
            module = importlib.import_module(self.module_path)
            self.callback = getattr(module, self.function_name)
            if not callable(self.callback):
                raise ValueError(
                    f"'{self.function_name}' in module '{self.module_path}' is not callable"
                )
            logger.info(
                f"Loaded callback '{self.function_name}' from module '{self.module_path}'"
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
            # Read line from stdin (blocking, but we run in executor)
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

            if message_type != "task":
                logger.warning(f"Unknown message type: {message_type}")
                await self._send_error_response(
                    request_id, f"Unknown message type: {message_type}"
                )
                continue

            await self._handle_task(envelope)

    async def _handle_task(self, envelope: Dict[str, Any]) -> None:
        """Handle a task by calling the wrapped callback function."""
        assert self.callback is not None

        request_id = envelope.get("id")
        # The payload IS the message dict that OmniDaemon would pass to the callback
        message = envelope.get("payload") or {}

        try:
            # Call the original callback function with the message
            result = await self._maybe_await(self.callback(message))

            # Ensure result is a dict
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
