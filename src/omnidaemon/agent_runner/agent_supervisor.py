import asyncio
import json
import logging
import os
import sys
from typing import Any, Dict, Optional
from uuid import uuid4
import time
import random
from omnidaemon.agent_runner.types import AgentProcessConfig, AgentState, AgentMetadata
from omnidaemon.storage.base import BaseStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("AgentSupervisor")


class AgentSupervisor:
    """
    Launches and manages a single agent process, communicating over stdio.

    The agent process must accept newline-delimited JSON messages on stdin and
    respond with JSON per line on stdout. Each request is expected to contain a unique
    "id" so responses can be correlated.
    """

    def __init__(self, config: AgentProcessConfig, store: BaseStore):
        self.config = config
        self.store = store
        self._process: Optional[asyncio.subprocess.Process] = None
        self._stdout_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._pending: Dict[str, asyncio.Future] = {}
        self._write_lock = asyncio.Lock()
        self._restart_attempts = 0
        self._state: AgentState = AgentState.IDLE
        self._metadata: AgentMetadata = AgentMetadata()
        self._stopping = False

    async def _transition_to(self, new_state: AgentState) -> None:
        """Transition to a new state and persist metadata."""
        old_state = self._state
        self._state = new_state
        logger.info(
            "[%s] State transition: %s -> %s",
            self.config.name,
            old_state.value,
            new_state.value,
        )

        await self._save_metric_safe(
            {
                "event": "agent_state_change",
                "agent_name": self.config.name,
                "old_state": old_state.value,
                "new_state": new_state.value,
                "timestamp": time.time(),
            }
        )

        await self._save_supervisor_state()

    async def _save_supervisor_state(self) -> None:
        """Persist supervisor state to storage for cross-process access.

        This saves the current supervisor state so it can be queried from anywhere
        with storage access (e.g., SDK, API, other processes). Includes:
        - Current state and health
        - Process info (PID)
        - Resource usage (CPU, memory)
        - Lifecycle metrics (uptime, restarts, heartbeat)

        State is always saved (overwrites previous) to ensure metrics stay current.
        """
        state_data = {
            "agent_name": self.config.name,
            "state": self._state.value,
            "healthy": self._state == AgentState.RUNNING,
            "pid": self._process.pid if self._process else None,
            "start_time": self._metadata.start_time,
            "cpu_percent": self._metadata.cpu_percent,
            "memory_mb": self._metadata.memory_mb,
            "total_requests": self._metadata.total_requests,
            "restart_count": self._metadata.restart_count,
            "last_heartbeat": self._metadata.last_health_check,
            "last_updated": time.time(),
        }

        await self._save_config_safe(f"supervisor:{self.config.name}", state_data)

    async def _save_metric_safe(self, metric: dict) -> bool:
        """Save metric with timeout and graceful error handling.

        Returns:
            True if saved successfully, False if storage unavailable/failed
        """

        try:
            await asyncio.wait_for(
                self.store.save_metric(metric),
                timeout=5.0,
            )
            return True
        except asyncio.TimeoutError:
            logger.warning(
                "[%s] Storage timeout saving metric (event: %s)",
                self.config.name,
                metric.get("event", "unknown"),
            )
            return False
        except Exception as e:
            logger.error("[%s] Storage error saving metric: %s", self.config.name, e)
            return False

    async def _save_config_safe(self, key: str, value: Any) -> bool:
        """Save config with timeout and graceful error handling.

        Returns:
            True if saved successfully, False if storage unavailable/failed
        """

        try:
            await asyncio.wait_for(self.store.save_config(key, value), timeout=5.0)
            return True
        except asyncio.TimeoutError:
            logger.warning(
                "[%s] Storage timeout saving config (key: %s)", self.config.name, key
            )
            return False
        except Exception as e:
            logger.error("[%s] Storage error saving config: %s", self.config.name, e)
            return False

    async def start(self) -> None:
        """Spawn the agent process if it is not already running."""
        if self._process and self._process.returncode is None:
            return

        await self._transition_to(AgentState.STARTING)

        env = os.environ.copy()
        if self.config.env:
            env.update(self.config.env)

        logger.debug(
            "[%s] Launching agent: %s %s",
            self.config.name,
            self.config.command,
            " ".join(self.config.args),
        )

        try:
            self._process = await asyncio.create_subprocess_exec(
                self.config.command,
                *self.config.args,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.config.cwd,
                env=env,
            )

            self._metadata.start_time = time.time()
            self._metadata.restart_count = self._restart_attempts

            self._stopping = False

            self._stdout_task = asyncio.create_task(self._read_stdout())
            self._stderr_task = asyncio.create_task(self._stream_stderr())
            self._heartbeat_task = asyncio.create_task(self._run_heartbeat_loop())
            asyncio.create_task(self._wait_for_exit())

            await self._transition_to(AgentState.RUNNING)

        except Exception as e:
            logger.error("[%s] Failed to start agent: %s", self.config.name, e)
            await self._transition_to(AgentState.CRASHED)
            raise

    async def stop(self) -> None:
        """Terminate the agent process and clean up using 3-phase shutdown."""
        if self._state in (AgentState.STOPPED, AgentState.IDLE):
            return

        await self._transition_to(AgentState.STOPPING)
        self._stopping = True

        if not self._process:
            await self._transition_to(AgentState.STOPPED)
            return

        if self._process.stdin:
            try:
                logger.info("[%s] Sending shutdown signal...", self.config.name)
                self._process.stdin.write(
                    json.dumps({"type": "shutdown"}).encode() + b"\n"
                )
                await self._process.stdin.drain()

                try:
                    await asyncio.wait_for(
                        self._process.wait(), timeout=self.config.graceful_timeout_sec
                    )
                    logger.info("[%s] Agent shutdown gracefully", self.config.name)
                    await self._cleanup_tasks()
                    self._process = None
                    await self._transition_to(AgentState.STOPPED)
                    return
                except asyncio.TimeoutError:
                    logger.warning("[%s] Graceful shutdown timed out", self.config.name)
            except Exception as e:
                logger.warning(
                    "[%s] Failed to send shutdown signal: %s", self.config.name, e
                )

        if self._process and self._process.returncode is None:
            logger.info("[%s] Sending SIGTERM...", self.config.name)
            try:
                self._process.terminate()
                try:
                    await asyncio.wait_for(
                        self._process.wait(), timeout=self.config.sigterm_timeout_sec
                    )
                    logger.info("[%s] Agent stopped via SIGTERM", self.config.name)
                    await self._cleanup_tasks()
                    self._process = None
                    await self._transition_to(AgentState.STOPPED)
                    return
                except asyncio.TimeoutError:
                    logger.warning("[%s] SIGTERM timed out", self.config.name)
            except ProcessLookupError:
                pass

        if self._process and self._process.returncode is None:
            logger.warning("[%s] Sending SIGKILL...", self.config.name)
            try:
                self._process.kill()
                await self._process.wait()
                logger.info("[%s] Agent killed via SIGKILL", self.config.name)
            except ProcessLookupError:
                pass

        await self._cleanup_tasks()
        self._process = None
        await self._transition_to(AgentState.STOPPED)

    async def _cleanup_tasks(self) -> None:
        """Cancel and cleanup all async tasks (stdout, stderr, heartbeat)."""
        if self._stdout_task:
            self._stdout_task.cancel()
            try:
                await self._stdout_task
            except asyncio.CancelledError:
                pass
        if self._stderr_task:
            self._stderr_task.cancel()
            try:
                await self._stderr_task
            except asyncio.CancelledError:
                pass
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        self._stdout_task = None
        self._stderr_task = None
        self._heartbeat_task = None

    async def handle_event(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send an event to the agent process and await the response.

        Args:
            payload: Event dictionary received from OmniDaemon.

        Returns:
            The agent response payload (dict).
        """
        if not self._process or self._process.returncode is not None:
            await self.start()

        if not self._process or not self._process.stdin:
            raise RuntimeError(f"[{self.config.name}] Agent process unavailable")

        request_id = str(uuid4())
        envelope = {
            "id": request_id,
            "type": "task",
            "payload": payload,
        }

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._pending[request_id] = future

        async with self._write_lock:
            message = json.dumps(envelope).encode() + b"\n"
            self._process.stdin.write(message)
            await self._process.stdin.drain()

        try:
            response = await asyncio.wait_for(
                future, timeout=self.config.request_timeout
            )
        except asyncio.TimeoutError:
            if not future.done():
                future.cancel()
            self._pending.pop(request_id, None)
            raise TimeoutError(
                f"[{self.config.name}] Timeout waiting for response (id={request_id})"
            )
        finally:
            self._pending.pop(request_id, None)

        if isinstance(response, dict):
            status = response.get("status", "ok")
            if status != "ok":
                raise RuntimeError(
                    f"[{self.config.name}] Agent error: {response.get('error', 'unknown error')}"
                )
            return response.get("result") or {}

        return {}

    async def _read_stdout(self) -> None:
        assert self._process and self._process.stdout
        while True:
            line = await self._process.stdout.readline()
            if not line:
                break

            decoded_line = line.decode().strip()
            if not decoded_line:
                continue

            try:
                message = json.loads(decoded_line)
                request_id = message.get("id")
                if request_id and request_id in self._pending:
                    future = self._pending[request_id]
                    if not future.done():
                        future.set_result(message)
            except json.JSONDecodeError:
                logger.debug(
                    "[%s] Non-JSON output on stdout (agent log): %s",
                    self.config.name,
                    decoded_line[:100],
                )
        logger.info("[%s] stdout closed", self.config.name)

    async def _stream_stderr(self) -> None:
        assert self._process and self._process.stderr
        while True:
            line = await self._process.stderr.readline()
            if not line:
                break
            decoded_line = line.decode().rstrip()
            if not decoded_line:
                continue

            log_level = self._parse_log_level(decoded_line)

            if log_level == "ERROR":
                logger.error("[%s] %s", self.config.name, decoded_line)
            elif log_level == "WARNING" or log_level == "WARN":
                logger.warning("[%s] %s", self.config.name, decoded_line)
            elif log_level == "DEBUG":
                logger.debug("[%s] %s", self.config.name, decoded_line)
            else:
                logger.info("[%s] %s", self.config.name, decoded_line)
        logger.info("[%s] stderr closed", self.config.name)

    @staticmethod
    def _parse_log_level(line: str) -> str:
        """
        Parse log level from a log line.

        Supports formats:
        - "INFO:module:message"
        - "ERROR:module:message"
        - "2025-11-20 18:33:02 - module - INFO - message"
        - "2025-11-20 18:33:02 - module - ERROR - message"
        """
        import re

        level_match = re.match(
            r"^(ERROR|WARNING|WARN|INFO|DEBUG|CRITICAL):", line, re.IGNORECASE
        )
        if level_match:
            return level_match.group(1).upper()

        timestamp_match = re.search(
            r" - \w+ - (ERROR|WARNING|WARN|INFO|DEBUG|CRITICAL) - ", line, re.IGNORECASE
        )
        if timestamp_match:
            return timestamp_match.group(1).upper()

        level_in_line = re.search(
            r"\b(ERROR|WARNING|WARN|INFO|DEBUG|CRITICAL)\b", line, re.IGNORECASE
        )
        if level_in_line:
            return level_in_line.group(1).upper()

        return "INFO"

    async def _wait_for_exit(self) -> None:
        """Wait for agent process to exit and trigger restart if needed."""
        assert self._process
        await self._process.wait()
        logger.warning(
            "[%s] Agent process exited with code %s",
            self.config.name,
            self._process.returncode,
        )
        await self._cleanup_tasks()
        if not self._stopping and self.config.restart_on_exit:
            await self._restart_if_needed()

    async def _restart_if_needed(self) -> None:
        """Attempt to restart the agent process with exponential backoff."""
        if self._stopping:
            return

        await self._transition_to(AgentState.CRASHED)

        if self._restart_attempts >= self.config.max_restart_attempts:
            logger.error(
                "[%s] Max restart attempts (%d) reached; agent will remain stopped",
                self.config.name,
                self.config.max_restart_attempts,
            )
            self._reject_all_pending(
                RuntimeError("Agent process stopped and cannot be restarted")
            )
            await self._transition_to(AgentState.CRASHED)
            return

        self._restart_attempts += 1
        await self._transition_to(AgentState.RESTARTING)

        base_delay = self.config.restart_backoff_seconds
        delay = min(60.0, base_delay * (2 ** (self._restart_attempts - 1)))
        jitter = random.uniform(0, 0.1 * delay)
        final_delay = delay + jitter

        logger.info(
            "[%s] Restarting agent in %.2f seconds (attempt %s/%s)",
            self.config.name,
            final_delay,
            self._restart_attempts,
            self.config.max_restart_attempts,
        )

        await asyncio.sleep(final_delay)
        try:
            await self.start()
            self._reject_all_pending(
                RuntimeError("Agent restarted; previous requests were cancelled")
            )
        except Exception as exc:
            logger.exception("[%s] Failed to restart agent: %s", self.config.name, exc)
            await self._restart_if_needed()

    # _cleanup_tasks is defined above at line 251.
    # Removing duplicate definition here.

    async def _run_heartbeat_loop(self) -> None:
        """Periodic heartbeat loop to check agent health."""
        logger.debug(
            "[%s] Heartbeat loop starting, waiting 5s for agent initialization",
            self.config.name,
        )
        await asyncio.sleep(5.0)

        logger.debug(
            "[%s] Heartbeat loop active, interval=%ds",
            self.config.name,
            self.config.heartbeat_interval_seconds,
        )

        while not self._stopping:
            try:
                if not self._process or self._process.returncode is not None:
                    logger.warning(
                        "[%s] Heartbeat loop exiting: process not running (returncode=%s)",
                        self.config.name,
                        self._process.returncode if self._process else "no process",
                    )
                    break

                await asyncio.sleep(self.config.heartbeat_interval_seconds)

                if (
                    self._stopping
                    or not self._process
                    or self._process.returncode is not None
                ):
                    logger.info(
                        "[%s] Heartbeat loop exiting after sleep: stopping=%s, returncode=%s",
                        self.config.name,
                        self._stopping,
                        self._process.returncode if self._process else "no process",
                    )
                    break

                ping_id = str(uuid4())
                start_time = time.time()

                logger.debug(
                    "[%s] Sending heartbeat ping (id=%s)", self.config.name, ping_id[:8]
                )

                try:
                    async with self._write_lock:
                        if self._process and self._process.stdin:
                            ping_envelope = {
                                "id": ping_id,
                                "type": "ping",
                                "payload": {},
                            }
                            message = json.dumps(ping_envelope).encode() + b"\n"
                            self._process.stdin.write(message)
                            await self._process.stdin.drain()

                    loop = asyncio.get_running_loop()
                    pong_future: asyncio.Future = loop.create_future()
                    self._pending[ping_id] = pong_future

                    try:
                        response = await asyncio.wait_for(pong_future, timeout=10.0)
                        latency_ms = (time.time() - start_time) * 1000

                        self._metadata.last_health_check = time.time()

                        if isinstance(response, dict):
                            health = response.get("result", {}).get("health", {})

                            logger.debug(
                                "[%s] Received health data: %s",
                                self.config.name,
                                health,
                            )

                            if health:
                                new_cpu = health.get("cpu_percent", 0.0)
                                new_memory = health.get("memory_mb", 0.0)
                                new_requests = health.get("total_requests", 0)

                                self._metadata.cpu_percent = new_cpu
                                self._metadata.memory_mb = new_memory
                                self._metadata.total_requests = new_requests

                                logger.debug(
                                    "[%s] Updated metrics - CPU: %.1f%%, Memory: %.1fMB, Requests: %d",
                                    self.config.name,
                                    new_cpu,
                                    new_memory,
                                    new_requests,
                                )

                        await self._save_supervisor_state()

                        logger.debug(
                            "[%s] Heartbeat successful (latency: %.1fms, cpu: %.1f%%, mem: %.1fMB, requests: %d)",
                            self.config.name,
                            latency_ms,
                            self._metadata.cpu_percent,
                            self._metadata.memory_mb,
                            self._metadata.total_requests,
                        )

                    except asyncio.TimeoutError:
                        logger.warning(
                            "[%s] Heartbeat timeout (no pong response after 10s)",
                            self.config.name,
                        )

                        pending_count = len(self._pending)
                        if pending_count > 1:
                            logger.info(
                                "[%s] Agent unresponsive to heartbeat but has %d pending tasks. "
                                "Assuming BUSY, skipping restart.",
                                self.config.name,
                                pending_count - 1,
                            )

                        else:
                            logger.error(
                                "[%s] Agent unresponsive and IDLE (no pending tasks). "
                                "Assuming CRASHED/FROZEN, initiating restart.",
                                self.config.name,
                            )
                            await self.stop()
                            await self._restart_if_needed()
                    finally:
                        self._pending.pop(ping_id, None)

                except Exception as e:
                    logger.warning(
                        "[%s] Heartbeat send failed: %s", self.config.name, e
                    )

            except asyncio.CancelledError:
                logger.info("[%s] Heartbeat loop cancelled", self.config.name)
                break
            except Exception as e:
                logger.error("[%s] Error in heartbeat loop: %s", self.config.name, e)
                await asyncio.sleep(5.0)

        logger.info("[%s] Heartbeat loop stopped", self.config.name)

    def _reject_all_pending(self, exc: Exception) -> None:
        """Reject all pending request futures with the given exception."""
        for future in self._pending.values():
            if not future.done():
                future.set_exception(exc)
        self._pending.clear()
