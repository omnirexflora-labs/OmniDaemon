from omnidaemon.agent_runner.types import AgentProcessConfig
from typing import Dict, Optional, Literal
from omnidaemon.agent_runner.agent_supervisor import AgentSupervisor
from pathlib import Path
import sys
import os
from omnidaemon.agent_runner.module_discovery import (
    _detect_language,
    _find_callback_in_directory,
)
from omnidaemon.agent_runner.dependency_manager import (
    _ensure_python_dependencies,
)
from omnidaemon.agent_runner.utils import _load_env_files
from omnidaemon.agent_runner.supervisor_storage import (
    get_supervisor_state,
    register_supervisor_in_storage,
    unregister_supervisor_from_storage,
)
from omnidaemon.storage import store as default_store
import logging

logger = logging.getLogger(__name__)


_supervisor_registry: Dict[str, AgentSupervisor] = {}


async def create_supervisor_from_directory(
    agent_name: str,
    agent_dir: str,
    callback_function: str,
    language: Optional[Literal["python"]] = None,
    request_timeout: float = 300.0,
    heartbeat_interval_seconds: float = 60.0,
    restart_on_exit: bool = True,
    max_restart_attempts: int = 5,
    restart_backoff_seconds: float = 2.0,
) -> AgentSupervisor:
    """
    Create a supervisor for an agent by discovering it in a directory.

    This is a convenience factory function that:
    1. Detects the language from the directory structure
    2. Finds the callback function in the directory
    3. Creates and starts the supervisor

    Args:
        agent_name: Unique name for this agent
        agent_dir: Path to agent directory containing the agent code
        callback_function: Name of the callback function to call (entry point)
        language: Optional language hint. If not provided, will be auto-detected.
        request_timeout: Timeout for requests in seconds
        heartbeat_interval_seconds: Interval between heartbeats in seconds
        restart_on_exit: Whether to restart the agent if it exits
        max_restart_attempts: Maximum number of restart attempts
        restart_backoff_seconds: Backoff delay between restarts

    Returns:
        AgentSupervisor instance (already started)
    """
    if agent_name in _supervisor_registry:
        existing = _supervisor_registry[agent_name]
        logger.debug(
            f"Supervisor '{agent_name}' already exists in local registry, returning existing instance"
        )
        return existing

    store = default_store

    existing_state = await get_supervisor_state(agent_name)
    if existing_state and existing_state.get("state") == "RUNNING":
        pid = existing_state.get("pid")
        if pid and pid != os.getpid():
            raise RuntimeError(
                f"Agent '{agent_name}' is already running in process {pid}. "
                "Please stop it first or use a different agent name."
            )

    if language is None:
        language = _detect_language(agent_dir)

    logger.info(
        f"Setting up supervisor for '{agent_name}' "
        f"(language: {language}, dir: {agent_dir}, callback: {callback_function})"
    )

    agent_path = Path(agent_dir).resolve()
    env_overrides = _load_env_files(Path.cwd() / ".env", agent_path / ".env")
    env_for_process = env_overrides if env_overrides else None

    python_env = {}
    extra_paths = []
    if language == "python":
        python_env = await _ensure_python_dependencies(agent_path)
        if python_env:
            env_for_process = env_for_process or {}
            env_for_process.update(python_env)

            project_root = str(Path.cwd())
            current_pp = env_for_process.get("PYTHONPATH", "")
            if project_root not in current_pp.split(os.pathsep):
                env_for_process["PYTHONPATH"] = (
                    f"{project_root}{os.pathsep}{current_pp}"
                    if current_pp
                    else project_root
                )

            if "PYTHONPATH" in python_env:
                extra_paths = python_env["PYTHONPATH"].split(os.pathsep)

    if language == "python":
        module_path, func_name = _find_callback_in_directory(
            agent_dir, callback_function, language, extra_paths=extra_paths
        )

        adapter_module = "omnidaemon.agent_runner.python_callback_adapter"
        config = AgentProcessConfig(
            name=agent_name,
            command=sys.executable,
            args=[
                "-m",
                adapter_module,
                "--module",
                module_path,
                "--function",
                func_name,
            ],
            cwd=str(agent_path),
            request_timeout=request_timeout,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            restart_on_exit=restart_on_exit,
            max_restart_attempts=max_restart_attempts,
            restart_backoff_seconds=restart_backoff_seconds,
            env=env_for_process,
        )

    else:
        raise ValueError(f"Unsupported language: {language}")

    supervisor = AgentSupervisor(config, store=store)
    await supervisor.start()

    _supervisor_registry[agent_name] = supervisor

    await register_supervisor_in_storage(agent_name)

    logger.info(
        f"Supervisor created for '{agent_name}' "
        f"(dir: {agent_dir}, callback: {callback_function})"
    )
    return supervisor


async def shutdown_all_supervisors() -> None:
    """Shutdown all registered supervisors."""
    for name, supervisor in list(_supervisor_registry.items()):
        try:
            await supervisor.stop()
            await unregister_supervisor_from_storage(name)
            logger.info(f"Supervisor '{name}' shutdown and unregistered complete")
        except Exception as exc:
            logger.error(
                f"Error shutting down supervisor '{name}': {exc}", exc_info=True
            )
    _supervisor_registry.clear()
