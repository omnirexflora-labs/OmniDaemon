import asyncio
import hashlib
import importlib
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Tuple
from uuid import uuid4

logger = logging.getLogger(__name__)

# Global registry of supervisors by agent name
_supervisor_registry: Dict[str, "AgentSupervisor"] = {}


def _load_env_files(*paths: Path) -> Dict[str, str]:
    """
    Load simple KEY=VALUE pairs from one or more .env files.

    Later files override earlier ones. Lines starting with '#' or blank lines
    are ignored. Quotes around values are stripped.
    """
    env: Dict[str, str] = {}
    for path in paths:
        if not path:
            continue
        try:
            resolved = path.resolve()
        except Exception:
            continue
        if not resolved.exists() or not resolved.is_file():
            continue
        try:
            with resolved.open("r") as fh:
                for raw_line in fh:
                    line = raw_line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip("'\"")
                    if key:
                        env[key] = value
        except Exception as exc:
            logger.warning("Failed to load env file %s: %s", resolved, exc)
            continue
    return env


@dataclass
class AgentProcessConfig:
    """Configuration used to launch and supervise an agent process."""

    name: str
    command: str
    args: list[str] = field(default_factory=list)
    env: Optional[Dict[str, str]] = None
    cwd: Optional[str] = None
    request_timeout: float = 60.0
    restart_on_exit: bool = True
    max_restart_attempts: int = 3
    restart_backoff_seconds: float = 5.0


class AgentSupervisor:
    """
    Launches and manages a single agent process, communicating over stdio.

    The agent process must accept newline-delimited JSON messages on stdin and
    respond with JSON per line on stdout. Each request is expected to contain a unique
    "id" so responses can be correlated.
    """

    def __init__(self, config: AgentProcessConfig):
        self.config = config
        self._process: Optional[asyncio.subprocess.Process] = None
        self._stdout_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._pending: Dict[str, asyncio.Future] = {}
        self._write_lock = asyncio.Lock()
        self._restart_attempts = 0
        self._stopping = False

    async def start(self) -> None:
        """Spawn the agent process if it is not already running."""
        if self._process and self._process.returncode is None:
            return

        env = os.environ.copy()
        if self.config.env:
            env.update(self.config.env)

        logger.info(
            "[%s] Launching agent: %s %s",
            self.config.name,
            self.config.command,
            " ".join(self.config.args),
        )
        self._process = await asyncio.create_subprocess_exec(
            self.config.command,
            *self.config.args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.config.cwd,
            env=env,
        )
        self._stopping = False
        self._restart_attempts = 0
        self._stdout_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._stream_stderr())
        asyncio.create_task(self._wait_for_exit())

    async def stop(self) -> None:
        """Terminate the agent process and clean up."""
        self._stopping = True
        if not self._process:
            return
        if self._process.stdin:
            try:
                self._process.stdin.write(
                    json.dumps({"type": "shutdown"}).encode() + b"\n"
                )
                await self._process.stdin.drain()
            except Exception:
                pass
        self._process.terminate()
        await self._process.wait()
        self._cleanup_tasks()
        self._process = None

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
            try:
                message = json.loads(line.decode().strip())
                request_id = message.get("id")
                if request_id and request_id in self._pending:
                    future = self._pending[request_id]
                    if not future.done():
                        future.set_result(message)
            except json.JSONDecodeError:
                logger.warning(
                    "[%s] Invalid JSON from agent: %r", self.config.name, line
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

            # Parse log level from the line to log at appropriate level
            # Common formats: "INFO:module:message", "ERROR:module:message", "2025-11-20 18:33:02 - module - INFO - message"
            log_level = self._parse_log_level(decoded_line)

            if log_level == "ERROR":
                logger.error("[%s] %s", self.config.name, decoded_line)
            elif log_level == "WARNING" or log_level == "WARN":
                logger.warning("[%s] %s", self.config.name, decoded_line)
            elif log_level == "DEBUG":
                logger.debug("[%s] %s", self.config.name, decoded_line)
            else:  # INFO or unknown - default to INFO
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

        # Try to match standard Python logging format: LEVEL:module:message
        level_match = re.match(
            r"^(ERROR|WARNING|WARN|INFO|DEBUG|CRITICAL):", line, re.IGNORECASE
        )
        if level_match:
            return level_match.group(1).upper()

        # Try to match timestamp format: "2025-11-20 18:33:02 - module - LEVEL - message"
        timestamp_match = re.search(
            r" - \w+ - (ERROR|WARNING|WARN|INFO|DEBUG|CRITICAL) - ", line, re.IGNORECASE
        )
        if timestamp_match:
            return timestamp_match.group(1).upper()

        # Try to match any occurrence of log level in the line
        level_in_line = re.search(
            r"\b(ERROR|WARNING|WARN|INFO|DEBUG|CRITICAL)\b", line, re.IGNORECASE
        )
        if level_in_line:
            return level_in_line.group(1).upper()

        # Default to INFO if no level detected
        return "INFO"

    async def _wait_for_exit(self) -> None:
        assert self._process
        await self._process.wait()
        logger.warning(
            "[%s] Agent process exited with code %s",
            self.config.name,
            self._process.returncode,
        )
        self._cleanup_tasks()
        if not self._stopping and self.config.restart_on_exit:
            await self._restart_if_needed()

    async def _restart_if_needed(self) -> None:
        if self._restart_attempts >= self.config.max_restart_attempts:
            logger.error(
                "[%s] Max restart attempts reached; agent will remain stopped",
                self.config.name,
            )
            self._reject_all_pending(
                RuntimeError("Agent process stopped and cannot be restarted")
            )
            return
        self._restart_attempts += 1
        delay = self.config.restart_backoff_seconds * self._restart_attempts
        logger.info(
            "[%s] Restarting agent in %s seconds (attempt %s/%s)",
            self.config.name,
            delay,
            self._restart_attempts,
            self.config.max_restart_attempts,
        )
        await asyncio.sleep(delay)
        try:
            await self.start()
            self._reject_all_pending(
                RuntimeError("Agent restarted; previous requests were cancelled")
            )
        except Exception as exc:
            logger.exception("[%s] Failed to restart agent: %s", self.config.name, exc)
            await self._restart_if_needed()

    def _cleanup_tasks(self) -> None:
        if self._stdout_task:
            self._stdout_task.cancel()
        if self._stderr_task:
            self._stderr_task.cancel()
        self._stdout_task = None
        self._stderr_task = None

    def _reject_all_pending(self, exc: Exception) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(exc)
        self._pending.clear()


# Factory functions for creating supervisors from agent directories


def _detect_language(agent_path: str) -> Literal["python", "go", "typescript"]:
    """
    Detect the language/runtime based on directory structure.
    """
    path = Path(agent_path).resolve()

    if not path.is_dir():
        raise ValueError(
            f"Agent path must be a directory containing the callback and dependencies: {agent_path}"
        )

    if (path / "main.go").exists() or (path / "agent.go").exists():
        return "go"
    if (path / "tsconfig.json").exists() or _has_typescript_sources(path):
        return "typescript"
    if (path / "__init__.py").exists() or any(path.rglob("*.py")):
        return "python"

    raise ValueError(
        "Unable to detect agent language. Ensure the directory contains either Python files, "
        "Go sources, or a TypeScript/JavaScript package.json."
    )


def _directory_to_module_path(agent_dir: str) -> str:
    """
    Convert a directory path to a Python module path.

    Example:
        examples/omnicoreagent_dir/agents/omnicore_agent
        -> examples.omnicoreagent_dir.agents.omnicore_agent
    """
    # Handle relative paths
    if not Path(agent_dir).is_absolute():
        path = (Path.cwd() / agent_dir).resolve()
    else:
        path = Path(agent_dir).resolve()

    cwd = Path.cwd()

    try:
        # Get relative path from current directory
        rel_path = path.relative_to(cwd)
        # Convert to module path
        module_path = str(rel_path).replace(os.sep, ".").replace("/", ".")
        # Remove leading dots if any
        module_path = module_path.lstrip(".")
        return module_path
    except ValueError:
        # If can't make relative, try to find it in sys.path
        # This handles cases where the directory is in PYTHONPATH
        for sys_path in sys.path:
            try:
                rel_path = path.relative_to(Path(sys_path).resolve())
                module_path = str(rel_path).replace(os.sep, ".").replace("/", ".")
                module_path = module_path.lstrip(".")
                if module_path:
                    return module_path
            except (ValueError, OSError):
                continue
        # Last resort: use absolute path (won't work for imports, but better error)
        return str(path).replace(os.sep, ".").replace("/", ".")


def _has_typescript_sources(agent_path: Path) -> bool:
    ignore_dirs = {
        "node_modules",
        ".git",
        ".omnidaemon_pydeps",
        ".omnidaemon_cache",
        ".omnidaemon_tsbuild",
        ".venv",
        "venv",
        "__pycache__",
    }
    for root, dirs, files in os.walk(agent_path):
        dirs[:] = [d for d in dirs if d not in ignore_dirs]
        for filename in files:
            if filename.endswith(".ts") or filename.endswith(".tsx"):
                return True
    return False


def _find_callback_in_directory(
    agent_dir: str,
    callback_name: str,
    language: str,
    python_env: Optional[Dict[str, str]] = None,
) -> Tuple[str, str]:
    """
    Find the callback function in the agent directory.

    For Python: Looks for callback.py in the agent directory and imports it.

    Args:
        agent_dir: Path to agent directory
        callback_name: Name of the callback function to find
        language: Language type ("python", "go", etc.)
        python_env: Optional Python environment variables (including PYTHONPATH with dependencies)

    Returns:
        Tuple of (module_path, function_name)
        For Python: (module.path.to.callback, function_name)
    """
    if language == "python":
        dir_path = Path(agent_dir)
        # Resolve to absolute path
        if not dir_path.is_absolute():
            dir_path = Path.cwd() / dir_path
        dir_path = dir_path.resolve()

        if not dir_path.is_dir():
            raise ValueError(
                f"Agent directory does not exist: {agent_dir} (resolved to: {dir_path})"
            )

        logger.info(f"Looking for callback '{callback_name}' in {dir_path}/callback.py")

        # Look for callback.py in the agent directory
        callback_file = dir_path / "callback.py"
        if not callback_file.exists():
            raise ValueError(
                f"callback.py not found in agent directory: {agent_dir}\n"
                f"Expected file: {callback_file}"
            )

        # Set up PYTHONPATH for imports - this must include dependencies!
        project_root = Path.cwd()
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
            logger.debug(f"Added project root to sys.path: {project_root}")

        # Add agent's dependencies to sys.path if available
        if python_env and "PYTHONPATH" in python_env:
            pythonpath = python_env["PYTHONPATH"]
            # PYTHONPATH is colon/semicolon separated
            separator = ";" if os.name == "nt" else ":"
            for path_str in pythonpath.split(separator):
                if path_str and path_str not in sys.path:
                    sys.path.insert(0, path_str)
                    logger.debug(f"Added to sys.path (from dependencies): {path_str}")

        # Convert callback.py path to module path relative to project root
        try:
            rel_path = callback_file.relative_to(project_root)
            # Convert to module path: remove .py extension, replace / with .
            file_module = (
                str(rel_path.with_suffix("")).replace(os.sep, ".").replace("/", ".")
            )
            # Remove leading dots
            file_module = file_module.lstrip(".")
        except ValueError:
            # File is not under project root, use directory-based module path
            agent_module_base = _directory_to_module_path(str(dir_path))
            file_module = f"{agent_module_base}.callback"

        logger.debug(f"Importing module: {file_module}")

        # Import the callback module and check for the function
        try:
            module = importlib.import_module(file_module)
            if hasattr(module, callback_name):
                func = getattr(module, callback_name)
                if callable(func):
                    logger.info(f"Found callback '{callback_name}' in {file_module}")
                    return (file_module, callback_name)
                else:
                    raise ValueError(
                        f"'{callback_name}' in {file_module} is not callable"
                    )
            else:
                available = [name for name in dir(module) if not name.startswith("_")]
                raise ValueError(
                    f"Callback function '{callback_name}' not found in {file_module}.\n"
                    f"Available functions/attributes: {', '.join(available) if available else 'none'}"
                )
        except ImportError as e:
            raise ValueError(
                f"Failed to import {file_module}: {e}\n"
                f"This might be due to missing dependencies or incorrect module path.\n"
                f"File: {callback_file}"
            ) from e
        except Exception as e:
            raise ValueError(
                f"Error importing {file_module}: {e}\nFile: {callback_file}"
            ) from e
    else:
        raise ValueError(
            f"Language '{language}' not yet supported for dynamic callback discovery"
        )


def _resolve_typescript_entry(agent_path: Path) -> Path:
    """
    Resolve the JavaScript entry file for a TypeScript/JavaScript agent.

    Prefers values defined in package.json (omnidaemonEntry, module, main, exports).
    Falls back to common dist/build locations.
    """

    if not agent_path.is_dir():
        raise ValueError(f"Agent directory does not exist: {agent_path}")

    candidates: list[Path] = []

    package_json = agent_path / "package.json"
    if package_json.exists():
        try:
            package_data = json.loads(package_json.read_text())
            for key in ("omnidaemonEntry", "module", "main"):
                value = package_data.get(key)
                if isinstance(value, str):
                    candidates.append(agent_path / value)
            exports_value = package_data.get("exports")
            if isinstance(exports_value, str):
                candidates.append(agent_path / exports_value)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Failed to parse package.json at {package_json}: {exc}"
            ) from exc

    fallback_paths = [
        "dist/callback.mjs",
        "dist/callback.js",
        "dist/index.mjs",
        "dist/index.js",
        "build/callback.js",
        "build/index.js",
        "callback.mjs",
        "callback.js",
        "index.mjs",
        "index.js",
    ]

    for relative in fallback_paths:
        candidates.append(agent_path / relative)

    for candidate in candidates:
        resolved = candidate
        if resolved.is_dir():
            resolved = resolved / "index.js"
        resolved = resolved.resolve()
        if resolved.exists():
            if resolved.suffix in {".ts", ".tsx"}:
                raise ValueError(
                    f"TypeScript entry file detected ({resolved}). "
                    "Please compile your agent to JavaScript before running it under OmniDaemon."
                )
            return resolved

    raise ValueError(
        "Could not determine JavaScript entry file for TypeScript agent. "
        "Ensure your project has a package.json with a 'main' field or a compiled file "
        "such as dist/index.js."
    )


def _hash_files(files: list[Path]) -> str:
    hasher = hashlib.sha256()
    for file in files:
        if file.exists() and file.is_file():
            hasher.update(file.read_bytes())
    return hasher.hexdigest()


async def _run_subprocess(
    cmd: list[str],
    *,
    cwd: Optional[Path] = None,
    env: Optional[Dict[str, str]] = None,
    description: str,
) -> None:
    """
    Run a subprocess asynchronously to avoid blocking the event loop.
    """
    logger.info("Running %s: %s", description, " ".join(cmd))
    process = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        stdout_str = stdout.decode("utf-8", errors="replace") if stdout else ""
        stderr_str = stderr.decode("utf-8", errors="replace") if stderr else ""
        raise RuntimeError(
            f"{description} failed (exit code {process.returncode}).\n"
            f"STDOUT: {stdout_str}\n"
            f"STDERR: {stderr_str}"
        )
    # Log output for debugging (only if verbose)
    if stdout:
        logger.debug(
            "[%s stdout] %s",
            description,
            stdout.decode("utf-8", errors="replace").strip(),
        )
    if stderr:
        logger.debug(
            "[%s stderr] %s",
            description,
            stderr.decode("utf-8", errors="replace").strip(),
        )


async def _ensure_python_dependencies(agent_path: Path) -> Dict[str, str]:
    """
    Install Python dependencies from requirements.txt OR pyproject.toml (not both).

    FLOW:
    1. Check if agent has requirements.txt OR pyproject.toml (error if both exist)
    2. Hash the manifest file to check if dependencies are already installed
    3. If hash matches existing .omnidaemon_pydeps/.hash, reuse cached dependencies
    4. Otherwise, install dependencies to agent_path/.omnidaemon_pydeps/
    5. Return PYTHONPATH env vars that include:
       - .omnidaemon_pydeps/ (for dependencies)
       - agent_path/ (for agent's own code)

    IMPORTANT:
    - We ONLY install dependencies, NOT the agent code itself
    - Agent code stays in agent_path/ and is imported via PYTHONPATH
    - .omnidaemon_pydeps/ is a cache - if manifest hasn't changed, we reuse it
    - Each agent has its own isolated .omnidaemon_pydeps/ directory
    - We ignore uv.lock - only use pyproject.toml or requirements.txt
    """
    requirements = agent_path / "requirements.txt"
    pyproject = agent_path / "pyproject.toml"

    # Check what files actually exist (for debugging)
    has_requirements = requirements.exists()
    has_pyproject = pyproject.exists()

    logger.debug(
        f"Checking dependency manifests in {agent_path}: "
        f"requirements.txt={has_requirements}, pyproject.toml={has_pyproject}"
    )

    # Strict standard: Only one dependency manifest per agent
    # Raise error if both exist to avoid confusion and conflicts
    if has_requirements and has_pyproject:
        raise ValueError(
            f"Agent directory {agent_path} has both requirements.txt and pyproject.toml. "
            "OmniDaemon requires only ONE dependency manifest per agent. "
            "Please choose one:\n"
            "  - Use requirements.txt for simple dependency lists\n"
            "  - Use pyproject.toml for modern Python projects with metadata\n"
            f"Remove one of these files: {requirements} or {pyproject}"
        )

    # Determine which manifest to use
    if pyproject.exists():
        manifest_file = pyproject
    elif requirements.exists():
        manifest_file = requirements
    else:
        manifest_file = None

    if not manifest_file:
        # No dependency manifest - just return PYTHONPATH with agent directory
        logger.info(
            "No dependency manifest found in %s - assuming no dependencies needed",
            agent_path,
        )
        return _python_env_vars(agent_path, None)

    install_dir = agent_path / ".omnidaemon_pydeps"
    stamp_file = install_dir / ".hash"
    hash_value = _hash_files([manifest_file])

    if install_dir.exists() and stamp_file.exists():
        try:
            if stamp_file.read_text().strip() == hash_value:
                logger.info(
                    "Python dependencies already installed for %s (hash match)",
                    agent_path,
                )
                return _python_env_vars(agent_path, install_dir)
        except OSError:
            pass

    if install_dir.exists():
        shutil.rmtree(install_dir)
    install_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()

    if requirements.exists():
        # Install from requirements.txt
        logger.info("Installing dependencies from requirements.txt for %s", agent_path)
        cmd = [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "--target",
            str(install_dir),
            "-r",
            str(requirements),
        ]
        await _run_subprocess(
            cmd, cwd=agent_path, env=env, description="pip install requirements"
        )
    elif pyproject.exists():
        # Install dependencies from pyproject.toml
        # Strategy: Extract dependencies from pyproject.toml and install them directly
        # We do NOT install the agent package itself - only its dependencies
        logger.info("Installing dependencies from pyproject.toml for %s", agent_path)

        # Parse pyproject.toml to extract dependencies
        try:
            # Try tomllib (Python 3.11+)
            try:
                import tomllib

                with (agent_path / "pyproject.toml").open("rb") as f:
                    pyproject_data = tomllib.load(f)
            except ImportError:
                # Fallback to tomli for older Python
                try:
                    import tomli

                    with (agent_path / "pyproject.toml").open("rb") as f:
                        pyproject_data = tomli.load(f)
                except ImportError:
                    raise RuntimeError(
                        "Cannot parse pyproject.toml: tomllib (Python 3.11+) or tomli package required"
                    )

            # Extract dependencies from project.dependencies
            project_data = pyproject_data.get("project", {})
            dependencies = project_data.get("dependencies", [])

            if not dependencies:
                logger.warning(
                    "No dependencies found in pyproject.toml for %s. "
                    "If dependencies are defined elsewhere, please use requirements.txt instead.",
                    agent_path,
                )
                # No dependencies to install
            else:
                # Install all dependencies in one command
                cmd = [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "--upgrade",
                    "--target",
                    str(install_dir),
                ] + dependencies
                await _run_subprocess(
                    cmd,
                    cwd=agent_path,
                    env=env,
                    description="pip install dependencies from pyproject.toml",
                )
        except Exception as e:
            raise RuntimeError(
                f"Failed to install dependencies from pyproject.toml for {agent_path}: {e}"
            ) from e

    stamp_file.write_text(hash_value)
    return _python_env_vars(
        agent_path,
        install_dir if install_dir.exists() and any(install_dir.iterdir()) else None,
    )


def _python_env_vars(agent_path: Path, deps_dir: Optional[Path]) -> Dict[str, str]:
    """
    Build PYTHONPATH that includes:
    1. Agent's own directory (so agent code can be imported)
    2. Dependencies directory (if installed)
    3. Existing PYTHONPATH from environment
    """
    paths = [str(agent_path)]
    if deps_dir:
        paths.insert(0, str(deps_dir))

    existing_path = os.environ.get("PYTHONPATH", "")
    if existing_path:
        paths.append(existing_path)

    return {
        "PYTHONPATH": os.pathsep.join(paths),
        "PYTHONNOUSERSITE": "1",
    }


def _detect_node_package_manager(agent_path: Path) -> Optional[list[str]]:
    if (agent_path / "package-lock.json").exists():
        return ["npm", "ci"]
    if (agent_path / "yarn.lock").exists():
        return ["yarn", "install", "--frozen-lockfile"]
    if (agent_path / "pnpm-lock.yaml").exists():
        return ["pnpm", "install", "--frozen-lockfile"]
    if (agent_path / "package.json").exists():
        return ["npm", "install"]
    return None


async def _ensure_node_dependencies(agent_path: Path) -> Dict[str, Any]:
    package_json = agent_path / "package.json"
    if not package_json.exists():
        return {"package_json": None}

    with package_json.open("r") as fh:
        package_data = json.load(fh)

    lock_files = [
        path
        for path in [
            agent_path / "package-lock.json",
            agent_path / "yarn.lock",
            agent_path / "pnpm-lock.yaml",
            package_json,
        ]
        if path.exists()
    ]
    hash_value = _hash_files(lock_files)
    cache_dir = agent_path / ".omnidaemon_cache"
    cache_dir.mkdir(exist_ok=True)
    stamp_file = cache_dir / "node.hash"

    install_cmd = _detect_node_package_manager(agent_path)
    if install_cmd:
        need_install = True
        if stamp_file.exists():
            try:
                if stamp_file.read_text().strip() == hash_value:
                    need_install = False
                    logger.info(
                        "Node dependencies already installed for %s", agent_path
                    )
            except OSError:
                need_install = True
        if need_install:
            await _run_subprocess(
                install_cmd,
                cwd=agent_path,
                env=os.environ.copy(),
                description="node dependency install",
            )
            stamp_file.write_text(hash_value)

    return {"package_json": package_data}


async def _build_typescript_project(
    agent_path: Path, package_data: Optional[Dict[str, Any]]
) -> None:
    has_ts_sources = _has_typescript_sources(agent_path)
    tsconfig = agent_path / "tsconfig.json"

    if not has_ts_sources and not tsconfig.exists():
        raise ValueError(
            "TypeScript agents must include TypeScript sources (.ts/.tsx) or a tsconfig.json. "
            "Plain JavaScript projects are not supported."
        )

    scripts: Dict[str, Any] = {}
    if package_data and isinstance(package_data.get("scripts"), dict):
        scripts = package_data["scripts"]

    if "build" in scripts:
        manager_cmd = _detect_node_package_manager(agent_path)
        if manager_cmd is None:
            raise RuntimeError(
                "Cannot run build script because no package manager was detected."
            )
        runner = manager_cmd[0]
        if runner == "npm":
            cmd = ["npm", "run", "build"]
        elif runner == "yarn":
            cmd = ["yarn", "run", "build"]
        elif runner == "pnpm":
            cmd = ["pnpm", "run", "build"]
        else:
            cmd = [runner, "run", "build"]
        await _run_subprocess(
            cmd,
            cwd=agent_path,
            env=os.environ.copy(),
            description="typescript build script",
        )
        return

    cmd = ["npx", "tsc"]
    if tsconfig.exists():
        cmd.extend(["-p", str(tsconfig)])
    await _run_subprocess(
        cmd,
        cwd=agent_path,
        env=os.environ.copy(),
        description="typescript compilation",
    )


async def create_supervisor_from_directory(
    agent_name: str,
    agent_dir: str,
    callback_function: str,
    language: Optional[Literal["python", "go", "typescript"]] = None,
    request_timeout: float = 120.0,
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
        restart_on_exit: Whether to restart the agent if it exits
        max_restart_attempts: Maximum number of restart attempts
        restart_backoff_seconds: Backoff delay between restarts

    Returns:
        AgentSupervisor instance (already started)
    """
    # Check registry first - return existing if already created
    if agent_name in _supervisor_registry:
        existing = _supervisor_registry[agent_name]
        logger.debug(
            f"Supervisor '{agent_name}' already exists, returning existing instance"
        )
        return existing

    # Check for duplicate name with different directory (error case)
    for name, supervisor in _supervisor_registry.items():
        if name == agent_name:
            # This shouldn't happen due to check above, but just in case
            raise ValueError(
                f"Agent name '{agent_name}' is already registered. "
                f"Each agent must have a unique name."
            )

    # Detect language if not provided
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
    if language == "python":
        python_env = await _ensure_python_dependencies(agent_path)
        if python_env:
            env_for_process = env_for_process or {}
            env_for_process.update(python_env)

    # Build command based on language
    if language == "python":
        # Dynamically find the callback function in the directory
        # Pass python_env so callback discovery can use the correct PYTHONPATH
        module_path, func_name = _find_callback_in_directory(
            agent_dir, callback_function, language, python_env=python_env
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
            cwd=str(agent_path),  # Set working directory to agent directory
            request_timeout=request_timeout,
            restart_on_exit=restart_on_exit,
            max_restart_attempts=max_restart_attempts,
            restart_backoff_seconds=restart_backoff_seconds,
            env=env_for_process,  # Includes PYTHONPATH with dependencies
        )
    elif language == "typescript":
        if not agent_path.is_dir():
            raise ValueError(f"TypeScript agent directory does not exist: {agent_dir}")

        node_meta = await _ensure_node_dependencies(agent_path)
        await _build_typescript_project(agent_path, node_meta.get("package_json"))

        entry_file = _resolve_typescript_entry(agent_path)
        adapter_path = (
            Path(__file__).resolve().parent
            / "ts_callback_adapter"
            / "dist"
            / "index.js"
        ).resolve()

        if not adapter_path.exists():
            raise RuntimeError(
                "TypeScript adapter is missing. Make sure the project includes "
                "the compiled adapter at "
                f"{adapter_path} or run 'npm install && npm run build' inside "
                "ts_callback_adapter."
            )

        config = AgentProcessConfig(
            name=agent_name,
            command="node",
            args=[
                str(adapter_path),
                "--module",
                str(entry_file),
                "--function",
                callback_function,
            ],
            cwd=str(agent_path),
            request_timeout=request_timeout,
            restart_on_exit=restart_on_exit,
            max_restart_attempts=max_restart_attempts,
            restart_backoff_seconds=restart_backoff_seconds,
            env=env_for_process,
        )
    elif language == "go":
        # For Go agents, we need to:
        # 1. Find the main.go or callback.go file in the directory
        # 2. Build it to a binary
        # 3. Run the binary

        if not agent_path.is_dir():
            raise ValueError(f"Go agent directory does not exist: {agent_dir}")

        # Look for main.go or callback.go
        main_file = None
        for filename in ["callback.go", "main.go"]:
            candidate = agent_path / filename
            if candidate.exists():
                main_file = candidate
                break

        if main_file is None:
            raise ValueError(
                f"No main.go or callback.go found in Go agent directory: {agent_dir}"
            )

        # Check for go.mod
        go_mod = agent_path / "go.mod"
        if not go_mod.exists():
            raise ValueError(
                f"go.mod not found in Go agent directory: {agent_dir}. "
                "Go agents must have a go.mod file."
            )

        # Use hash-based caching similar to Python
        # Hash go.mod, go.sum (if exists), and all .go files
        go_files = list(agent_path.glob("*.go"))
        hash_files = [go_mod]
        go_sum = agent_path / "go.sum"
        if go_sum.exists():
            hash_files.append(go_sum)
        hash_files.extend(go_files)
        hash_value = _hash_files(hash_files)

        # Create cache directory in agent directory (like Python's .omnidaemon_pydeps)
        cache_dir = agent_path / ".omnidaemon_gocache"
        cache_dir.mkdir(exist_ok=True)
        stamp_file = cache_dir / ".hash"
        binary_path = cache_dir / "agent"

        # Check if we need to rebuild
        need_rebuild = True
        if binary_path.exists() and stamp_file.exists():
            try:
                if stamp_file.read_text().strip() == hash_value:
                    need_rebuild = False
                    logger.info(
                        "Go agent binary already built for %s (hash match)", agent_path
                    )
            except OSError:
                need_rebuild = True

        if not need_rebuild:
            logger.info(f"Using cached Go agent binary: {binary_path}")
        else:
            # Remove old binary if it exists
            if binary_path.exists():
                binary_path.unlink()

        if need_rebuild:
            logger.info(f"Building Go agent from {main_file} to {binary_path}")

            # First, ensure dependencies are downloaded
            logger.info("Downloading Go dependencies...")

            # Get the current environment and ensure Go environment variables are set
            # This ensures the subprocess has the same environment as the shell
            env = os.environ.copy()
            if env_overrides:
                env.update(env_overrides)

            # Ensure PATH includes Go binary location
            # Try to find go binary and add its directory to PATH if not already there
            go_binary_path = shutil.which("go")
            if go_binary_path:
                go_bin_dir = str(Path(go_binary_path).parent)
                current_path = env.get("PATH", "")
                if go_bin_dir not in current_path:
                    env["PATH"] = f"{go_bin_dir}:{current_path}"
                    logger.debug(f"Added {go_bin_dir} to PATH")

            # Get GOROOT and GOPATH from go env if not set
            try:
                go_env_process = await asyncio.create_subprocess_exec(
                    "go",
                    "env",
                    "GOROOT",
                    "GOPATH",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env,
                )
                stdout, stderr = await go_env_process.communicate()
                if go_env_process.returncode == 0:
                    lines = stdout.decode("utf-8", errors="replace").strip().split("\n")
                    if len(lines) >= 1 and lines[0]:
                        env["GOROOT"] = lines[0]
                    if len(lines) >= 2 and lines[1]:
                        env["GOPATH"] = lines[1]
            except Exception as e:
                logger.warning(f"Could not get Go environment: {e}")

            # Run go mod tidy (non-blocking)
            tidy_process = await asyncio.create_subprocess_exec(
                "go",
                "mod",
                "tidy",
                cwd=str(agent_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            _, stderr = await tidy_process.communicate()
            if tidy_process.returncode != 0:
                stderr_str = stderr.decode("utf-8", errors="replace") if stderr else ""
                logger.warning(f"go mod tidy had issues: {stderr_str}")

            # Build the package (not a single file)
            # Use "." to build the current package - this is the correct way
            # Disable VCS stamping to avoid Git issues in Docker
            build_cmd = [
                "go",
                "build",
                "-buildvcs=false",  # Disable VCS stamping (avoids Git errors in Docker)
                "-o",
                str(binary_path),
                ".",  # Build the current package
            ]

            logger.info(f"Running: {' '.join(build_cmd)} in {agent_path}")
            logger.debug(
                f"GOROOT: {env.get('GOROOT', 'not set')}, GOPATH: {env.get('GOPATH', 'not set')}"
            )

            # Run build in the agent directory (for go.mod resolution) - async
            build_process = await asyncio.create_subprocess_exec(
                *build_cmd,
                cwd=str(agent_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            stdout, stderr = await build_process.communicate()
            stdout_str = stdout.decode("utf-8", errors="replace") if stdout else ""
            stderr_str = stderr.decode("utf-8", errors="replace") if stderr else ""

            if build_process.returncode != 0:
                # Check if this is a Go installation issue
                stderr_lower = stderr_str.lower()
                is_go_install_issue = (
                    "runtime" in stderr_lower and "redeclared" in stderr_lower
                ) or "/usr/local/go/src/runtime" in stderr_str

                if is_go_install_issue:
                    # Get GOROOT for error message (async)
                    try:
                        go_root_process = await asyncio.create_subprocess_exec(
                            "go",
                            "env",
                            "GOROOT",
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                            env=env,
                        )
                        go_root_stdout, _ = await go_root_process.communicate()
                        goroot_path = (
                            go_root_stdout.decode("utf-8", errors="replace").strip()
                            if go_root_stdout
                            else "unknown"
                        )
                    except Exception:
                        goroot_path = "unknown"

                    error_msg = (
                        f"Failed to build Go agent due to Go installation issue:\n"
                        f"Your Go installation at {goroot_path} appears to be corrupted.\n"
                        f"Error: {stderr_str[:500]}...\n\n"
                        f"To fix this, try:\n"
                        f"1. Reinstall Go: https://go.dev/doc/install\n"
                        f"2. Or use a Go version manager like gvm or asdf\n"
                        f"3. Check for multiple Go installations conflicting\n"
                        f"\nCommand: {' '.join(build_cmd)}\n"
                        f"Working directory: {agent_path}"
                    )
                else:
                    error_msg = (
                        f"Failed to build Go agent:\n"
                        f"STDOUT: {stdout_str}\n"
                        f"STDERR: {stderr_str}\n"
                        f"Command: {' '.join(build_cmd)}\n"
                        f"Working directory: {agent_path}\n"
                        f"Main file: {main_file}"
                    )
                logger.error(error_msg)
                raise RuntimeError(error_msg)

            # Save hash after successful build
            stamp_file.write_text(hash_value)
            logger.info(f"Go agent built successfully: {binary_path}")

        # Create supervisor config to run the binary
        config = AgentProcessConfig(
            name=agent_name,
            command=str(binary_path),
            args=[],  # Binary is self-contained
            request_timeout=request_timeout,
            restart_on_exit=restart_on_exit,
            max_restart_attempts=max_restart_attempts,
            restart_backoff_seconds=restart_backoff_seconds,
            env=env_for_process,
        )
    else:
        raise ValueError(f"Unsupported language: {language}")

    supervisor = AgentSupervisor(config)
    await supervisor.start()
    _supervisor_registry[agent_name] = supervisor
    logger.info(
        f"Supervisor created for '{agent_name}' "
        f"(dir: {agent_dir}, callback: {callback_function})"
    )
    return supervisor


async def shutdown_all_supervisors() -> None:
    """Shutdown all registered supervisors."""
    for name, supervisor in _supervisor_registry.items():
        try:
            await supervisor.stop()
            logger.info(f"Supervisor '{name}' shutdown complete")
        except Exception as exc:
            logger.error(
                f"Error shutting down supervisor '{name}': {exc}", exc_info=True
            )
    _supervisor_registry.clear()
