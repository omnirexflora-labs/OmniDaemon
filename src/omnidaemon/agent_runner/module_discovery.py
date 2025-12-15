"""
Module Discovery and Dynamic Import for OmniDaemon Agents.

This module provides utilities for dynamically importing Python modules and
discovering callback functions for OmniDaemon agents. It handles:
- sys.path manipulation for agent code discovery
- Dynamic module imports from file paths
- Callback function discovery and validation
- Agent directory dependency path setup
"""

from typing import Literal, Tuple, Optional
from pathlib import Path
import importlib.util
import os
import sys
import logging

logger = logging.getLogger(__name__)


def _detect_language(agent_path: str) -> Literal["python"]:
    """
    Detect the language/runtime based on directory structure.
    """
    path = Path(agent_path).resolve()

    if not path.is_dir():
        raise ValueError(
            f"Agent path must be a directory containing the callback and dependencies: {agent_path}"
        )

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
    if not Path(agent_dir).is_absolute():
        path = (Path.cwd() / agent_dir).resolve()
    else:
        path = Path(agent_dir).resolve()

    cwd = Path.cwd()

    try:
        rel_path = path.relative_to(cwd)
        module_path = str(rel_path).replace(os.sep, ".").replace("/", ".")
        module_path = module_path.lstrip(".")
        return module_path
    except ValueError:
        for sys_path in sys.path:
            try:
                rel_path = path.relative_to(Path(sys_path).resolve())
                module_path = str(rel_path).replace(os.sep, ".").replace("/", ".")
                module_path = module_path.lstrip(".")
                if module_path:
                    return module_path
            except (ValueError, OSError):
                continue
        return str(path).replace(os.sep, ".").replace("/", ".")


def _find_callback_in_directory(
    dir_path: str,
    callback_name: str,
    language: str,
    extra_paths: Optional[list[str]] = None,
) -> Tuple[str, str]:
    """
    Find the module and function name for the callback.

    Args:
        dir_path: Directory to search in
        callback_name: Name of the function to find
        language: Language of the agent
        extra_paths: Optional list of paths to add to sys.path (for dependencies)

    Returns:
        Tuple of (module_path, function_name)
    """
    if language == "python":
        if extra_paths:
            for path in extra_paths:
                if path not in sys.path:
                    sys.path.insert(0, path)
                    logger.debug(f"Added dependency path to sys.path: {path}")

        project_root = Path.cwd()
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
            logger.debug(f"Added project root to sys.path: {project_root}")

        agent_path = Path(dir_path)
        if not agent_path.is_absolute():
            agent_path = Path.cwd() / agent_path
        agent_path = agent_path.resolve()

        if not agent_path.is_dir():
            raise ValueError(
                f"Agent directory does not exist: {agent_path} (resolved to: {agent_path})"
            )

        logger.info(
            f"Looking for callback '{callback_name}' in {agent_path}/callback.py"
        )

        callback_file = agent_path / "callback.py"
        if not callback_file.exists():
            raise ValueError(
                f"callback.py not found in agent directory: {agent_path}\n"
                f"Expected file: {callback_file}"
            )

        project_root = Path.cwd()
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
            logger.debug(f"Added project root to sys.path: {project_root}")

        try:
            rel_path = callback_file.relative_to(project_root)
            file_module = (
                str(rel_path.with_suffix("")).replace(os.sep, ".").replace("/", ".")
            )
            file_module = file_module.lstrip(".")
        except ValueError:
            agent_module_base = _directory_to_module_path(str(agent_path))
            file_module = f"{agent_module_base}.callback"

        logger.debug(f"Importing module: {file_module}")

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
            # Re-raising with context for easier debugging
            raise ValueError(
                f"Error importing {file_module}: {e}\nFile: {callback_file}"
            ) from e
    else:
        raise ValueError(
            f"Language '{language}' not yet supported for dynamic callback discovery"
        )
