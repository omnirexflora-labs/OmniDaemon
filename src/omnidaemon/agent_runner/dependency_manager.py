"""
Dependency Manager for Python Agent Isolation.

This module handles Python dependency installation for OmniDaemon agents,
ensuring each agent has its own isolated dependency environment.

Key features:
- Per-agent dependency isolation using `.omnidaemon_pydeps/` directory
- Support for both requirements.txt and pyproject.toml
- Dependency caching with hash-based validation
- Automatic PYTHONPATH configuration
"""

from typing import Dict, Optional
from pathlib import Path
import os
import sys
import shutil
import logging
from .utils import _run_subprocess, _hash_files

logger = logging.getLogger(__name__)


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
       - agent_path/ (for agent code)

    IMPORTANT:
    - We ONLY install dependencies, NOT the agent code itself
    - Agent code stays in agent_path/ and is imported via PYTHONPATH
    - .omnidaemon_pydeps/ is a cache - if manifest hasn't changed, we reuse it
    - Each agent has its own isolated .omnidaemon_pydeps/ directory
    - We ignore uv.lock - only use pyproject.toml or requirements.txt
    """
    requirements = agent_path / "requirements.txt"
    pyproject = agent_path / "pyproject.toml"

    has_requirements = requirements.exists()
    has_pyproject = pyproject.exists()

    logger.debug(
        f"Checking dependency manifests in {agent_path}: "
        f"requirements.txt={has_requirements}, pyproject.toml={has_pyproject}"
    )

    if has_requirements and has_pyproject:
        raise ValueError(
            f"Agent directory {agent_path} has both requirements.txt and pyproject.toml. "
            "OmniDaemon requires only ONE dependency manifest per agent. "
            "Please choose one:\n"
            "  - Use requirements.txt for simple dependency lists\n"
            "  - Use pyproject.toml for modern Python projects with metadata\n"
            f"Remove one of these files: {requirements} or {pyproject}"
        )

    if pyproject.exists():
        manifest_file = pyproject
    elif requirements.exists():
        manifest_file = requirements
    else:
        manifest_file = None

    if not manifest_file:
        raise ValueError(
            f"No dependency manifest (pyproject.toml or requirements.txt) found in {agent_path}.\n"
            "OmniDaemon requires strict agent isolation: every agent must define its own dependencies.\n"
            f"Action: Please create a 'pyproject.toml' or 'requirements.txt' inside '{agent_path}'."
        )

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
        logger.info("Installing dependencies from pyproject.toml for %s", agent_path)

        try:
            try:
                import tomllib

                with (agent_path / "pyproject.toml").open("rb") as f:
                    pyproject_data = tomllib.load(f)
            except ImportError:
                try:
                    import tomli

                    with (agent_path / "pyproject.toml").open("rb") as f:
                        pyproject_data = tomli.load(f)
                except ImportError:
                    raise RuntimeError(
                        "Cannot parse pyproject.toml: tomllib (Python 3.11+) or tomli package required"
                    )

            project_data = pyproject_data.get("project", {})
            dependencies = project_data.get("dependencies", [])

            if not dependencies:
                logger.warning(
                    "No dependencies found in pyproject.toml for %s. "
                    "If dependencies are defined elsewhere, please use requirements.txt instead.",
                    agent_path,
                )
            else:
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
