from typing import Dict
from pathlib import Path
import hashlib
import logging
import asyncio
from typing import Optional


logger = logging.getLogger(__name__)


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


def _hash_files(files: list[Path]) -> str:
    hasher = hashlib.sha256()
    for file in files:
        if file.exists() and file.is_file():
            hasher.update(file.read_bytes())
    return hasher.hexdigest()
