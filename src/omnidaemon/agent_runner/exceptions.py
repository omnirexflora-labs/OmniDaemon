"""Custom exceptions for Agent Supervisor with remediation guidance.

This module provides a hierarchy of exceptions that include:
- Clear error messages
- Actionable remediation steps
- Context for debugging
"""

from typing import Optional


class AgentSupervisorError(Exception):
    """Base exception for all Agent Supervisor errors.

    All supervisor exceptions include remediation guidance to help
    users quickly resolve issues.
    """

    def __init__(self, message: str, remediation: str = ""):
        self.message = message
        self.remediation = remediation

        if remediation:
            full_message = f"{message}\n\n💡 To fix: {remediation}"
        else:
            full_message = message

        super().__init__(full_message)


class AgentStartupError(AgentSupervisorError):
    """Agent failed to start.

    Common causes:
    - Command not found
    - Permission denied
    - Missing dependencies
    - Invalid configuration
    """

    def __init__(
        self,
        agent_name: str,
        command: str,
        exit_code: Optional[int] = None,
        error: str = "",
    ):
        message = f"Agent '{agent_name}' failed to start"

        if exit_code is not None:
            message += f" (exit code: {exit_code})"
        if error:
            message += f": {error}"

        if "command not found" in error.lower() or "No such file" in error:
            remediation = f"Verify command '{command}' exists and is in PATH. Check agent configuration."
        elif "permission denied" in error.lower():
            remediation = (
                f"Ensure '{command}' has execute permissions: chmod +x {command}"
            )
        elif exit_code == 1:
            remediation = "Check agent logs for import errors or missing dependencies. Verify requirements.txt installed."
        else:
            remediation = f"Check agent logs and verify '{command}' runs successfully outside supervisor."

        super().__init__(message, remediation)


class AgentCallbackNotFoundError(AgentSupervisorError):
    """Callback function not found in agent code.

    The specified callback function doesn't exist or isn't callable.
    """

    def __init__(
        self,
        callback_name: str,
        module_path: str,
        available_functions: Optional[list] = None,
    ):
        message = f"Callback function '{callback_name}' not found in {module_path}"

        if available_functions:
            remediation = f"Available functions: {', '.join(available_functions)}. Check spelling or use one of these."
        else:
            remediation = (
                f"Ensure '{callback_name}' is defined and callable in {module_path}"
            )

        super().__init__(message, remediation)


class AgentDependencyError(AgentSupervisorError):
    """Agent dependencies failed to install or import.

    Common causes:
    - requirements.txt has invalid syntax
    - Package version conflict
    - Network issues during pip install
    - Missing system dependencies
    """

    def __init__(self, agent_dir: str, error: str):
        message = f"Dependency error for agent in {agent_dir}: {error}"

        if "requirement" in error.lower():
            remediation = "Check requirements.txt syntax. Ensure all package names and versions are valid."
        elif (
            "could not find" in error.lower()
            or "no matching distribution" in error.lower()
        ):
            remediation = (
                "Verify package exists on PyPI. Check for typos in package names."
            )
        elif "conflicting" in error.lower():
            remediation = "Resolve version conflicts in requirements.txt. Use compatible package versions."
        else:
            remediation = (
                "Manually test: cd {agent_dir} && pip install -r requirements.txt. "
                "Check for network connectivity and PyPI availability."
            )

        super().__init__(message, remediation)


class AgentTimeoutError(AgentSupervisorError):
    """Agent request timed out.

    The agent didn't respond within the configured timeout period.
    """

    def __init__(self, agent_name: str, request_id: str, timeout: float):
        message = (
            f"Agent '{agent_name}' timeout after {timeout}s (request: {request_id})"
        )
        remediation = (
            "1. Check if agent is processing long-running tasks. Increase request_timeout if needed.\n"
            "2. Verify agent is responsive: check heartbeat logs.\n"
            "3. Check agent logs for blocking operations or deadlocks."
        )
        super().__init__(message, remediation)


class AgentCrashError(AgentSupervisorError):
    """Agent process crashed unexpectedly.

    The agent exited with a non-zero exit code.
    """

    def __init__(
        self, agent_name: str, exit_code: int, restart_attempts: int, max_attempts: int
    ):
        message = f"Agent '{agent_name}' crashed (exit: {exit_code}, restarts: {restart_attempts}/{max_attempts})"

        if restart_attempts >= max_attempts:
            remediation = (
                "Max restart attempts reached. Agent has systematic issue:\n"
                "1. Check agent startup logs for errors\n"
                "2. Verify dependencies installed correctly\n"
                "3. Test agent standalone: python callback.py\n"
                "4. Check for infinite crash loops"
            )
        else:
            remediation = (
                f"Agent will auto-restart (attempt {restart_attempts + 1}/{max_attempts}). "
                "Check logs to identify crash cause."
            )

        super().__init__(message, remediation)


class AgentConfigurationError(AgentSupervisorError):
    """Invalid agent configuration.

    The provided configuration is invalid or incomplete.
    """

    def __init__(self, message: str, config_key: Optional[str] = None):
        if config_key:
            remediation = f"Check configuration for '{config_key}'. Refer to AgentProcessConfig documentation."
        else:
            remediation = (
                "Verify all required configuration fields are provided and valid types."
            )

        super().__init__(message, remediation)


class AgentLanguageNotSupportedError(AgentSupervisorError):
    """Language not supported by agent runner.

    Currently only Python is supported.
    """

    def __init__(self, language: str, agent_dir: str):
        message = f"Language '{language}' not supported for agent in {agent_dir}"
        remediation = (
            "Currently supported languages: Python\n"
            "Ensure agent directory contains:\n"
            "- Python: __init__.py or .py files\n"
            "Future: TypeScript/JavaScript, Go support coming soon"
        )
        super().__init__(message, remediation)


class AgentStorageError(AgentSupervisorError):
    """Storage backend error (non-critical).

    Storage operation failed but supervisor continues operating.
    """

    def __init__(self, operation: str, error: str):
        message = f"Storage {operation} failed: {error}"
        remediation = (
            "Supervisor will continue without storage. To fix:\n"
            "1. Verify storage backend is running and accessible\n"
            "2. Check storage configuration and credentials\n"
            "3. Review storage backend logs for errors"
        )
        super().__init__(message, remediation)
