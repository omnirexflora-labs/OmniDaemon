"""Unit tests for agent_supervisor_runner."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

from omnidaemon.agent_runner.agent_supervisor_runner import (
    create_supervisor_from_directory,
    shutdown_all_supervisors,
    _supervisor_registry,
)
from omnidaemon.agent_runner.dependency_manager import _ensure_python_dependencies
from omnidaemon.agent_runner.agent_supervisor import AgentSupervisor


@pytest.fixture(autouse=True)
def clear_supervisor_registry():
    """Clear the global supervisor registry before each test."""
    _supervisor_registry.clear()
    yield
    _supervisor_registry.clear()


class TestCreateSupervisorFromDirectory:
    """Test suite for create_supervisor_from_directory factory function."""

    @pytest.mark.asyncio
    async def test_create_supervisor_python_basic(self):
        """Test creating supervisor for Python agent."""
        with tempfile.TemporaryDirectory() as tmpdir:
            agent_path = Path(tmpdir)
            callback_file = agent_path / "callback.py"
            callback_file.write_text("async def my_callback(msg):\n    return msg\n")

            with (
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._ensure_python_dependencies"
                ) as mock_deps,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._find_callback_in_directory"
                ) as mock_find,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner.get_supervisor_state",
                    new_callable=AsyncMock,
                ) as mock_get_state,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner.register_supervisor_in_storage",
                    new_callable=AsyncMock,
                ),
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._detect_language",
                    return_value="python",
                ),
                patch.object(AgentSupervisor, "start") as mock_start,
            ):
                mock_deps.return_value = {}
                mock_find.return_value = ("test.module", "my_callback")
                mock_get_state.return_value = None

                supervisor = await create_supervisor_from_directory(
                    agent_name="test-agent",
                    agent_dir=tmpdir,
                    callback_function="my_callback",
                )

                assert supervisor is not None
                assert isinstance(supervisor, AgentSupervisor)
                assert supervisor.config.name == "test-agent"
                mock_start.assert_called_once()
                assert "test-agent" in _supervisor_registry

                mock_find.assert_called_with(
                    tmpdir, "my_callback", "python", extra_paths=[]
                )

    @pytest.mark.asyncio
    async def test_create_supervisor_returns_existing(self):
        """Test that creating same supervisor twice returns existing instance."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with (
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._ensure_python_dependencies"
                ) as mock_deps,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._find_callback_in_directory"
                ) as mock_find,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner.get_supervisor_state",
                    new_callable=AsyncMock,
                ) as mock_get_state,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner.register_supervisor_in_storage",
                    new_callable=AsyncMock,
                ),
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._detect_language",
                    return_value="python",
                ),
                patch.object(AgentSupervisor, "start") as mock_start,
            ):
                mock_deps.return_value = {}
                mock_find.return_value = ("test.module", "my_callback")
                mock_get_state.return_value = None

                supervisor1 = await create_supervisor_from_directory(
                    agent_name="test-agent",
                    agent_dir=tmpdir,
                    callback_function="my_callback",
                )

                supervisor2 = await create_supervisor_from_directory(
                    agent_name="test-agent",
                    agent_dir=tmpdir,
                    callback_function="my_callback",
                )

                assert supervisor1 is supervisor2
                assert mock_start.call_count == 1

    @pytest.mark.asyncio
    async def test_create_supervisor_with_custom_config(self):
        """Test creating supervisor with custom configuration."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with (
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._ensure_python_dependencies"
                ) as mock_deps,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._find_callback_in_directory"
                ) as mock_find,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner.get_supervisor_state",
                    new_callable=AsyncMock,
                ) as mock_get_state,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner.register_supervisor_in_storage",
                    new_callable=AsyncMock,
                ),
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._detect_language",
                    return_value="python",
                ),
                patch.object(AgentSupervisor, "start"),
            ):
                mock_deps.return_value = {}
                mock_find.return_value = ("test.module", "my_callback")
                mock_get_state.return_value = None

                supervisor = await create_supervisor_from_directory(
                    agent_name="custom-agent",
                    agent_dir=tmpdir,
                    callback_function="my_callback",
                    request_timeout=30.0,
                    restart_on_exit=False,
                    max_restart_attempts=10,
                    restart_backoff_seconds=1.0,
                )

                assert supervisor.config.request_timeout == 30.0
                assert supervisor.config.restart_on_exit is False
                assert supervisor.config.max_restart_attempts == 10
                assert supervisor.config.restart_backoff_seconds == 1.0

    @pytest.mark.asyncio
    async def test_create_supervisor_with_store(self):
        """Test creating supervisor with storage backend."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mock_store = AsyncMock()

            with (
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._ensure_python_dependencies"
                ) as mock_deps,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._find_callback_in_directory"
                ) as mock_find,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner.get_supervisor_state",
                    new_callable=AsyncMock,
                ) as mock_get_state,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner.register_supervisor_in_storage",
                    new_callable=AsyncMock,
                ),
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._detect_language",
                    return_value="python",
                ),
                patch.object(AgentSupervisor, "start"),
            ):
                mock_deps.return_value = {}
                mock_find.return_value = ("test.module", "my_callback")
                mock_get_state.return_value = None

                with patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner.default_store",
                    mock_store,
                ):
                    supervisor = await create_supervisor_from_directory(
                        agent_name="store-agent",
                        agent_dir=tmpdir,
                        callback_function="my_callback",
                    )

                assert supervisor.store is mock_store

    @pytest.mark.asyncio
    async def test_create_supervisor_auto_detect_language(self):
        """Test language auto-detection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            agent_path = Path(tmpdir)
            (agent_path / "__init__.py").touch()

            with (
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._ensure_python_dependencies"
                ) as mock_deps,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._find_callback_in_directory"
                ) as mock_find,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner.get_supervisor_state",
                    new_callable=AsyncMock,
                ) as mock_get_state,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner.register_supervisor_in_storage",
                    new_callable=AsyncMock,
                ),
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._detect_language",
                    return_value="python",
                ),
                patch.object(AgentSupervisor, "start"),
            ):
                mock_deps.return_value = {}
                mock_find.return_value = ("test.module", "my_callback")
                mock_get_state.return_value = None

                supervisor = await create_supervisor_from_directory(
                    agent_name="auto-detect",
                    agent_dir=tmpdir,
                    callback_function="my_callback",
                    language=None,
                )

                assert supervisor is not None

    @pytest.mark.asyncio
    async def test_create_supervisor_unsupported_language(self):
        """Test error for unsupported language."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="Unsupported language"):
                with (
                    patch(
                        "omnidaemon.agent_runner.agent_supervisor_runner._detect_language"
                    ) as mock_detect,
                    patch(
                        "omnidaemon.agent_runner.agent_supervisor_runner.get_supervisor_state",
                        new_callable=AsyncMock,
                    ) as mock_get_state,
                ):
                    mock_detect.return_value = "golang"
                    mock_get_state.return_value = None

                    await create_supervisor_from_directory(
                        agent_name="bad-lang",
                        agent_dir=tmpdir,
                        callback_function="callback",
                    )

    @pytest.mark.asyncio
    async def test_create_supervisor_loads_env_files(self):
        """Test that environment files are loaded."""
        with tempfile.TemporaryDirectory() as tmpdir:
            agent_path = Path(tmpdir)
            env_file = agent_path / ".env"
            env_file.write_text("TEST_VAR=test_value\n")

            with (
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._ensure_python_dependencies"
                ) as mock_deps,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._find_callback_in_directory"
                ) as mock_find,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner.get_supervisor_state",
                    new_callable=AsyncMock,
                ) as mock_get_state,
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner.register_supervisor_in_storage",
                    new_callable=AsyncMock,
                ),
                patch(
                    "omnidaemon.agent_runner.agent_supervisor_runner._detect_language",
                    return_value="python",
                ),
                patch.object(AgentSupervisor, "start"),
            ):
                mock_deps.return_value = {}
                mock_find.return_value = ("test.module", "my_callback")
                mock_get_state.return_value = None

                supervisor = await create_supervisor_from_directory(
                    agent_name="env-agent",
                    agent_dir=tmpdir,
                    callback_function="my_callback",
                )

                assert supervisor.config.env is not None


class TestShutdownAllSupervisors:
    """Test suite for shutdown_all_supervisors function."""

    @pytest.mark.asyncio
    async def test_shutdown_all_supervisors_empty(self):
        """Test shutting down when no supervisors exist."""
        await shutdown_all_supervisors()

    @pytest.mark.asyncio
    async def test_shutdown_all_supervisors_single(self):
        """Test shutting down single supervisor."""
        mock_supervisor = AsyncMock()
        _supervisor_registry["test-agent"] = mock_supervisor

        with patch(
            "omnidaemon.agent_runner.agent_supervisor_runner.unregister_supervisor_from_storage",
            new_callable=AsyncMock,
        ) as mock_unregister:
            await shutdown_all_supervisors()

            mock_supervisor.stop.assert_called_once()
            mock_unregister.assert_called_once_with("test-agent")
            assert len(_supervisor_registry) == 0

    @pytest.mark.asyncio
    async def test_shutdown_all_supervisors_multiple(self):
        """Test shutting down multiple supervisors."""
        mock_super1 = AsyncMock()
        mock_super2 = AsyncMock()
        mock_super3 = AsyncMock()

        _supervisor_registry["agent1"] = mock_super1
        _supervisor_registry["agent2"] = mock_super2
        _supervisor_registry["agent3"] = mock_super3

        with patch(
            "omnidaemon.agent_runner.agent_supervisor_runner.unregister_supervisor_from_storage",
            new_callable=AsyncMock,
        ) as mock_unregister:
            await shutdown_all_supervisors()

            mock_super1.stop.assert_called_once()
            mock_super2.stop.assert_called_once()
            mock_super3.stop.assert_called_once()
            assert mock_unregister.call_count == 3
            assert len(_supervisor_registry) == 0

    @pytest.mark.asyncio
    async def test_shutdown_all_supervisors_with_error(self):
        """Test shutdown continues even if one supervisor fails."""
        mock_super1 = AsyncMock()
        mock_super2 = AsyncMock()

        async def raise_error():
            raise Exception("Shutdown error")

        mock_super2.stop.side_effect = raise_error
        mock_super3 = AsyncMock()

        _supervisor_registry["agent1"] = mock_super1
        _supervisor_registry["agent2"] = mock_super2
        _supervisor_registry["agent3"] = mock_super3

        with patch(
            "omnidaemon.agent_runner.agent_supervisor_runner.unregister_supervisor_from_storage",
            new_callable=AsyncMock,
        ) as mock_unregister:
            await shutdown_all_supervisors()

            mock_super1.stop.assert_called_once()
            mock_super2.stop.assert_called_once()
            mock_super3.stop.assert_called_once()
            assert mock_unregister.call_count == 2
            assert len(_supervisor_registry) == 0


class TestEnsurePythonDependencies:
    """Test suite for _ensure_python_dependencies function."""

    @pytest.mark.asyncio
    async def test_ensure_deps_no_manifest(self):
        """Test when no dependency manifest exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            agent_path = Path(tmpdir)

            with pytest.raises(ValueError, match="No dependency manifest"):
                await _ensure_python_dependencies(agent_path)

    @pytest.mark.asyncio
    async def test_ensure_deps_both_manifests_error(self):
        """Test error when both requirements.txt and pyproject.toml exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            agent_path = Path(tmpdir)
            (agent_path / "requirements.txt").touch()
            (agent_path / "pyproject.toml").touch()

            with pytest.raises(
                ValueError, match="has both requirements.txt and pyproject.toml"
            ):
                await _ensure_python_dependencies(agent_path)

    @pytest.mark.asyncio
    async def test_ensure_deps_requirements_txt(self):
        """Test installing from requirements.txt."""
        with tempfile.TemporaryDirectory() as tmpdir:
            agent_path = Path(tmpdir)
            req_file = agent_path / "requirements.txt"
            req_file.write_text("requests==2.28.0\n")

            with patch(
                "omnidaemon.agent_runner.dependency_manager._run_subprocess"
            ) as mock_run:
                env = await _ensure_python_dependencies(agent_path)

                mock_run.assert_called()
                assert "PYTHONPATH" in env

    @pytest.mark.asyncio
    async def test_ensure_deps_pyproject_toml(self):
        """Test installing from pyproject.toml."""
        with tempfile.TemporaryDirectory() as tmpdir:
            agent_path = Path(tmpdir)
            pyproject_file = agent_path / "pyproject.toml"
            pyproject_file.write_text(
                "[project]\nname = 'test'\ndependencies = ['requests']\n"
            )

            with patch(
                "omnidaemon.agent_runner.dependency_manager._run_subprocess"
            ) as mock_run:
                env = await _ensure_python_dependencies(agent_path)

                mock_run.assert_called()
                assert "PYTHONPATH" in env

    @pytest.mark.asyncio
    async def test_ensure_deps_cache_reuse(self):
        """Test that cached dependencies are reused when hash matches."""
        with tempfile.TemporaryDirectory() as tmpdir:
            agent_path = Path(tmpdir)
            req_file = agent_path / "requirements.txt"
            req_file.write_text("requests==2.28.0\n")

            install_dir = agent_path / ".omnidaemon_pydeps"
            install_dir.mkdir()

            from omnidaemon.agent_runner.utils import _hash_files

            hash_value = _hash_files([req_file])
            stamp_file = install_dir / ".hash"
            stamp_file.write_text(hash_value)

            with patch(
                "omnidaemon.agent_runner.dependency_manager._run_subprocess"
            ) as mock_run:
                env = await _ensure_python_dependencies(agent_path)

                mock_run.assert_not_called()
                assert "PYTHONPATH" in env
                assert str(install_dir) in env["PYTHONPATH"]

    @pytest.mark.asyncio
    async def test_ensure_deps_cache_invalidation(self):
        """Test that cached dependencies are reinstalled when hash changes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            agent_path = Path(tmpdir)
            req_file = agent_path / "requirements.txt"
            req_file.write_text("requests==2.28.0\n")

            install_dir = agent_path / ".omnidaemon_pydeps"
            install_dir.mkdir()
            stamp_file = install_dir / ".hash"
            stamp_file.write_text("wrong_hash_value")

            with patch(
                "omnidaemon.agent_runner.dependency_manager._run_subprocess"
            ) as mock_run:
                env = await _ensure_python_dependencies(agent_path)

                mock_run.assert_called()
                assert "PYTHONPATH" in env
