"""Unit tests for agent_runner utils."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from omnidaemon.agent_runner.utils import (
    _run_subprocess,
    _load_env_files,
    _hash_files,
)
from omnidaemon.agent_runner.module_discovery import (
    _detect_language,
    _directory_to_module_path,
    _find_callback_in_directory,
)
from omnidaemon.agent_runner.dependency_manager import (
    _python_env_vars,
)


class TestRunSubprocess:
    """Test suite for _run_subprocess utility."""

    @pytest.mark.asyncio
    async def test_run_subprocess_success(self):
        """Test successful subprocess execution."""
        await _run_subprocess(["echo", "test"], description="test command")

    @pytest.mark.asyncio
    async def test_run_subprocess_with_cwd(self):
        """Test subprocess with custom working directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            await _run_subprocess(["pwd"], cwd=Path(tmpdir), description="pwd test")

    @pytest.mark.asyncio
    async def test_run_subprocess_failure(self):
        """Test subprocess that fails."""
        with pytest.raises(RuntimeError, match="failed"):
            await _run_subprocess(
                ["false"],
                description="failing command",
            )


class TestLoadEnvFiles:
    """Test suite for _load_env_files utility."""

    def test_load_env_files_single_file(self):
        """Test loading from single env file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            f.write("KEY1=value1\n")
            f.write("KEY2=value2\n")
            f.flush()

            env = _load_env_files(Path(f.name))

            assert env["KEY1"] == "value1"
            assert env["KEY2"] == "value2"

            Path(f.name).unlink()

    def test_load_env_files_with_quotes(self):
        """Test loading env values with quotes."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            f.write('KEY1="quoted value"\n')
            f.write("KEY2='single quoted'\n")
            f.flush()

            env = _load_env_files(Path(f.name))

            assert env["KEY1"] == "quoted value"
            assert env["KEY2"] == "single quoted"

            Path(f.name).unlink()

    def test_load_env_files_ignores_comments(self):
        """Test that comments are ignored."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            f.write("# This is a comment\n")
            f.write("KEY1=value1\n")
            f.write("  # Another comment\n")
            f.flush()

            env = _load_env_files(Path(f.name))

            assert "KEY1" in env
            assert len(env) == 1

            Path(f.name).unlink()

    def test_load_env_files_ignores_blank_lines(self):
        """Test that blank lines are ignored."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            f.write("\n")
            f.write("KEY1=value1\n")
            f.write("   \n")
            f.write("KEY2=value2\n")
            f.flush()

            env = _load_env_files(Path(f.name))

            assert len(env) == 2

            Path(f.name).unlink()

    def test_load_env_files_multiple_files_override(self):
        """Test that later files override earlier ones."""
        with (
            tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f1,
            tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f2,
        ):
            f1.write("KEY1=first\n")
            f1.write("KEY2=keepthis\n")
            f1.flush()

            f2.write("KEY1=second\n")
            f2.flush()

            env = _load_env_files(Path(f1.name), Path(f2.name))

            assert env["KEY1"] == "second"
            assert env["KEY2"] == "keepthis"

            Path(f1.name).unlink()
            Path(f2.name).unlink()

    def test_load_env_files_nonexistent_file(self):
        """Test loading nonexistent file returns empty dict."""
        env = _load_env_files(Path("/nonexistent/file.env"))
        assert env == {}


class TestDetectLanguage:
    """Test suite for _detect_language utility."""

    def test_detect_language_python_with_init(self):
        """Test detecting Python with __init__.py."""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "__init__.py").touch()

            lang = _detect_language(tmpdir)
            assert lang == "python"

    def test_detect_language_python_with_py_files(self):
        """Test detecting Python with .py files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "script.py").touch()

            lang = _detect_language(tmpdir)
            assert lang == "python"

    def test_detect_language_not_a_directory(self):
        """Test error when path is not a directory."""
        with tempfile.NamedTemporaryFile() as f:
            with pytest.raises(ValueError, match="must be a directory"):
                _detect_language(f.name)

    def test_detect_language_no_recognizable_files(self):
        """Test error when no language can be detected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="Unable to detect"):
                _detect_language(tmpdir)


class TestHashFiles:
    """Test suite for _hash_files utility."""

    def test_hash_files_single_file(self):
        """Test hashing single file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("content")
            f.flush()

            hash1 = _hash_files([Path(f.name)])
            hash2 = _hash_files([Path(f.name)])

            assert hash1 == hash2
            assert len(hash1) == 64

            Path(f.name).unlink()

    def test_hash_files_different_content(self):
        """Test different content produces different hashes."""
        with (
            tempfile.NamedTemporaryFile(mode="w", delete=False) as f1,
            tempfile.NamedTemporaryFile(mode="w", delete=False) as f2,
        ):
            f1.write("content1")
            f1.flush()
            f2.write("content2")
            f2.flush()

            hash1 = _hash_files([Path(f1.name)])
            hash2 = _hash_files([Path(f2.name)])

            assert hash1 != hash2

            Path(f1.name).unlink()
            Path(f2.name).unlink()

    def test_hash_files_nonexistent_file(self):
        """Test hashing nonexistent file is skipped."""
        hash_result = _hash_files([Path("/nonexistent/file.txt")])
        assert len(hash_result) == 64


class TestDirectoryToModulePath:
    """Test suite for _directory_to_module_path utility."""

    def test_directory_to_module_path_relative(self):
        """Test converting relative directory to module path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_dir = Path(tmpdir) / "mypackage" / "subpackage"
            test_dir.mkdir(parents=True)

            result = _directory_to_module_path(str(test_dir))
            assert isinstance(result, str)
            assert "mypackage" in result or "subpackage" in result

    def test_directory_to_module_path_absolute(self):
        """Test converting absolute directory to module path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = _directory_to_module_path(tmpdir)
            assert isinstance(result, str)


class TestPythonEnvVars:
    """Test suite for _python_env_vars utility."""

    def test_python_env_vars_basic(self):
        """Test basic Python env vars."""
        agent_path = Path("/path/to/agent")
        env = _python_env_vars(agent_path, None)

        assert "PYTHONPATH" in env
        assert str(agent_path) in env["PYTHONPATH"]
        assert env["PYTHONNOUSERSITE"] == "1"

    def test_python_env_vars_with_deps(self):
        """Test Python env vars with dependencies directory."""
        agent_path = Path("/path/to/agent")
        deps_path = Path("/path/to/deps")

        env = _python_env_vars(agent_path, deps_path)

        assert str(deps_path) in env["PYTHONPATH"]
        assert str(agent_path) in env["PYTHONPATH"]
        assert env["PYTHONPATH"].index(str(deps_path)) < env["PYTHONPATH"].index(
            str(agent_path)
        )

    def test_python_env_vars_preserves_existing(self):
        """Test that existing PYTHONPATH is preserved."""
        agent_path = Path("/path/to/agent")

        with patch.dict("os.environ", {"PYTHONPATH": "/existing/path"}):
            env = _python_env_vars(agent_path, None)

            assert "/existing/path" in env["PYTHONPATH"]
            assert str(agent_path) in env["PYTHONPATH"]


class TestFindCallbackInDirectory:
    """Test suite for _find_callback_in_directory utility."""

    def test_find_callback_python_success(self):
        """Test finding callback in Python directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            agent_dir = Path(tmpdir)
            (agent_dir / "callback.py").touch()

            mock_module = MagicMock()
            mock_func = MagicMock()
            setattr(mock_module, "test_callback", mock_func)

            with patch(
                "importlib.import_module", return_value=mock_module
            ) as mock_import:
                module_name, func_name = _find_callback_in_directory(
                    tmpdir, "test_callback", "python"
                )

                assert func_name == "test_callback"
                mock_import.assert_called_once()

    def test_find_callback_missing_file(self):
        """Test error when callback.py doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="callback.py not found"):
                _find_callback_in_directory(tmpdir, "test_callback", "python")

    def test_find_callback_not_directory(self):
        """Test error when path is not a directory."""
        with tempfile.NamedTemporaryFile() as f:
            with pytest.raises(ValueError, match="does not exist"):
                _find_callback_in_directory(f.name, "test_callback", "python")

    def test_find_callback_function_not_found(self):
        """Test error when function doesn't exist in callback.py."""
        with tempfile.TemporaryDirectory() as tmpdir:
            agent_dir = Path(tmpdir)
            (agent_dir / "callback.py").touch()

            mock_module = MagicMock()
            del mock_module.missing_func

            with patch("importlib.import_module", return_value=mock_module):
                with pytest.raises(
                    ValueError, match="Callback function 'missing_func' not found"
                ):
                    _find_callback_in_directory(tmpdir, "missing_func", "python")

    def test_find_callback_not_callable(self):
        """Test error when attribute is not callable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            agent_dir = Path(tmpdir)
            (agent_dir / "callback.py").touch()

            mock_module = MagicMock()
            mock_module.not_a_func = "string value"

            with patch("importlib.import_module", return_value=mock_module):
                with pytest.raises(ValueError, match="is not callable"):
                    _find_callback_in_directory(tmpdir, "not_a_func", "python")

    def test_find_callback_unsupported_language(self):
        """Test error for unsupported language."""
        with pytest.raises(ValueError, match="not yet supported"):
            _find_callback_in_directory("/some/path", "callback", "golang")
