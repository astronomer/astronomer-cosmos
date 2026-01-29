from unittest.mock import MagicMock, patch

from cosmos.dbt.executable import get_system_dbt, is_dbt_installed_in_same_environment


class TestGetSystemDbt:
    @patch("shutil.which")
    def test_get_system_dbt_returns_path_when_found(self, mock_which):
        """Test that get_system_dbt returns the path when dbt is found."""
        mock_which.return_value = "/usr/local/bin/dbt"
        result = get_system_dbt()
        assert result == "/usr/local/bin/dbt"
        mock_which.assert_called_once_with("dbt")

    @patch("shutil.which")
    def test_get_system_dbt_returns_dbt_when_not_found(self, mock_which):
        """Test that get_system_dbt returns 'dbt' when dbt is not found."""
        mock_which.return_value = None
        result = get_system_dbt()
        assert result == "dbt"
        mock_which.assert_called_once_with("dbt")


class TestIsDbtInstalledInSameEnvironment:
    @patch("cosmos.dbt.executable.find_spec")
    def test_is_dbt_installed_in_same_environment_returns_true_when_dbt_found(self, mock_find_spec):
        """Test that is_dbt_installed_in_same_environment returns True when find_spec finds dbt."""
        mock_find_spec.return_value = MagicMock()  # Simulates dbt module spec found
        result = is_dbt_installed_in_same_environment()
        assert result is True
        mock_find_spec.assert_called_once_with("dbt")

    @patch("cosmos.dbt.executable.find_spec")
    def test_is_dbt_installed_in_same_environment_returns_false_when_import_error(self, mock_find_spec):
        """Test that is_dbt_installed_in_same_environment returns False when find_spec raises ImportError."""
        mock_find_spec.side_effect = ImportError("No module named 'dbt'")
        result = is_dbt_installed_in_same_environment()
        assert result is False
        mock_find_spec.assert_called_once_with("dbt")
