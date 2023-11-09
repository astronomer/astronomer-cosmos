from pathlib import Path
from cosmos.utils import create_symlinks

DBT_PROJECTS_ROOT_DIR = Path(__file__).parent.parent / "dev/dags/dbt"


def test_create_symlinks(tmp_path):
    """Tests that symlinks are created for expected files in the dbt project directory."""
    tmp_dir = tmp_path / "dbt-project"
    tmp_dir.mkdir()

    create_symlinks(DBT_PROJECTS_ROOT_DIR / "jaffle_shop", tmp_dir)
    for child in tmp_dir.iterdir():
        assert child.is_symlink()
        assert child.name not in ("logs", "target", "profiles.yml", "dbt_packages")
