import hashlib
import json
import logging
from pathlib import Path
from unittest.mock import MagicMock

from cosmos import settings
from cosmos.versioning import _create_folder_version_hash


def _write_file(root: Path, relative_path: str, content: bytes = b"select 1") -> Path:
    file_path = root / relative_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(content)
    return file_path


def _build_dbt_project(root: Path) -> None:
    _write_file(root, "dbt_project.yml", b"name: my_project")
    _write_file(root, "models/stg_orders.sql", b"select * from orders")
    _write_file(root, "models/stg_customers.sql", b"select * from customers")
    _write_file(root, "macros/quote.sql", b"{% macro quote() %}{% endmacro %}")


def _build_generated_dirs(root: Path) -> None:
    _write_file(root, "target/manifest.json", b"{}")
    _write_file(root, "target/compiled/model.sql", b"select 1")
    _write_file(root, "dbt_packages/dbt_utils/package.sql", b"select 2")
    _write_file(root, "logs/dbt.log", b"some log line")
    _write_file(root, ".git/refs/heads/main", b"abc123")


def test__create_folder_version_hash(tmp_path, caplog):
    """
    Test that Cosmos is still able to create the hash of a dbt project folder even when
    there is a symbolic link referencing a no longer existing file.

    This test addresses the issue:
    https://github.com/astronomer/astronomer-cosmos/issues/1096
    """
    caplog.set_level(logging.INFO)

    # Create a source folder with two files
    source_dir = tmp_path / "original_dbt_folder"
    source_dir.mkdir()
    file_1 = Path(source_dir / "file_1.sql")
    file_1.touch()
    file_2 = Path(source_dir / "file_2.sql")
    file_2.touch()

    # Create a target folder with symbolic links to the two files in the source folder
    target_dir = tmp_path / "cosmos_dbt_folder"
    target_dir.mkdir()
    file_1_symlink = Path(target_dir / "file_1.sql")
    file_1_symlink.symlink_to(file_1)
    file_2_symlink = Path(target_dir / "file_2.sql")
    file_2_symlink.symlink_to(file_2)

    # Delete one of the original files from the source folder
    file_1.unlink()

    _create_folder_version_hash(target_dir)


def test__create_folder_version_hash_excludes_generated_dirs(tmp_path):
    """Generated dbt/VCS folders should not contribute to the hash (see issue #2857)."""
    with_extras = tmp_path / "project_with_generated"
    _build_dbt_project(with_extras)
    _build_generated_dirs(with_extras)

    without_extras = tmp_path / "project_without_generated"
    _build_dbt_project(without_extras)

    assert _create_folder_version_hash(with_extras) == _create_folder_version_hash(without_extras)


def test__create_folder_version_hash_ignores_changes_in_excluded_dirs(tmp_path):
    project_dir = tmp_path / "project"
    _build_dbt_project(project_dir)
    _build_generated_dirs(project_dir)

    initial_hash = _create_folder_version_hash(project_dir)

    _write_file(project_dir, "target/manifest.json", b'{"nodes": {}}')
    _write_file(project_dir, "logs/dbt.log", b"another log line")
    _write_file(project_dir, "dbt_packages/dbt_utils/other.sql", b"select 3")

    assert _create_folder_version_hash(project_dir) == initial_hash


def test__create_folder_version_hash_detects_source_changes(tmp_path):
    project_dir = tmp_path / "project"
    _build_dbt_project(project_dir)
    _build_generated_dirs(project_dir)

    initial_hash = _create_folder_version_hash(project_dir)

    _write_file(project_dir, "models/stg_orders.sql", b"select * from orders_v2")

    assert _create_folder_version_hash(project_dir) != initial_hash


def test__create_folder_version_hash_detects_renames(tmp_path):
    """dbt derives node names from file names, so a content-preserving rename must change the hash."""
    project_dir = tmp_path / "project"
    _build_dbt_project(project_dir)

    initial_hash = _create_folder_version_hash(project_dir)

    (project_dir / "models/stg_orders.sql").rename(project_dir / "models/stg_orders_renamed.sql")

    assert _create_folder_version_hash(project_dir) != initial_hash


def test__create_folder_version_hash_is_deterministic(tmp_path):
    project_dir = tmp_path / "project"
    _build_dbt_project(project_dir)
    _build_generated_dirs(project_dir)

    first_hash = _create_folder_version_hash(project_dir)
    for _ in range(3):
        assert _create_folder_version_hash(project_dir) == first_hash


def test__create_folder_version_hash_prunes_nested_excluded_dirs(tmp_path):
    """Directories with an excluded name are pruned anywhere in the tree, not only at the root."""
    project_dir = tmp_path / "project"
    _build_dbt_project(project_dir)
    _write_file(project_dir, "models/intermediate/target/scratch.sql", b"select 4")

    hash_with_nested_target = _create_folder_version_hash(project_dir)

    (project_dir / "models/intermediate/target/scratch.sql").unlink()
    (project_dir / "models/intermediate/target").rmdir()

    assert hash_with_nested_target == _create_folder_version_hash(project_dir)


def test__create_folder_version_hash_custom_excluded_dirs(tmp_path):
    project_dir = tmp_path / "project"
    _write_file(project_dir, "models/stg_orders.sql", b"select * from orders")

    initial_hash = _create_folder_version_hash(project_dir)

    _write_file(project_dir, "macros/other.sql", b"{% macro other() %}{% endmacro %}")

    assert _create_folder_version_hash(project_dir, excluded_dirs={"macros"}) == initial_hash
    assert _create_folder_version_hash(project_dir, excluded_dirs=set()) != initial_hash


def test__create_folder_version_hash_uses_settings_default(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    _write_file(project_dir, "models/stg_orders.sql", b"select * from orders")

    initial_hash = _create_folder_version_hash(project_dir)
    _write_file(project_dir, "macros/other.sql", b"{% macro other() %}{% endmacro %}")

    monkeypatch.setattr(settings, "project_hash_excluded_dirs", frozenset({"macros"}))

    assert _create_folder_version_hash(project_dir) == initial_hash


def test__create_folder_version_hash_reads_large_files_in_chunks(tmp_path):
    """Files larger than the read chunk size must hash to the same value as a single-shot read."""
    project_dir = tmp_path / "project"
    payload = b"select 1\n" * 300_000  # ~2.7MB, larger than the 1MB read chunk
    _write_file(project_dir, "models/big_model.sql", payload)

    expected = hashlib.md5(b"models/big_model.sql\x00" + payload).hexdigest()

    assert _create_folder_version_hash(project_dir) == expected


def test__create_folder_version_hash_no_path_content_boundary_ambiguity(tmp_path):
    """path='a',content='bc' and path='ab',content='c' must hash differently despite equal concatenation."""
    project_a = tmp_path / "project_a"
    _write_file(project_a, "a", b"bc")

    project_b = tmp_path / "project_b"
    _write_file(project_b, "ab", b"c")

    assert _create_folder_version_hash(project_a) != _create_folder_version_hash(project_b)


def test__create_folder_version_hash_folds_in_manifest_path(tmp_path):
    """manifest_path need not live under dir_path at all (e.g. target/ is pruned from the walk, or the
    manifest is deployed standalone), and its content must still affect the result."""
    project_dir = tmp_path / "project"
    _write_file(project_dir, "models/stg_orders.sql", b"select * from orders")

    manifest_path = tmp_path / "elsewhere" / "manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps({"nodes": {"model.a": {}}}))

    without_manifest = _create_folder_version_hash(project_dir)
    with_manifest_v1 = _create_folder_version_hash(project_dir, manifest_path=manifest_path)

    manifest_path.write_text(json.dumps({"nodes": {"model.a": {}, "model.b": {}}}))
    with_manifest_v2 = _create_folder_version_hash(project_dir, manifest_path=manifest_path)

    assert without_manifest != with_manifest_v1
    assert with_manifest_v1 != with_manifest_v2


def test__create_folder_version_hash_manifest_ignores_volatile_metadata(tmp_path):
    """generated_at/invocation_id/invocation_started_at change on every dbt invocation even when
    the project doesn't, so two otherwise-identical manifests must hash the same despite them."""
    project_dir = tmp_path / "project"
    _write_file(project_dir, "models/stg_orders.sql", b"select * from orders")

    manifest_a = tmp_path / "a.json"
    manifest_a.write_text(
        json.dumps(
            {
                "metadata": {
                    "generated_at": "2026-01-01T00:00:00Z",
                    "invocation_id": "aaaa",
                    "invocation_started_at": "2026-01-01T00:00:00Z",
                },
                "nodes": {"model.a": {}},
            }
        )
    )

    manifest_b = tmp_path / "b.json"
    manifest_b.write_text(
        json.dumps(
            {
                "metadata": {
                    "generated_at": "2026-01-02T00:00:00Z",
                    "invocation_id": "bbbb",
                    "invocation_started_at": "2026-01-02T00:00:00Z",
                },
                "nodes": {"model.a": {}},
            }
        )
    )

    assert _create_folder_version_hash(project_dir, manifest_path=manifest_a) == _create_folder_version_hash(
        project_dir, manifest_path=manifest_b
    )


def test__create_folder_version_hash_survives_manifest_read_failure(tmp_path):
    """A manifest read/parse failure must not drop the folder hash that was already computed."""
    project_dir = tmp_path / "project"
    _write_file(project_dir, "models/stg_orders.sql", b"select * from orders")

    broken_manifest_path = MagicMock(open=MagicMock(side_effect=OSError("boom")))

    assert _create_folder_version_hash(project_dir, manifest_path=broken_manifest_path) == _create_folder_version_hash(
        project_dir
    )
