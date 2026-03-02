"""
This test suite focuses on the source pruning logic that removes sources
without downstream dependencies from the rendered DAG.
"""

from datetime import datetime
from pathlib import Path

from airflow.models import DAG

from cosmos.airflow.graph import (
    _is_source_used_by_filtered_nodes,
    create_task_metadata,
    generate_task_or_group,
)
from cosmos.config import ProfileConfig, RenderConfig
from cosmos.constants import (
    DbtResourceType,
    ExecutionMode,
    SourceRenderingBehavior,
    TestBehavior,
    TestIndirectSelection,
)
from cosmos.dbt.graph import DbtNode
from cosmos.profiles import PostgresUserPasswordProfileMapping


class TestSourcePruningLogic:
    """Test the core source pruning logic."""

    def setup_method(self):
        """Set up test data for each test method."""
        # Create a graph structure for testing:
        # source_used -> model1 -> model2
        # source_orphaned (no downstream dependencies)
        # source_with_multiple_deps -> model3
        #                           -> model4

        self.source_used = DbtNode(
            unique_id="source.test_project.source_used",
            resource_type=DbtResourceType.SOURCE,
            depends_on=[],
            file_path=Path("/test/source_used.yml"),
            package_name="test_project",
        )

        self.source_orphaned = DbtNode(
            unique_id="source.test_project.source_orphaned",
            resource_type=DbtResourceType.SOURCE,
            depends_on=[],
            file_path=Path("/test/source_orphaned.yml"),
            package_name="test_project",
        )

        self.source_with_multiple_deps = DbtNode(
            unique_id="source.test_project.source_multiple",
            resource_type=DbtResourceType.SOURCE,
            depends_on=[],
            file_path=Path("/test/source_multiple.yml"),
            package_name="test_project",
        )

        self.model1 = DbtNode(
            unique_id="model.test_project.model1",
            resource_type=DbtResourceType.MODEL,
            depends_on=["source.test_project.source_used"],
            file_path=Path("/test/model1.sql"),
            package_name="test_project",
        )

        self.model2 = DbtNode(
            unique_id="model.test_project.model2",
            resource_type=DbtResourceType.MODEL,
            depends_on=["model.test_project.model1"],
            file_path=Path("/test/model2.sql"),
            package_name="test_project",
        )

        self.model3 = DbtNode(
            unique_id="model.test_project.model3",
            resource_type=DbtResourceType.MODEL,
            depends_on=["source.test_project.source_multiple"],
            file_path=Path("/test/model3.sql"),
            package_name="test_project",
        )

        self.model4 = DbtNode(
            unique_id="model.test_project.model4",
            resource_type=DbtResourceType.MODEL,
            depends_on=["source.test_project.source_multiple"],
            file_path=Path("/test/model4.sql"),
            package_name="test_project",
        )

    def test_is_source_used_by_filtered_nodes_basic(self):
        """Test the basic functionality of _is_source_used_by_filtered_nodes."""
        # Test case 1: Source is used by a filtered node
        filtered_nodes = {
            self.model1.unique_id: self.model1,
            self.model2.unique_id: self.model2,
        }

        assert _is_source_used_by_filtered_nodes(self.source_used, filtered_nodes) is True
        assert _is_source_used_by_filtered_nodes(self.source_orphaned, filtered_nodes) is False

        # Test case 2: Source is used by multiple filtered nodes
        filtered_nodes_multiple = {
            self.model3.unique_id: self.model3,
            self.model4.unique_id: self.model4,
        }

        assert _is_source_used_by_filtered_nodes(self.source_with_multiple_deps, filtered_nodes_multiple) is True

    def test_is_source_used_by_filtered_nodes_empty_filtered_nodes(self):
        """Test behavior with empty filtered_nodes."""
        empty_filtered_nodes = {}

        # No filtered nodes means no source should be considered "used"
        assert _is_source_used_by_filtered_nodes(self.source_used, empty_filtered_nodes) is False
        assert _is_source_used_by_filtered_nodes(self.source_orphaned, empty_filtered_nodes) is False

    def test_is_source_used_by_filtered_nodes_no_dependencies(self):
        """Test with filtered nodes that have no dependencies."""
        # Create a model with no dependencies
        independent_model = DbtNode(
            unique_id="model.test_project.independent",
            resource_type=DbtResourceType.MODEL,
            depends_on=[],  # No dependencies
            file_path=Path("/test/independent.sql"),
            package_name="test_project",
        )

        filtered_nodes = {
            independent_model.unique_id: independent_model,
        }

        # Sources should not be considered "used" if no models depend on them
        assert _is_source_used_by_filtered_nodes(self.source_used, filtered_nodes) is False
        assert _is_source_used_by_filtered_nodes(self.source_orphaned, filtered_nodes) is False

    def test_is_source_used_by_filtered_nodes_mixed_dependencies(self):
        """Test with filtered nodes that have mixed dependency types."""
        # Create a model that depends on another model AND a source
        complex_model = DbtNode(
            unique_id="model.test_project.complex",
            resource_type=DbtResourceType.MODEL,
            depends_on=[
                "model.test_project.model1",  # Model dependency
                "source.test_project.source_used",  # Source dependency
            ],
            file_path=Path("/test/complex.sql"),
            package_name="test_project",
        )

        filtered_nodes = {
            self.model1.unique_id: self.model1,
            complex_model.unique_id: complex_model,
        }

        # Source should be considered "used" even when model has mixed dependencies
        assert _is_source_used_by_filtered_nodes(self.source_used, filtered_nodes) is True
        assert _is_source_used_by_filtered_nodes(self.source_orphaned, filtered_nodes) is False


class TestSourcePruningIntegration:
    """Test source pruning integration with create_task_metadata and generate_task_or_group."""

    def setup_method(self):
        """Set up test data for integration tests."""
        self.dag = DAG("test_source_pruning", start_date=datetime(2023, 1, 1))

        self.profile_config = ProfileConfig(
            profile_name="test",
            target_name="test",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="fake_conn",
                profile_args={"schema": "public"},
            ),
        )

        self.task_args = {
            "project_dir": Path("/test/project"),
            "profile_config": self.profile_config,
        }

        # Create test nodes
        self.source_with_downstream = DbtNode(
            unique_id="source.test_project.source_with_downstream",
            resource_type=DbtResourceType.SOURCE,
            depends_on=[],
            file_path=Path("/test/source_with_downstream.yml"),
            package_name="test_project",
            has_freshness=True,  # Make it renderable
        )

        self.source_orphaned = DbtNode(
            unique_id="source.test_project.source_orphaned",
            resource_type=DbtResourceType.SOURCE,
            depends_on=[],
            file_path=Path("/test/source_orphaned.yml"),
            package_name="test_project",
            has_freshness=True,  # Make it renderable
        )

        self.model_using_source = DbtNode(
            unique_id="model.test_project.model_using_source",
            resource_type=DbtResourceType.MODEL,
            depends_on=["source.test_project.source_with_downstream"],
            file_path=Path("/test/model_using_source.sql"),
            package_name="test_project",
        )

        # Filtered nodes represent what will actually be rendered
        self.filtered_nodes_with_model = {
            self.model_using_source.unique_id: self.model_using_source,
        }

        self.filtered_nodes_empty = {}

    def test_create_task_metadata_source_pruning_disabled(self):
        """Test create_task_metadata when source_pruning is disabled."""
        # Source should be rendered regardless of downstream dependencies
        metadata_with_downstream = create_task_metadata(
            node=self.source_with_downstream,
            execution_mode=ExecutionMode.LOCAL,
            args=self.task_args,
            dbt_dag_task_group_identifier="tes  t",
            render_config=RenderConfig(
                source_rendering_behavior=SourceRenderingBehavior.ALL,
                source_pruning=False,  # Disabled
            ),
            filtered_nodes=self.filtered_nodes_with_model,
        )

        metadata_orphaned = create_task_metadata(
            node=self.source_orphaned,
            execution_mode=ExecutionMode.LOCAL,
            args=self.task_args,
            dbt_dag_task_group_identifier="test",
            render_config=RenderConfig(
                source_rendering_behavior=SourceRenderingBehavior.ALL,
                source_pruning=False,  # Disabled
            ),
            filtered_nodes=self.filtered_nodes_empty,
        )

        # Both sources should generate metadata when pruning is disabled
        assert metadata_with_downstream is not None
        assert metadata_orphaned is not None
        assert metadata_with_downstream.id == "source_with_downstream_source"
        assert metadata_orphaned.id == "source_orphaned_source"

    def test_create_task_metadata_source_pruning_enabled(self):
        """Test create_task_metadata when source_pruning is enabled."""
        # Source with downstream should be rendered
        metadata_with_downstream = create_task_metadata(
            node=self.source_with_downstream,
            execution_mode=ExecutionMode.LOCAL,
            args=self.task_args,
            dbt_dag_task_group_identifier="test",
            render_config=RenderConfig(
                source_rendering_behavior=SourceRenderingBehavior.ALL,
                source_pruning=True,  # Enabled
            ),
            filtered_nodes=self.filtered_nodes_with_model,
        )

        # Orphaned source should NOT be rendered
        metadata_orphaned = create_task_metadata(
            node=self.source_orphaned,
            execution_mode=ExecutionMode.LOCAL,
            args=self.task_args,
            dbt_dag_task_group_identifier="test",
            render_config=RenderConfig(
                source_rendering_behavior=SourceRenderingBehavior.ALL,
                source_pruning=True,  # Enabled
            ),
            filtered_nodes=self.filtered_nodes_with_model,
        )

        assert metadata_with_downstream is not None
        assert metadata_orphaned is None  # Pruned!
        assert metadata_with_downstream.id == "source_with_downstream_source"

    def test_create_task_metadata_source_pruning_edge_cases(self):
        """Test create_task_metadata edge cases with filtered_nodes."""
        # When filtered_nodes is None, pruning should be skipped (condition: source_pruning and filtered_nodes)
        metadata_none = create_task_metadata(
            node=self.source_orphaned,
            execution_mode=ExecutionMode.LOCAL,
            args=self.task_args,
            dbt_dag_task_group_identifier="test",
            render_config=RenderConfig(
                source_rendering_behavior=SourceRenderingBehavior.ALL,
                source_pruning=True,
            ),
            filtered_nodes=None,  # None means skip pruning check
        )

        # When filtered_nodes is empty, pruning should be skipped (empty dict is falsy)
        metadata_empty = create_task_metadata(
            node=self.source_orphaned,
            execution_mode=ExecutionMode.LOCAL,
            args=self.task_args,
            dbt_dag_task_group_identifier="test",
            render_config=RenderConfig(
                source_rendering_behavior=SourceRenderingBehavior.ALL,
                source_pruning=True,
            ),
            filtered_nodes={},  # Empty dict means skip pruning check
        )

        # When filtered_nodes contains nodes that don't use this source, it should be pruned
        unrelated_model = DbtNode(
            unique_id="model.test_project.unrelated",
            resource_type=DbtResourceType.MODEL,
            depends_on=["some.other.source"],  # Doesn't depend on source_orphaned
            file_path=Path("/test/unrelated.sql"),
            package_name="test_project",
        )
        metadata_unused = create_task_metadata(
            node=self.source_orphaned,
            execution_mode=ExecutionMode.LOCAL,
            args=self.task_args,
            dbt_dag_task_group_identifier="test",
            render_config=RenderConfig(
                source_rendering_behavior=SourceRenderingBehavior.ALL,
                source_pruning=True,
            ),
            filtered_nodes={unrelated_model.unique_id: unrelated_model},  # Non-empty but doesn't use this source
        )

        # Looking at the code: if source_pruning and filtered_nodes and not _is_source_used_by_filtered_nodes(...)
        # When filtered_nodes is None, the condition fails at "filtered_nodes", so no pruning occurs
        # When filtered_nodes is {}, the condition also fails at "filtered_nodes" (empty dict is falsy), so no pruning occurs
        # When filtered_nodes is non-empty but doesn't use the source, pruning occurs
        assert metadata_none is not None  # Not pruned when filtered_nodes is None
        assert metadata_empty is not None  # Not pruned when filtered_nodes is empty dict (falsy)
        assert metadata_unused is None  # Pruned when filtered_nodes doesn't use this source

    def test_generate_task_or_group_source_pruning(self):
        """Test generate_task_or_group with source pruning."""
        # Source with downstream dependencies should generate a task
        task_with_downstream = generate_task_or_group(
            dag=self.dag,
            task_group=None,
            node=self.source_with_downstream,
            execution_mode=ExecutionMode.LOCAL,
            task_args=self.task_args,
            render_config=RenderConfig(
                test_behavior=TestBehavior.NONE,
                source_rendering_behavior=SourceRenderingBehavior.ALL,
                source_pruning=True,
            ),
            test_indirect_selection=TestIndirectSelection.EAGER,
            filtered_nodes=self.filtered_nodes_with_model,
            node_converters={},
        )

        # Orphaned source should NOT generate a task
        task_orphaned = generate_task_or_group(
            dag=self.dag,
            task_group=None,
            node=self.source_orphaned,
            execution_mode=ExecutionMode.LOCAL,
            task_args=self.task_args,
            test_behavior=TestBehavior.NONE,
            render_config=RenderConfig(
                source_rendering_behavior=SourceRenderingBehavior.ALL,
                source_pruning=True,
            ),
            test_indirect_selection=TestIndirectSelection.EAGER,
            filtered_nodes=self.filtered_nodes_with_model,
            node_converters={},
        )

        assert task_with_downstream is not None
        assert task_orphaned is None

    def test_source_pruning_with_different_source_rendering_behaviors(self):
        """Test source pruning interaction with different source rendering behaviors."""
        # Create source without freshness
        source_no_freshness = DbtNode(
            unique_id="source.test_project.source_no_freshness",
            resource_type=DbtResourceType.SOURCE,
            depends_on=[],
            file_path=Path("/test/source_no_freshness.yml"),
            package_name="test_project",
            has_freshness=False,  # No freshness
        )

        # Test with SourceRenderingBehavior.NONE - should return None regardless of pruning
        metadata_none = create_task_metadata(
            node=source_no_freshness,
            execution_mode=ExecutionMode.LOCAL,
            args=self.task_args,
            dbt_dag_task_group_identifier="test",
            render_config=RenderConfig(
                source_rendering_behavior=SourceRenderingBehavior.NONE,
                source_pruning=True,
            ),
            filtered_nodes=self.filtered_nodes_with_model,
        )

        # Test with SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS - should return None for source without freshness
        metadata_conditional = create_task_metadata(
            node=source_no_freshness,
            execution_mode=ExecutionMode.LOCAL,
            args=self.task_args,
            dbt_dag_task_group_identifier="test",
            render_config=RenderConfig(
                source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS,
                source_pruning=True,
            ),
            filtered_nodes=self.filtered_nodes_with_model,
        )

        # Both should be None due to source rendering behavior, before pruning logic is even reached
        assert metadata_none is None
        assert metadata_conditional is None


class TestSourcePruningEdgeCases:
    """Test edge cases and error conditions for source pruning."""

    def test_non_source_node_with_pruning_function(self):
        """Test that _is_source_used_by_filtered_nodes works with non-source nodes."""
        # Create a model node
        model_node = DbtNode(
            unique_id="model.test_project.some_model",
            resource_type=DbtResourceType.MODEL,
            depends_on=["source.test_project.some_source"],
            file_path=Path("/test/some_model.sql"),
            package_name="test_project",
        )

        # Create a filtered node that depends on the model
        dependent_model = DbtNode(
            unique_id="model.test_project.dependent_model",
            resource_type=DbtResourceType.MODEL,
            depends_on=["model.test_project.some_model"],
            file_path=Path("/test/dependent_model.sql"),
            package_name="test_project",
        )

        filtered_nodes = {
            dependent_model.unique_id: dependent_model,
        }

        # Function should work with any node type, not just sources
        assert _is_source_used_by_filtered_nodes(model_node, filtered_nodes) is True

    def test_source_pruning_with_complex_dependencies(self):
        """Test source pruning with complex dependency chains."""
        # Create a complex dependency chain
        source = DbtNode(
            unique_id="source.test_project.raw_data",
            resource_type=DbtResourceType.SOURCE,
            depends_on=[],
            file_path=Path("/test/raw_data.yml"),
            package_name="test_project",
            has_freshness=True,
        )

        staging_model = DbtNode(
            unique_id="model.test_project.stg_data",
            resource_type=DbtResourceType.MODEL,
            depends_on=["source.test_project.raw_data"],
            file_path=Path("/test/stg_data.sql"),
            package_name="test_project",
        )

        mart_model = DbtNode(
            unique_id="model.test_project.mart_data",
            resource_type=DbtResourceType.MODEL,
            depends_on=["model.test_project.stg_data"],
            file_path=Path("/test/mart_data.sql"),
            package_name="test_project",
        )

        # Test case 1: Only the mart model is in filtered_nodes
        # Source should still be considered "unused" because the direct dependency (staging_model) is not included
        filtered_nodes_mart_only = {
            mart_model.unique_id: mart_model,
        }

        assert _is_source_used_by_filtered_nodes(source, filtered_nodes_mart_only) is False

        # Test case 2: Both staging and mart models are in filtered_nodes
        # Source should be considered "used" because staging model directly depends on it
        filtered_nodes_both = {
            staging_model.unique_id: staging_model,
            mart_model.unique_id: mart_model,
        }

        assert _is_source_used_by_filtered_nodes(source, filtered_nodes_both) is True

    def test_source_pruning_parameter_defaults(self):
        """Test that source_pruning parameter defaults work correctly."""
        # Create minimal test data
        source = DbtNode(
            unique_id="source.test_project.test_source",
            resource_type=DbtResourceType.SOURCE,
            depends_on=[],
            file_path=Path("/test/test_source.yml"),
            package_name="test_project",
            has_freshness=True,
        )

        dag = DAG("test", start_date=datetime(2023, 1, 1))
        task_args = {
            "project_dir": Path("/test"),
            "profile_config": ProfileConfig(
                profile_name="test",
                target_name="test",
                profile_mapping=PostgresUserPasswordProfileMapping(
                    conn_id="fake_conn",
                    profile_args={"schema": "public"},
                ),
            ),
        }

        # Test that generate_task_or_group works without explicitly passing source_pruning
        # (should default to False)
        task = generate_task_or_group(
            dag=dag,
            task_group=None,
            node=source,
            execution_mode=ExecutionMode.LOCAL,
            task_args=task_args,
            render_config=RenderConfig(
                test_behavior=TestBehavior.NONE,
                source_rendering_behavior=SourceRenderingBehavior.ALL,
                # source_pruning not specified - should default to False
            ),
            test_indirect_selection=TestIndirectSelection.EAGER,
            # filtered_nodes not specified - should default to None
            node_converters={},
        )

        # Should create a task because pruning is disabled by default
        assert task is not None
