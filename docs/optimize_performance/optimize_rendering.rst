.. _optimize_rendering:

Optimize rendering
===================

When you approach improving the rendering of your Cosmos implementation, you should address the following questions:

- How do you parse the dbt project?
- How do you select a subset of the original dbt project?
- How are tests represented?

Parse the dbt project
~~~~~~~~~~~~~~~~~~~~~

Pre-compile your dbt project
----------------------------

Pre-compiling your project can help the Airflow Dag processor save time running your Dags.

.. code-block:: python

    DbtDag(
        project_config=ProjectConfig(
            manifest_path="/path/to/manifest.json"),
        render_config=RenderConfig(
                load_method=LoadMode.DBT_MANIFEST))

Make dbt available in the Airflow scheduler
-------------------------------------------

Cosmos can use the ``dbt ls`` command to identify the pipeline topology when the Airflow scheduler can access your dbt project. The output of the ``dbt ls`` command is cached and refreshed automatically. You can also purge it manually.

Use built-in project parser
---------------------------

Cosmos has a built-in dbt project parser, which it uses to process the dbt project when the dbt project is not accessible to the Airflow scheduler or possilbe to pre-compile.

Select a subset of nodes
~~~~~~~~~~~~~~~~~~~~~~~~~

Choose whether or not to use dbt ls
------------------------------------

If you use ``dbt ls`` to parse your project, you can use any selector flag availabe in the version of dbt: ``select``, ``exclude``, or ``selector``.

.. code-block:: python

    DbtDag(
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            selector="my_selector"
            )
        )

If you do not use ``dbt ls`` to parse your project, Cosmos uses a custom implementation of dbt selectors to ``exclude`` and ``select`` nodes.

For example, for using ``exclude``:

.. code-block:: python

    DbtDag(
        render_config=RenderConfig(
            exclude=["node_name+"],  # node and its children
        )
    )

You can use the following for selecting:

- ``tags``
- ``paths``
- ``config.materialized``
- ``graph operators``
- ``tags intersections``

For example, the followingexample:

.. code-block:: python

    DbtDag(
        render_config=RenderConfig( # intersection
            select=["tag:include_tag1,tag:include_tag2"]
        )
    )

Representing test nodes
~~~~~~~~~~~~~~~~~~~~~~~

The default behavior for Cosmos allows you to run tests that relate to a specific model, snapshot, or seed together with the specific dbt node that they relate to.

Decide whether you want to hide dbt tests
-----------------------------------------

You can choose whether or not to render tests by using the parameter ``TestBehavior.NONE``.

.. code-block:: python

    DbtDag(
        render_config=RenderConfig(
            test_behavior=TestBehavior.NONE,
        )
    )

Decide if you want to run all dbt tests
----------------------------------------

You can run all dbt tests by the end of the Dag run by using ``Test.Behavior.After_All``.

.. code-block:: python

    DbtDag(
        render_config=RenderConfig(
            test_behavior=TestBehavior.AFTER_ALL,
        )
    )

Render dbt within DbtTaskGroup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can choose how to render dbt by using the ``DbtTaskGroup``, which allows you to mix dbt and non-dbt task in the same Dag.

.. code-block:: python

    @dag(
        schedule_interval="@daily",
        start_date=datetime(2023, 1, 1),
        catchup=False,
    )
    def basic_cosmos_task_group() -> None:
        pre_dbt = EmptyOperator(task_id="pre_dbt")

        customers = DbtTaskGroup(
            group_id="customers",
            project_config=ProjectConfig((DBT_ROOT_PATH / "jaffle_shop").as_posix(), dbt_vars={"var": "2"}),
            profile_config=profile_config,
            default_args={"retries": 2},
        )
        pre_dbt >> customers
    basic_cosmos_task_group()



Choose how to render nodes
~~~~~~~~~~~~~~~~~~~~~~~~~~

Cosmos allows you to define how you want to convert dbt nodes into Airflow. For example, the following script defines how you want to represent a dbt **source** node.

.. code-block:: python

    def convert_source(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
        """
        Return an instance of a desired operator to represent a dbt "source" node.
        """
        return EmptyOperator(dag=dag, task_group=task_group, task_id=f"{node.name}_source"
        )
    render_config = RenderConfig(
        node_converters={
            DbtResourceType("source"): convert_source,  # known dbt node type to Cosmos (part of DbtResourceType)
        }
    )
    project_config = ProjectConfig(
        DBT_ROOT_PATH / "simple",
        env_vars={"DBT_SQLITE_PATH": DBT_SQLITE_PATH},
        dbt_vars={"animation_alias": "top_5_animated_movies"},
    )
    example_cosmos_sources = DbtDag(
        project_config=project_config,
        profile_config=profile_config,
        render_config=render_config,
    )


