.. _dag_customization:

Post-rendering DAG customization
================

.. note::
    The DbtToAirflowConverter.tasks_map property is only available for cosmos >= 1.8.0

After Cosmos has rendered an Airflow DAG from a dbt project, you may want to add some extra Airflow tasks that interact
with the tasks created by Cosmos. This document explains how to do this.

An example use case you can think of is implementing sensor tasks that wait for an external DAG task to complete before
running a source node task (or task group, if the source contains a test).

Mapping from dbt nodes to Airflow tasks
----------------------

To interact with Airflow tasks created by Cosmos,
you can iterate over the dag.dbt_graph.filtered_nodes property like so:

..
   This is an abbreviated copy of example_tasks_map.py, as GitHub does not render literalinclude blocks

.. code-block:: python

    with DbtDag(
        dag_id="customized_cosmos_dag",
        # Other arguments omitted for brevity
    ) as dag:
        # Walk the dbt graph
        for unique_id, dbt_node in dag.dbt_graph.filtered_nodes.items():
            # Filter by any dbt_node property you prefer. In this case, we are adding upstream tasks to source nodes.
            if dbt_node.resource_type == DbtResourceType.SOURCE:
                # Look up the corresponding Airflow task or task group in the DbtToAirflowConverter.tasks_map property.
                task = dag.tasks_map[unique_id]
                # Create a task upstream of this Airflow source task/task group.
                upstream_task = EmptyOperator(task_id=f"upstream_of_{unique_id}")
                upstream_task >> task

You can also leverage ``dbt_node.context_dict["depends_on"]`` to find upstream dependencies of every dbt node.
This way, you can add sensors for inter-dag dependencies to ensure these follow the dbt graph without having to manually wire each node.












