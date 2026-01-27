.. _callbacks:

Callbacks
=========

.. note::
    Feature available when using ``ExecutionMode.LOCAL`` and ``ExecutionMode.VIRTUALENV``.

Most dbt commands output `one or more artifacts <https://docs.getdbt.com/reference/artifacts/dbt-artifacts>`_
such as ``semantic_manifest.json``, ``manifest.json``, ``catalog.json``, ``run_results.json``, and ``sources.json`` in the target folder, which by default resides in the dbt project's root folder.
However, since Cosmos creates temporary folders to run each dbt command, this folder vanishes by the end of the Cosmos task execution,
alongside the artifacts created by dbt.

Many users care about those artifacts and want to perform additional actions after running the dbt command. Some examples of usage:

* Upload the artifacts to an object storage;
* Run a command after the dbt command runs, such as `montecarlo <https://docs.getmontecarlo.com/docs/dbt-core>`_; or
* Define other custom behaviours based on a specific artifact.

To support these use cases, Cosmos allows users to define functions called callbacks that can run as part of the task execution before deleting the target's folder.

Users can define their custom callback methods or, since Cosmos 1.8.0, they can leverage built-in callbacks, available in `cosmos/io.py <https://github.com/astronomer/astronomer-cosmos/blob/main/cosmos/io.py>`_ module.
These functions illustrate how to upload the generated dbt artifacts to remote cloud storage providers such as AWS S3, GCP GCS, and Azure WASB.

There are two ways users can leverage using Cosmos auxiliary callback functions:

* When instantiating a Cosmos operator;
* When using ``DbtDag`` or ``DbtTaskGroup`` (users can define a callback that will apply to all tasks).


Example: Using Callbacks with a Single Operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To demonstrate how to specify a callback function for uploading files from the target directory, here’s an example
using a single operator in an Airflow DAG:

.. literalinclude:: ../../dev/dags/example_operators.py
    :language: python
    :start-after: [START single_operator_callback]
    :end-before: [END single_operator_callback]

Example: Using DbtDag or DbtTaskGroup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can leverage the :ref:`remote_target_path` configuration to upload files
from the target directory to a remote storage. Below is an example of how to define a callback helper function in your
``DbtDag`` that utilizes this configuration:

.. literalinclude:: ../../dev/dags/cosmos_callback_dag.py
    :language: python
    :start-after: [START cosmos_callback_example]
    :end-before: [END cosmos_callback_example]

An example of how the data uploaded to GCS looks like when using ``upload_to_gcp_gs`` in a ``DbtDag``:

.. image:: /_static/cosmos_callback_object_storage.png

The path naming convention is:

* Bucket configured by the user
* Name of the DAG
* DAG Run identifier
* Task ID
* Task retry identifier
* Target folder with its contents

If users are unhappy with this structure or format, they can implement similar methods, which can be based (or not) on the Cosmos standard ones.

Custom Callbacks
~~~~~~~~~~~~~~~~

The helper functions introduced in Cosmos 1.8.0 are examples of how callback functions. Users are not limited to using these predefined functions — they can also create their custom
callback functions to meet specific needs.

Cosmos passes a few arguments to these functions, including the path to the dbt project directory and the Airflow task context, which includes DAG and task instance metadata.

Below, find an example of a callback method that raises an exception if the query takes more than 10 seconds to run:

.. code-block:: python

    def error_if_slow(project_dir: str, **kwargs: Any) -> None:
        """
        An example of a custom callback that errors if a particular query is slow.

        :param project_dir: Path of the project directory used by Cosmos to run the dbt command
        """
        import json
        from pathlib import Path

        slow_query_threshold = 10

        target_path = f"{project_dir}/target"
        run_results_path = Path(target_path, "run_results.json")
        if run_results_path.exists():
            with open(run_results_path) as fp:
                run_results = json.load(fp)
                for result in run_results["results"]:
                    node_name = result["unique_id"]
                    execution_time = result["execution_time"]
                    if execution_time > slow_query_threshold:
                        raise TimeoutError(
                            f"The query for the node {node_name} took too long: {execution_time}"
                        )

Users can use the same approach to call the data observability platform `montecarlo <https://docs.getmontecarlo.com/docs/dbt-core>`_ or other services.

.. code-block:: python

    def montecarlo_import_artifacts(
        project_dir: str,
        mcd_id: str,
        mcd_token: str,
        job_name: str,
        project_name: str = "default-project",
        resource_id: str = None,
        **kwargs,
    ):
        """
         An example of a custom callback that import dbt artifacts to Monte Carlo.

        Args:
        :param project_dir: Path of the project directory used by Cosmos to run the dbt command
        :param mcd_id: Monte Carlo token user ID
        :param mcd_token: Monte Carlo token value
        :param job_name: Job name (required - perhaps a logical sequence of dbt executions)
        :param project_name: Project name (perhaps a logical group of dbt models)
        :param resource_id: UUID of the warehouse as described by MonteCarlo. If not specified, the
                    first warehouse connected to the user's account will be used
        """
        from pycarlo.core import Client, Session, Query
        from pycarlo.features.dbt.dbt_importer import DbtImporter

        def get_resource_id(client):
            """Get the resource ID of the first warehouse connected to the user's account"""
            query = Query()
            query.get_user().account.warehouses.__fields__(
                "name", "connection_type", "uuid"
            )
            warehouses = client(query).get_user.account.warehouses
            warehouse_list = []
            if len(warehouses) > 0:
                for val in warehouses:
                    warehouse_list.append(val.uuid)
            else:
                raise Exception(
                    "no warehouses connected ! Please check your Monte Carlo account."
                )
            return warehouse_list[0]

        if not mcd_id or not mcd_token:
            raise Exception(
                "Monte Carlo credentials are required to authenticate with MonteCarlo!"
            )

        client = Client(session=Session(mcd_id=mcd_id, mcd_token=mcd_token))

        dbt_importer = DbtImporter(mc_client=client)

        target_path = f"{project_dir}/target"

        import_options = {
            "manifest_path": f"{target_path}/manifest.json",
            "run_results_path": f"{target_path}/run_results.json",
            "project_name": project_name,
            "job_name": job_name,
        }

        if resource_id:
            import_options["resource_id"] = resource_id
        else:
            first_resource_id = get_resource_id(client)
            import_options["resource_id"] = first_resource_id

        dbt_importer.import_run(**import_options)
        print("Successfully sent dbt run artifacts to Monte Carlo")


Limitations and Contributions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Callback support is available only when using ``ExecutionMode.LOCAL`` and ``ExecutionMode.VIRTUALENV``.
Contributions to extend this functionality to other execution modes are welcome and encouraged. You can reference the
implementation for ``ExecutionMode.LOCAL`` to add support for other modes.
