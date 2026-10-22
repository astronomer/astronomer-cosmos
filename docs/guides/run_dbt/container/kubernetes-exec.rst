.. _kubernetes-exec:

Kubernetes exec execution mode
==============================

``ExecutionMode.KUBERNETES_EXEC`` runs dbt commands in an existing running Kubernetes
container using Airflow's ``KubernetesPodExecOperator``. Cosmos still creates tasks
and dependencies for individual dbt nodes, but does not create, restart or delete Pods.
This avoids provisioning a Pod for each task. Each command still starts a new dbt process.

Requirements
~~~~~~~~~~~~

Install the optional dependency on the Airflow workers and components that parse Dags:

.. code-block:: bash

    pip install 'astronomer-cosmos[kubernetes-exec]'

This mode requires ``apache-airflow-providers-cncf-kubernetes>=10.22.0``. Other Kubernetes
execution modes retain their existing minimum provider version.

The target container must already be running and provide:

- dbt, its database adapter and the ``env`` executable.
- The dbt project and installed dbt packages at the configured container path.
- A ``profiles.yml`` file and database credentials accessible to dbt.
- Writable directories for dbt's logs and generated artifacts.

The Airflow Kubernetes connection must reach the cluster and have permission to read
the target Pod and execute commands through ``pods/exec``. A non-running target fails
the task; Cosmos does not wait for the Pod to become ready.

Configure the mode
~~~~~~~~~~~~~~~~~~

Keep the manifest available to the Dag parser. ``dbt_project_path`` below is the path
inside the target container; ``dbt_executable_path`` also refers to that container.
It defaults to ``dbt`` on the container's ``PATH``, independently of the scheduler's
dbt installation. Set an explicit path to use a different executable in the container.
The container's default environment is inherited. ``ProjectConfig.env_vars`` adds or
overrides variables for each exec process without modifying the Pod configuration.

.. code-block:: python

    from cosmos import DbtDag, ExecutionConfig, ProjectConfig, RenderConfig
    from cosmos.constants import ExecutionMode, LoadMode

    dbt_dag = DbtDag(
        dag_id="dbt_existing_pod",
        project_config=ProjectConfig(
            manifest_path="/opt/airflow/dbt/manifest.json",
            project_name="analytics",
            env_vars={"DBT_PROFILES_DIR": "/opt/dbt/profiles"},
        ),
        render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.KUBERNETES_EXEC,
            dbt_project_path="/opt/dbt/analytics",
        ),
        operator_args={
            "pod_name": "dbt-runner",
            "namespace": "analytics",
            "container_name": "dbt",
            "kubernetes_conn_id": "kubernetes_default",
            "pool": "dbt_runner",
        },
        schedule=None,
        catchup=False,
    )

Create the ``dbt_runner`` Airflow pool with one slot before running this example.
Use that pool for every task and Dag that shares this Pod and project directory.
For parallel execution, provide independent workspaces or Pods, including separate
dbt target and log paths. Sharing writable artifacts between concurrent dbt processes
can corrupt their results.

Supported commands include run, build, seed, snapshot, test, source freshness, ls,
run-operation and clone. Their operators are available in
``cosmos.operators.kubernetes_exec`` for direct use outside ``DbtDag`` and ``DbtTaskGroup``.

Set ``do_xcom_push=True`` in ``operator_args`` to return command stdout through XCom,
using the provider's output size limit (``max_xcom_output_size``). This does not
collect dbt artifact files.

Pod lifecycle
~~~~~~~~~~~~~

The same mode supports a Pod created before a Dag run and removed afterwards, or a Pod
kept available by an external process. For a run-specific Pod, add setup and readiness
tasks before a ``DbtTaskGroup`` and a teardown task afterwards. Pass its name through
the templated ``pod_name`` argument. The process that owns the Pod is responsible for
its lifetime, readiness, project version and cleanup.

Limitations
~~~~~~~~~~~

- Pod creation options such as ``image``, ``volumes`` and ``container_resources`` do not
  apply. Configure them when creating the Pod.
- Exec does not rerun the image entrypoint. Any setup needed by dbt must already be
  available, or be performed by a wrapper executable selected with ``dbt_executable_path``.
- ``ProfileConfig`` selects the profile and target names only. Cosmos does not generate
  or copy profiles into the container, install packages, or collect dbt artifact files.
- Commands stream stdout and stderr to task logs. ``on_warning_callback`` is not supported;
  use dbt's ``warn_error`` option when warnings should fail a task.
- Execution is synchronous. This mode does not implement the Watcher protocol or deferral.
- A nonzero remote exit code fails the task. Closing the exec connection on cancellation
  does not guarantee the remote dbt process stops. Before retrying a disconnected or
  cancelled task, ensure the previous command has stopped to avoid overlapping executions.
- Container reuse removes Pod startup overhead, not dbt process startup or project parsing.
