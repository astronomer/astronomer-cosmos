Databricks Workflow TaskGroup
""""""""""""""""""""""""""""""""""""

The Databricks Workflow TaskGroup is a recent addition to the Databricks platform that allows users to easily create
and manage Databricks Jobs with multiple notebooks, SQL statements, python files, etc. One of the biggest benefits
offered by Databricks Jobs is the use of Job Clusters, which are significantly cheaper than all-purpose clusters.

With the DatabricksWorkflowTaskGroup, users can take advantage of the cost savings offered by using Jobs clusters,
while also having the flexibility and multi-platform capabilities of Airflow.

The DatabricksWorkflowTaskGroup is designed to look and function like a standard task group,
with the added ability to include specific Databricks arguments.
An example of how to use the DatabricksWorkflowTaskGroup can be seen in the following code snippet:

.. literalinclude:: /../examples/databricks/example_databricks_workflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_databricks_workflow_notebook]
    :end-before: [END howto_databricks_workflow_notebook]

At the top-level Taskgroup definition, users can define workflow-level parameters such as ``notebook_params``,
``notebook_packages`` or ``spark_submit_params``. These parameters will be applied to all tasks within the Workflow.
Inside of the taskgroup, users can define the individual tasks that make up the workflow. Currently the only officially
supported operator is the DatabricksNotebookOperator, but other operators can be used as long as they contain the
``convert_to_databricks_workflow_task`` function. In the future we plan to support SQL and python functions via the
ref:`https://github.com/astronomer/astro-sdk<Astro SDK>`.

For each notebook task, packages defined with the ``notebook_packages`` parameter defined at the task level are
installed and additionally, all the packages supplied via the workflow-level parameter ``notebook_packages`` are also
installed for its run. The collated ``notebook_packages`` list type parameter is transformed into the ``libraries`` list
type parameter accepted by the Databricks API and a list of supported library types and their format for the API
specification is mentioned at the Databricks documentation:
https://docs.databricks.com/dev-tools/api/latest/libraries.html#managedlibrarieslibrary

.. warning::
    Make sure that you do not specify duplicate libraries across workflow-level and task-level ``notebook-packages`` as
    the Databricks task will then fail complaining about duplicate installation of the library.

Retries
=======

When repairing a DatabBricks workflow, we need to submit a repair request using databricks' Jobs API. One core difference between how databricks repairs work v.s. Airflow retries is that Airflow is able to retry one task at a time, while Databricks expects a single repair request for all tasks you want to rerun (this is because databricks starts a new job cluster for each repair request, and jobs clusters can't be modified once they are started).

To avoid creating multiple clusters for each failed task, we do not use Airflow's built-in retries. Instead, we offer a "Repair all tasks" button in the "launch" task's grid and graph node on the Airflow UI. This button finds all failed and skipped tasks and sends them to Databricks for repair. By using this approach, we can save time and resources, as we do not need to create a new cluster for each failed task.

![](img/repair-all-failed.png)

In addition to the "Repair all tasks" button, we also provide a "Repair single task" button to repair a specific failed task. This button can be used if we want to retry a single task rather than all failed tasks.

![](img/repair-single-failed.png)

It is important to note that Databricks does not support repairing a task that has a failing upstream. Therefore, if we want to skip a notebook and run downstream tasks, we need to add a "dbutils.notebook.exit("success")" at the top of the notebook inside of databricks. This will ensure that the notebook does not run and the downstream tasks can continue to execute.

![](img/dbutils-notebook-success.png)
Limitations
===========
The DatabricksWorkflowTaskGroup is currently in beta and has the following limitations:

* Since Databricks Workflow Jobs do not support dynamic parameters at the task level, we recommend placing dynamic parameters
at the TaskGroup level (e.g. the ``notebook_params`` parameter in the example above). This will ensure that the job is not changed every time
the DAG is run.
* If you plan to run the same DAG multiple times at the same time, make sure to set the ``max_concurrency`` parameter to the expected number of concurrent runs.
