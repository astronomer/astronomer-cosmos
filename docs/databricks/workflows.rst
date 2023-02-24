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

.. exampleinclude:: /../astronomer/providers/databricks/example_dags/example_databricks_workflow.py
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

Limitations
===========
The DatabricksWorkflowTaskGroup is currently in beta and has the following limitations:

* Since Databricks Workflow Jobs do not support dynamic parameters at the task level, we recommend placing dynamic parameters
at the TaskGroup level (e.g. the ``notebook_params`` parameter in the example above). This will ensure that the job is not changed every time
the DAG is run.
* If you plan to run the same DAG multiple times at the same time, make sure to set the ``max_concurrency`` parameter to the expected number of concurrent runs.
