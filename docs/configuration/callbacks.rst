.. _callbacks:

Callbacks
=========

Cosmos supports callback functions that execute at the end of a task's execution when using ``ExecutionMode.LOCAL`` and
``ExecutionMode.VIRTUALENV``.
These callbacks can be used for various purposes, such as uploading files from the target directory to remote
storage. While this feature has been available for some time, users may not be fully aware of its capabilities.

With the Cosmos 1.8.0 release, several helper functions were added in the ``cosmos/io.py`` module. These functions
provide examples of callback functions that can be hooked into Cosmos DAGs to upload files from the project’s
target directory to remote cloud storage providers such as AWS S3, GCP GS, and Azure WASB.

Example: Using Callbacks with a Single Operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To demonstrate how to specify a callback function for uploading files from the target directory, here’s an example
using a single operator in an Airflow DAG:

.. literalinclude:: ../../dev/dags/example_operators.py
    :language: python
    :start-after: [START single_operator_callback]
    :end-before: [END single_operator_callback]

Example: Using Callbacks with ``remote_target_path`` (Airflow 2.8+)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you're using Airflow 2.8 or later, you can leverage the :ref:`remote_target_path` configuration to upload files
from the target directory to a remote storage. Below is an example of how to define a callback helper function in your
``DbtDag`` that utilizes this configuration:

.. literalinclude:: ../../dev/dags/cosmos_callback_dag.py
    :language: python
    :start-after: [START cosmos_callback_example]
    :end-before: [END cosmos_callback_example]

Custom Callbacks
~~~~~~~~~~~~~~~~

The helper functions introduced in Cosmos 1.8.0 are just examples of how callback functions can be written and passed
to Cosmos DAGs. Users are not limited to using these predefined functions — they can also create their own custom
callback functions to meet specific needs. These custom functions can be provided to Cosmos DAGs, where they will
receive the path to the cloned project directory and the Airflow task context, which includes DAG and task instance
metadata.

Limitations and Contributions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Currently, callback support is available only when using ``ExecutionMode.LOCAL`` and ``ExecutionMode.VIRTUALENV``.
Contributions to extend this functionality to other execution modes are welcome and encouraged. You can reference the
implementation for ``ExecutionMode.LOCAL`` to add support for other modes.
