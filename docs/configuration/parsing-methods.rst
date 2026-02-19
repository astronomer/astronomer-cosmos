.. _parsing-methods:


Parsing Methods
===============

Cosmos offers several options to parse your dbt project:

- ``automatic``. Tries to find a user-supplied ``manifest.json`` file. If it can't find one, it will run ``dbt ls`` to generate one. If that fails, it will use Cosmos' dbt parser.
- ``dbt_manifest``. Parses a user-supplied ``manifest.json`` file. This can be generated manually with dbt commands or via a CI/CD process.
- ``dbt_ls``. Parses a dbt project directory using the ``dbt ls`` command.
- ``dbt_ls_file``. Parses a dbt project directory using the output of ``dbt ls`` command from a file.
- ``custom``. Uses Cosmos' custom dbt parser, which extracts dependencies from your dbt's model code.

There are benefits and drawbacks to each method:

- ``dbt_manifest``: You have to generate the manifest file on your own. When using the manifest, Cosmos gets a complete set of metadata about your models. However, Cosmos uses its own selecting & excluding logic to determine which models to run, which may not be as robust as dbt's.
- ``dbt_ls``: Cosmos will generate the manifest file for you. This method uses dbt's metadata AND dbt's selecting/excluding logic. This is the most robust method. However, this requires the dbt executable to be installed on your machine (either on the host directly or in a virtual environment).
- ``dbt_ls_file`` (new in 1.3): Path to a file containing the ``dbt ls`` output. To use this method, run ``dbt ls`` using ``--output json`` and store the output in a file. ``RenderConfig.select`` and ``RenderConfig.exclude`` will not work using this method.
- ``custom``: Cosmos will parse your project and model files. This means that Cosmos will not have access to dbt's metadata. However, this method does not require the dbt executable to be installed on your machine, and does not require the user to provide any dbt artifacts.

If you're using the ``local`` mode, you should use the ``dbt_ls`` method.

If you're using the ``docker`` or ``kubernetes`` modes, you should use either ``dbt_manifest`` or ``custom`` modes.


``automatic``
-------------

When you don't supply an argument to the ``load_mode`` parameter (or you supply the value ``"automatic"``), Cosmos will attempt the other methods in order:

1. Use a pre-existing ``manifest.json`` file (``dbt_manifest``)
2. Try to generate a ``manifest.json`` file from your dbt project (``dbt_ls``)
3. Use Cosmos' dbt parser (``custom``)

To use this method, you don't need to supply any additional config. This is the default.


``dbt_manifest``
----------------

If you already have a ``manifest.json`` file created by dbt, Cosmos will parse the manifest to generate your DAG.

You can supply a ``manifest_path`` parameter on the DbtDag / DbtTaskGroup with a path to a ``manifest.json`` file.

Starting with Cosmos 1.6.0, you can supply a remote path (e.g., an S3 URL) for ``manifest_path``,
in addition to local paths. For remote paths, Cosmos leverages the
`Airflow Object Storage <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html>`_
feature.
For remote paths, you can specify a ``manifest_conn_id``, which is an
Airflow connection ID containing the credentials to access the remote path. If you do not specify a
``manifest_conn_id``, Cosmos will use the default connection ID specific to the scheme, identified using the Airflow
hook's ``default_conn_id`` corresponding to the URL's scheme.

Examples of how to supply ``manifest.json`` using ``manifest_path`` argument:

- Local path:

.. literalinclude:: ../../dev/dags/cosmos_manifest_example.py
    :language: python
    :start-after: [START local_example]
    :end-before: [END local_example]

- AWS S3 URL (available since Cosmos 1.6):

Ensure that you have the required dependencies installed to use the S3 URL. You can install the required dependencies
using the following command: ``pip install "astronomer-cosmos[amazon]"``

.. literalinclude:: ../../dev/dags/cosmos_manifest_example.py
    :language: python
    :start-after: [START aws_s3_example]
    :end-before: [END aws_s3_example]

- GCP GCS URL (available since Cosmos 1.6):

Ensure that you have the required dependencies installed to use the GCS URL. You can install the required dependencies
using the following command: ``pip install "astronomer-cosmos[google]"``

.. literalinclude:: ../../dev/dags/cosmos_manifest_example.py
    :language: python
    :start-after: [START gcp_gs_example]
    :end-before: [END gcp_gs_example]

- Azure Blob Storage URL (available since Cosmos 1.6):

Ensure that you have the required dependencies installed to use the Azure blob URL. You can install the required
dependencies using the following command: ``pip install "astronomer-cosmos[microsoft]"``

.. literalinclude:: ../../dev/dags/cosmos_manifest_example.py
    :language: python
    :start-after: [START azure_abfs_example]
    :end-before: [END azure_abfs_example]

Package models (e.g. from the Elementary package) are included in the DAG when they are present in the manifest.
Generate the manifest **after** installing and enabling packages: run ``dbt deps`` then ``dbt compile`` (or ``dbt run``)
from your project directory. If the manifest was built before a package was added or enabled, package models will not
appear in the DAG until you regenerate the manifest.


``dbt_ls``
----------

.. note::

    This only works if a dbt command / executable is available to the scheduler.

If you don't have a ``manifest.json`` file, Cosmos will attempt to generate one from your dbt project. It does this by running ``dbt ls`` and parsing the output.

When Cosmos runs ``dbt ls``, it also passes your ``select`` and ``exclude`` arguments to the command. This means that Cosmos will only generate a manifest for the models you want to run.

To use this:

.. code-block:: python

    DbtDag(
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
        )
        # ...,
    )

Starting in Cosmos 1.5, Cosmos will cache the output of the ``dbt ls`` command, to improve the performance of this
parsing method. Learn more `here <./caching.html>`_.

Since Cosmos 1.9, it will attempt to use dbt as a library, and run ``dbt ls`` using the ``dbtRunner``  that is available for `dbt programmatic invocations <https://docs.getdbt.com/reference/programmatic-invocations>`__. This mode requires dbt version 1.5.0 or higher.
This mode,  named ``InvocationMode.DBT_RUNNER``, also depends on dbt being installed in the same Python virtual environment as Airflow.
In previous Cosmos versions, Cosmos would always run ``dbt ls`` using the  Python ``subprocess`` module, which can lead to significant CPU and memory usage.

Users can force Cosmos to run ``dbt ls`` with subprocess and not ``dbtRunner``, by setting:

.. code-block:: python

    DbtDag(
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS, invocation_mode=InvocationMode.SUBPROCESS
        )
        # ...,
    )


For more information, check the `RenderConfig docs <./render-config.html>`_.


``dbt_ls_file``
----------------

.. note::
   New in Cosmos 1.3.

If you provide the output of ``dbt ls --output json`` as a file, you can use this to parse similar to  ``dbt_ls``.
You can supply a ``dbt_ls_path`` parameter on the DbtDag / DbtTaskGroup with a path to a ``dbt_ls_output.txt`` file.
Check `this Dag <https://github.com/astronomer/astronomer-cosmos/blob/main/dev/dags/user_defined_profile.py>`_ for an example.

To use this:

.. code-block:: python

    DbtDag(
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS_FILE, dbt_ls_path="/path/to/dbt_ls_file.txt"
        )
        # ...,
    )

``custom``
----------

If the above methods fail, Cosmos will default to using its own dbt parser. This parser is not as robust as dbt's, so it's recommended that you use one of the above methods if possible.

The following are known limitations of the custom parser:

- it does not read from the ``dbt_project.yml`` file
- it does not parse Python files or models

To use this:

.. code-block:: python

    DbtDag(
        render_config=RenderConfig(
            load_method=LoadMode.CUSTOM,
        )
        # ...,
    )
