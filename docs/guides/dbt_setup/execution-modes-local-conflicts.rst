.. _execution-modes-local-conflicts:

Apache Airflow® and dbt dependencies conflicts
-----------------------------------------------

When using the :ref:`local-execution` without defining a custom ``ExecutionConfig.dbt_executable_path``, you might have dependency conflicts between
`Apache Airflow® <https://airflow.apache.org/>`_ and dbt. The number of conflicts can increase depending on the Airflow providers and dbt adapters you use.

If you find errors, we recommend users isolating the installation of dbt from the Airflow installation.
With the ``local`` execution mode, this can be accomplished by installing dbt in a separate
Python virtualenv and setting the `ExecutionConfig.dbt_executable_path <../../reference/configs/execution-config.html>`_  and
`RenderConfig.dbt_executable_path <../../guides/translate_dbt_to_airflow/render-config.html>`_ parameters.

The page, :ref:`execution-modes` describes many other methods that support isolating dbt from Airflow.

In the following table, ``x`` represents combinations that lead to conflicts (vanilla ``apache-airflow`` and ``dbt-core`` packages):

+---------------+-----+-----+-----+-----+-----+------+------+
| Airflow / DBT | 1.5 | 1.6 | 1.7 | 1.8 | 1.9 | 1.10 | 1.11 |
+===============+=====+=====+=====+=====+=====+======+======+
| 2.9           |     |     |     |     |     |      |      |
+---------------+-----+-----+-----+-----+-----+------+------+
| 2.10          |     |     |     |     |     |      |      |
+---------------+-----+-----+-----+-----+-----+------+------+
| 2.11          |     |     |     |     |     |      |      |
+---------------+-----+-----+-----+-----+-----+------+------+
| 3.0           | x   | x   | x   |     |     |  x   |      |
+---------------+-----+-----+-----+-----+-----+------+------+
| 3.1           | x   | x   | x   | x   |     |  x   |      |
+---------------+-----+-----+-----+-----+-----+------+------+
| 3.2           | x   | x   | x   | x   |     |  x   |      |
+---------------+-----+-----+-----+-----+-----+------+------+

Examples of errors
++++++++++++++++++

.. code-block:: bash

  ERROR: Cannot install apache-airflow==3.1 and dbt-core==1.5 because these package versions have conflicting dependencies.

  The conflict is caused by:
      dbt-core 1.5.0 depends on Jinja2==3.1.2
      apache-airflow-core 3.1.0 depends on jinja2>=3.1.5

.. code-block:: bash

  ERROR: Cannot install apache-airflow and dbt-core==1.10.0 because these package versions have conflicting dependencies.

  The conflict is caused by:
      dbt-core 1.10.0 depends on pydantic<2
      apache-airflow-core 3.0.0 depends on pydantic>=2.11.0



How to reproduce
++++++++++++++++

The table was created by running  `nox <https://nox.thea.codes/en/stable/>`__ with the following ``noxfile.py``:

.. code-block:: python

    import nox

    nox.options.sessions = ["compatibility"]
    nox.options.reuse_existing_virtualenvs = True


    @nox.session(python=["3.10"])
    @nox.parametrize(
        "dbt_version",
        ["1.5", "1.6", "1.7", "1.8", "1.9", "1.10", "1.11"],
    )
    @nox.parametrize(
        "airflow_version",
        ["2.9", "2.10", "2.11", "3.0", "3.1", "3.2"],
    )
    def compatibility(session: nox.Session, airflow_version, dbt_version) -> None:
        """Run both unit and integration tests."""
        session.run(
            "pip3",
            "install",
            "--pre",
            f"apache-airflow=={airflow_version}",
            f"dbt-core=={dbt_version}",
        )
