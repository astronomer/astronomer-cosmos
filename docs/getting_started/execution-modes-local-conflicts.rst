.. _execution-modes-local-conflicts:

Airflow and DBT dependencies conflicts
======================================

When using the `Local Execution Mode <execution-modes.html#local>`__, users may face dependency conflicts between
Apache Airflow and DBT. The amount of conflicts may increase depending on the Airflow providers and DBT plugins being used.

If you find errors, we recommend users look into using `alternative execution modes <execution-modes.html>`__.

In the following table, ``x`` represents combinations that lead to conflicts (vanilla ``apache-airflow`` and ``dbt-core`` packages):

+---------------+-----+-----+-----+-----+-----+-----+---------+
| Airflow \ DBT | 1.0 | 1.1 | 1.2 | 1.3 | 1.4 | 1.5 | 1.6.0b6 |
+===============+=====+=====+=====+=====+=====+=====+=========+
| 2.2           |     |     |     | x   | x   | x   | x       |
+---------------+-----+-----+-----+-----+-----+-----+---------+
| 2.3           | x   | x   |     | x   | x   | x   | x       |
+---------------+-----+-----+-----+-----+-----+-----+---------+
| 2.4           | x   | x   | x   |     |     |     |         |
+---------------+-----+-----+-----+-----+-----+-----+---------+
| 2.5           | x   | x   | x   |     |     |     |         |
+---------------+-----+-----+-----+-----+-----+-----+---------+
| 2.6           | x   | x   | x   | x   | x   |     | x       |
+---------------+-----+-----+-----+-----+-----+-----+---------+

Examples of errors
-----------------------------------

.. code-block:: bash

  ERROR: Cannot install apache-airflow==2.2.4 and dbt-core==1.5.0 because these package versions have conflicting dependencies.
  The conflict is caused by:
    apache-airflow 2.2.4 depends on jinja2<3.1 and >=2.10.1
    dbt-core 1.5.0 depends on Jinja2==3.1.2

.. code-block:: bash

  ERROR: Cannot install apache-airflow==2.6.0 and dbt-core because these package versions have conflicting dependencies.
  The conflict is caused by:
    apache-airflow 2.6.0 depends on importlib-metadata<5.0.0 and >=1.7; python_version < "3.9"
    dbt-semantic-interfaces 0.1.0.dev7 depends on importlib-metadata==6.6.0


How to reproduce
----------------

The table was created by running  `nox <https://nox.thea.codes/en/stable/>`__ with the following ``noxfile.py``:

.. code-block:: python

  import nox


  nox.options.sessions = ["compatibility"]
  nox.options.reuse_existing_virtualenvs = True


  @nox.session(python=["3.10"])
  @nox.parametrize("dbt_version", ["1.0", "1.1", "1.2", "1.3", "1.4", "1.5", "1.6.0b6"])
  @nox.parametrize("airflow_version", ["2.2.4", "2.3", "2.4", "2.5", "2.6"])
  def compatibility(session: nox.Session, airflow_version, dbt_version) -> None:
      """Run both unit and integration tests."""
      session.run(
          "pip3",
          "install",
          "--pre",
          f"apache-airflow=={airflow_version}",
          f"dbt-core=={dbt_version}",
      )
