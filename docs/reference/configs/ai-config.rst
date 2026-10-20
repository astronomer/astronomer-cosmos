.. _ai-config:

AI config
=========

Cosmos can ask an LLM why a dbt task failed, and add the answer to the error message and the logs of the task. It uses
the `Common AI provider <https://airflow.apache.org/docs/apache-airflow-providers-common-ai/stable/index.html>`_ of
Airflow and is configured with ``cosmos.config.AiConfig``. It is off by default.

.. note::

    This feature is new. Its options may change based on feedback.

Requirements
~~~~~~~~~~~~

Install Cosmos with the ``ai`` extra, which installs ``apache-airflow-providers-common-ai>=1.0.0``. The provider needs
Python 3.11+ and Airflow 2.11+.

.. code-block:: bash

    pip install "astronomer-cosmos[ai]"

Also install:

- The SDK of your model vendor, with an extra of the provider: ``[openai]`` (also for Azure), ``[anthropic]``,
  ``[bedrock]`` or ``[google]`` (also for Vertex). Without it, Cosmos logs a warning and skips the diagnosis.
- ``apache-airflow-providers-common-ai[sql]``, only to use ``introspect_schema``.

Then create an Airflow connection for your LLM, of type ``pydanticai``, ``pydanticai_azure``, ``pydanticai_bedrock`` or
``pydanticai_vertex``.

Usage
~~~~~

.. code-block:: python

    from cosmos import AiConfig, DbtDag

    dag = DbtDag(
        # ...
        ai_config=AiConfig(llm_conn_id="my_llm_conn", diagnose_on_failure=True),
    )

The error message keeps the dbt error first and ends with the diagnosis:

.. code-block:: text

    dbt invocation completed with errors: db_error: Database Error in model db_error (models/db_error.sql)
      column "missing_column" does not exist

    --------------------------------------------------------------------------------
    AI diagnosis (confidence: high)
    --------------------------------------------------------------------------------
    Root cause:
      The model selects a column named missing_column from s.base, but that table only has id and
      amount. The column was probably mistyped, or renamed upstream.

    Suggested fix:
      Select an existing column in models/db_error.sql, or add the column to s.base.

    --------------------------------------------------------------------------------

``ai_config`` can also be set in ``operator_args``, but not in both places. ``diagnose_on_failure=True`` only works with
``ExecutionMode.LOCAL`` and ``ExecutionMode.VIRTUALENV``. With any other execution mode, Cosmos raises an error when it
parses the DAG.

When it runs, and what it costs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- It runs once per failed task, on its last attempt. With ``retries=3``, the first three failures don't call the LLM.
- It makes at most ``max_requests`` requests, each with the whole prompt, and is cancelled after ``timeout_seconds``.
- Many tasks that fail for the same reason mean many calls. To limit the cost, set ``ai_config`` only on the
  ``DbtTaskGroup`` that needs it.
- It runs inside the task, so its time counts toward the ``execution_timeout`` of the task.
- A diagnosis that fails never hides the dbt error: Cosmos logs why and raises the original error.
- Killing the task cancels the diagnosis.

.. _ai-config-sent:

What is sent to the LLM
~~~~~~~~~~~~~~~~~~~~~~~

For each failed dbt node, Cosmos sends the error message and the compiled SQL that dbt wrote in ``run_results.json``.
When dbt fails before it runs any node, such as on a compilation error, it sends the end of the dbt output instead. With
``InvocationMode.DBT_RUNNER``, that is the error summary of dbt, not its full log.

A text over its limit keeps its head and its tail, with a marker for what was left out. The dbt output keeps its end.

.. list-table::
   :header-rows: 1

   * - Option
     - Limits
     - Default
   * - ``max_failed_nodes``
     - The failed nodes that are sent. The others are counted, not sent.
     - 5
   * - ``max_message_chars``
     - The characters of the error message of each node.
     - 4000
   * - ``max_sql_chars``
     - The characters of the compiled SQL of each node.
     - 8000
   * - ``max_output_chars``
     - The characters of the dbt output, when no node failed.
     - 8000
   * - ``max_artifact_mb``
     - The size of the biggest dbt artifact that is read. A bigger one is skipped.
     - 50

The task log says how many characters were sent, so you can tune the limits with a real failure.

Protect sensitive data
~~~~~~~~~~~~~~~~~~~~~~

Before anything is sent, Cosmos masks every secret that Airflow knows, such as connection passwords, and then applies
your ``redactor``. This also covers what the schema tools return to the agent, and the diagnosis itself.

Airflow can't know a secret that only exists as a dbt variable. The error text of dbt and of the database can also
repeat the values of the rows involved, as in a unique key violation, and names such as hosts and users. Use
``redactor`` to hide them. If it raises, nothing is sent.

.. code-block:: python

    import re

    AiConfig(
        llm_conn_id="my_llm_conn",
        diagnose_on_failure=True,
        redactor=lambda text: re.sub(r"(?i)(password|token)=\S+", r"\1=***", text),
    )

The text from dbt is untrusted. Cosmos fences it and tells the model never to follow it, which reduces the risk of
prompt injection but doesn't remove it.

Schema introspection
~~~~~~~~~~~~~~~~~~~~

With ``introspect_schema=True``, the agent can list the tables and read the columns of the failed node and of its
parents, to check things like whether a column exists. It is limited to 100 tables, the failed node first. It can't
read rows or query other tables, and the task log shows each table it looked at. If it asks for another table, Cosmos
logs a warning and the diagnosis goes on without it.

It needs ``ProfileConfig.profile_mapping`` with a connection whose Airflow hook is a SQL hook (``DbApiHook``), such as
Postgres, Snowflake, Redshift or MySQL. For other connections (BigQuery, Databricks, Athena, Spark and ``generic``),
Cosmos logs a warning and diagnoses without introspection.

Two more things to know:

- Cosmos reads the connection through Airflow, so the Airflow provider of your warehouse must be installed, such as
  ``apache-airflow-providers-postgres``. Without it, Cosmos logs a warning and diagnoses without introspection.
- The tables are looked up in the database of the Airflow connection, not in a different database set in
  ``profile_args``.

The agent uses the connection that dbt uses. Cosmos gives it only read-only tools, for those tables only, but the
provider calls such an allow-list a guardrail, not a security boundary. Use a database user that can only read what dbt
needs.

Customize the output
~~~~~~~~~~~~~~~~~~~~

Use ``diagnosis_output_type`` for your own structured output. It must be a pydantic ``BaseModel``, or Cosmos raises an
error. Add ``pydantic.Field(description=...)`` to each field to guide the LLM. Lists and nested models are rendered one
item per line in the logs, and multi-line text, such as SQL, keeps its line breaks.

.. code-block:: python

    from pydantic import BaseModel, Field


    class Issue(BaseModel):
        file: str = Field(description="The dbt file with the problem.")
        problem: str = Field(description="What is wrong in it.")


    class MyDiagnosis(BaseModel):
        issues: list[Issue] = Field(description="Every problem that explains the failure.")
        confidence: str


    AiConfig(
        llm_conn_id="my_llm_conn",
        diagnose_on_failure=True,
        diagnosis_output_type=MyDiagnosis,
    )

Arguments
~~~~~~~~~

- ``llm_conn_id``: Airflow connection ID for the LLM.
- ``diagnose_on_failure``: Diagnose failed tasks. Defaults to ``False``.
- ``introspect_schema``: Let the agent check the schema of the related tables. Defaults to ``False``.
- ``timeout_seconds``: Time limit for the LLM run. When it is reached, the run is cancelled and the task fails with the
  dbt error, without a diagnosis. Must be a number above ``0``. Defaults to ``30``.
- ``max_requests``: The most requests that the agent can make to the model. Defaults to ``8``.
- ``max_failed_nodes``, ``max_message_chars``, ``max_sql_chars``, ``max_output_chars`` and ``max_artifact_mb``: How much
  is sent to the model, as described in :ref:`what is sent <ai-config-sent>`. Each is a whole number of at least ``1``.
- ``diagnosis_output_type``: A pydantic model class for the output. Defaults to ``root_cause``, ``suggested_fix`` and
  ``confidence``.
- ``diagnosis_instructions``: Your own system instructions for the agent.
- ``redactor``: A function that hides what the secrets masker of Airflow can't know.
