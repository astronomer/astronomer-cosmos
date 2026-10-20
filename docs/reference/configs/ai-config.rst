.. _ai-config:

AI config
=========

Cosmos can ask an LLM why a dbt task failed, and add a structured root-cause summary to the task's error message and
logs. It is built on the `Common AI provider <https://airflow.apache.org/docs/apache-airflow-providers-common-ai/stable/index.html>`_
of Airflow and is configured with the ``cosmos.config.AiConfig`` class. It is off by default.

.. note::

    This feature is new. Its options may change based on feedback.

Requirements
~~~~~~~~~~~~

Install Cosmos with the ``ai`` extra, which installs ``apache-airflow-providers-common-ai>=1.0.0``:

.. code-block:: bash

    pip install "astronomer-cosmos[ai]"

The provider needs Python 3.11+ and Airflow 2.11+. On older versions, the install would try to upgrade Airflow itself.
To let the agent check your warehouse schema (``introspect_schema``), also install the provider's SQL extra:
``apache-airflow-providers-common-ai[sql]``.

Then create an Airflow connection for your LLM. Its type can be ``pydanticai``, ``pydanticai_azure``,
``pydanticai_bedrock`` or ``pydanticai_vertex``.

Usage
~~~~~

.. code-block:: python

    from cosmos import AiConfig, DbtDag

    dag = DbtDag(
        # ...
        ai_config=AiConfig(llm_conn_id="my_llm_conn", diagnose_on_failure=True),
    )

``ai_config`` can also be set in ``operator_args``, but not in both places. ``diagnose_on_failure=True`` is only
supported with ``ExecutionMode.LOCAL`` and ``ExecutionMode.VIRTUALENV``. Cosmos raises an error when parsing the DAG for
any other execution mode, such as ``ExecutionMode.WATCHER``, whose producer runs one ``dbt build`` for the whole DAG.

The diagnosis is added after the original error, so the dbt failure details always come first.

When it runs, and what it costs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- It runs once per failed task, on the task's last attempt. With ``retries=3``, the first three failures don't call the
  LLM.
- A diagnosis makes at most ``max_requests`` requests to the model, and its LLM run is cancelled after
  ``timeout_seconds``. Every request carries the whole prompt, which is bounded by the limits of
  :ref:`what is sent <ai-config-sent>`. The time limit is checked while Cosmos waits for the model, so a call that blocks
  its thread is cancelled when it returns.
- Many tasks failing for the same reason mean many calls. To limit the cost, set ``ai_config`` only on the
  ``DbtTaskGroup`` that needs it.
- The diagnosis runs inside the task, so its time counts toward the task's ``execution_timeout``.
- A failed diagnosis never hides the dbt error: Cosmos logs a warning and raises the original error. The same goes for an
  error that Cosmos did not expect, which it logs at the error level.
- Killing the task cancels the diagnosis.

.. _ai-config-sent:

What is sent to the LLM
~~~~~~~~~~~~~~~~~~~~~~~

For each failed dbt node, Cosmos sends the error message and the compiled SQL that dbt wrote in ``run_results.json``.
When dbt fails before it runs any node, such as on a compilation error, it sends the end of the dbt output instead. With
``InvocationMode.DBT_RUNNER``, that is the error summary that dbt returns, not its full log.

You decide how much is sent, with these limits. A text over its limit keeps its head and its tail, with a marker for
what was left out, except the dbt output, which keeps its end:

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
     - The size of the biggest dbt artifact that is read. A bigger one is skipped, to protect the memory of the worker.
     - 50

The task log says how many characters were sent, so you can tune the limits with a real failure. Raise them to give the
model more to work with, at the cost of more tokens, or lower them to send less.

Before sending, Cosmos masks every secret registered with Airflow's secrets masker, such as connection passwords, and
then applies ``redactor``. Airflow can't know a secret that only exists as a dbt variable, and dbt errors can echo the SQL
around the problem. Use ``redactor`` to cover those cases:

.. code-block:: python

    import re

    AiConfig(
        llm_conn_id="my_llm_conn",
        diagnose_on_failure=True,
        redactor=lambda text: re.sub(r"(?i)(password|token)=\S+", r"\1=***", text),
    )

The error text of dbt and of the database can repeat the values of the rows involved, such as in a unique key
violation, and names such as hosts and users. Airflow's masker doesn't know them, so they are sent unless
``redactor`` hides them. The text from dbt is untrusted: Cosmos fences it and tells the model never to follow it, which
reduces the risk of prompt injection but doesn't remove it. If ``redactor`` raises, nothing is sent. What the schema tools return to the agent goes through the same masking and
``redactor``.

Schema introspection
~~~~~~~~~~~~~~~~~~~~

With ``introspect_schema=True``, the agent can list the tables and read the column definitions of the failed node and
of its parents, to check things like whether a column exists. It can't read rows or query other tables. Cosmos tells
the agent the exact names of the tables it can inspect, such as ``analytics.orders``, and the task log shows every table
it looked at. If it asks for a table outside that list, Cosmos logs a warning and the diagnosis continues without it.

Cosmos looks the tables up in the database of the Airflow connection, so a different database in ``profile_args`` is not
used. It needs ``ProfileConfig.profile_mapping`` with a connection whose Airflow hook is a SQL hook (``DbApiHook``), such as
Postgres, Snowflake, Redshift or MySQL. Cosmos reads the connection through Airflow, so the Airflow provider of your
warehouse must be installed, for example ``apache-airflow-providers-postgres``. This is not needed to run dbt itself.
For other connections (BigQuery, Databricks, Athena, Spark and ``generic``), or when the provider is missing, Cosmos
logs a warning and diagnoses without introspection.

Customize the output
~~~~~~~~~~~~~~~~~~~~

Use ``diagnosis_output_type`` for your own structured output. It must be a pydantic ``BaseModel``: Cosmos raises an error
for anything else, such as ``str``, a dataclass or a ``TypedDict``. Add ``pydantic.Field(description=...)`` to each field
to guide the LLM. Lists, dicts and nested models are rendered one item per line in the logs, and the line breaks and
indentation of a multi-line text, such as a SQL snippet, are kept.

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
- ``diagnosis_output_type``: A pydantic model class (a subclass of ``pydantic.BaseModel``) for the output. Defaults to
  ``root_cause``, ``suggested_fix`` and ``confidence``.
- ``diagnosis_instructions``: Your own system instructions for the agent.
- ``redactor``: A function that hides what Airflow's secrets masker can't know.
