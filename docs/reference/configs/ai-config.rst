.. _ai-config:

AI Config
=========

Cosmos can optionally use an LLM to diagnose why a dbt task failed and surface a structured
root-cause summary in the task logs, instead of just the raw dbt error. This requires
`apache-airflow-providers-common-ai <https://pypi.org/project/apache-airflow-providers-common-ai/>`_
to be installed; if it isn't, diagnosis is skipped with a warning and the task fails as usual.
It is configured via the ``cosmos.config.AiConfig`` class, currently supported for
``ExecutionMode.LOCAL`` and ``ExecutionMode.VIRTUALENV``.

The ``AiConfig`` class takes the following arguments:

- ``llm_conn_id``: Airflow connection ID for the LLM provider (a ``pydanticai`` / ``pydanticai-azure`` /
  ``pydanticai-bedrock`` / ``pydanticai-vertex`` connection), as defined by
  ``apache-airflow-providers-common-ai``.
- ``diagnose_on_failure``: When ``True``, diagnose dbt task failures with an LLM call and append a
  structured root-cause summary to the exception message. Defaults to ``False``.
- ``introspect_schema``: When ``True`` and ``ProfileConfig.profile_mapping`` is set, give the
  diagnosis agent read-only access to the live warehouse schema (via ``SQLToolset``) to confirm
  hypotheses, e.g. whether a column actually exists. Defaults to ``False``.
- ``timeout_seconds``: Wall-clock budget for the diagnosis LLM call. Exceeding it aborts the
  diagnosis, not the task, and falls back to the standard exception. Defaults to ``30``.
- ``diagnosis_output_type``: Optional custom pydantic model describing the structured output the
  diagnosis agent should produce, instead of the built-in ``DbtFailureDiagnosis`` (fields
  ``root_cause``, ``suggested_fix``, ``confidence``). Add ``pydantic.Field(description=...)`` to each
  field to guide the LLM. String, list, and dict fields are all rendered readably in the task log.
- ``diagnosis_instructions``: Optional custom system instructions for the diagnosis agent, instead
  of Cosmos's built-in prompt.

.. code-block:: python

    from cosmos import AiConfig, DbtDag

    dag = DbtDag(
        # ...
        ai_config=AiConfig(llm_conn_id="my_llm_conn", diagnose_on_failure=True),
    )
