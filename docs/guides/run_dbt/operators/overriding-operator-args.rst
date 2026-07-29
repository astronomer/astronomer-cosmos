.. _operator-args-per-node:

Overriding operator arguments per dbt node (or group of nodes)
==============================================================

.. versionadded:: 1.8.0

Cosmos 1.8 introduced the capability for users to customise the operator arguments per dbt node, or per group of dbt nodes.
This can be done by defining the arguments via a dbt meta property alongside other dbt project configurations.

Let's say there is a DbtTaskGroup that sets a default pool to run all the dbt tasks, but a user would like the model expensive
to run a separate pool.

Users could either use ``operator_args`` or ``default args`` for defining the default behavior:

.. code-block:: python

    dbt_task_group = DbtTaskGroup(
        # ...
        profile_config=ProfileConfig,
        default_args={"pool": "default_pool"},
    )

While configuring in the dbt model YAML (e.g. ``models/schema.yml``) a different behaviour for the model "expensive", that should use the "expensive-pool":

.. code-block:: yaml

    version: 2
    models:
      - name: expensive
        description: description
        meta:
          cosmos:
            operator_kwargs:
              pool: expensive-pool


More information about this feature can be found in :ref:`custom-airflow-properties`.

To learn how to customise the profile per dbt model or Cosmos task, check :ref:`profile-customise-per-node`.

.. _operator-args-test-nodes:

Overriding operator arguments for dbt tests
-------------------------------------------

When using ``TestBehavior.AFTER_EACH``, the test task inherits the operator arguments of the resource it tests, and then
applies the ``operator_kwargs`` declared by the tests it runs. This allows, for example, retrying model runs without
retrying tests, which is useful when retrying a slow test would delay subsequent DAG runs:

.. code-block:: yaml

    # dbt_project.yml - retry every model run twice, but never retry their tests
    models:
      my_dbt_project:
        +meta:
          cosmos:
            operator_kwargs:
              retries: 2

    data_tests:  # named `tests` before dbt 1.8
      my_dbt_project:
        +meta:
          cosmos:
            operator_kwargs:
              retries: 0

The ``data_tests`` configuration above applies to both types of dbt data test. Generic (schema) tests can also be
configured individually in the model YAML, and singular tests can declare
``{{ config(meta={"cosmos": {"operator_kwargs": {...}}}) }}`` in their SQL file:

.. code-block:: yaml

    # models/schema.yml - a single generic test that should not be retried
    version: 2
    models:
      - name: orders
        columns:
          - name: order_id
            data_tests:
              - unique:
                  config:
                    meta:
                      cosmos:
                        operator_kwargs:
                          retries: 0

Since all the tests of a dbt node run in a single Airflow task, if they declare the same argument with different values,
the last one wins and Cosmos logs a warning. Tests without a parent resource (e.g. a singular test that does not
``ref()`` any model) are not rendered under ``TestBehavior.AFTER_EACH``, so their ``operator_kwargs`` do not apply.

Other test behaviors
~~~~~~~~~~~~~~~~~~~~

The tests' ``operator_kwargs`` are applied when Cosmos renders a test task for the resource being tested, which is the
case for ``TestBehavior.AFTER_EACH``. For the remaining behaviors:

- With ``TestBehavior.AFTER_ALL``, every test runs in a single project-wide task that is not associated with any dbt
  node, so it uses ``operator_args`` and the tests' ``operator_kwargs`` do not apply. To retry models but not tests, set
  ``operator_args={"retries": 0}`` and override ``retries`` for the models in ``dbt_project.yml``.
- With ``TestBehavior.BUILD``, there is no separate test task: tests run as part of the resource's ``dbt build``, using
  that resource's arguments.
- Detached tests are rendered as their own task, from the test node itself, so they always use their own
  ``operator_kwargs``.

Unit tests
~~~~~~~~~~

Unit tests (dbt 1.8+) are declared as ``unit_tests`` and are not rendered as Airflow tasks by Cosmos. When the resource
they test also has data tests, they run as part of that resource's test task, because ``dbt test --select <resource>``
selects them. The ``operator_kwargs`` declared under ``unit_tests`` in ``dbt_project.yml`` (or in the unit test
``config``) have no effect: the arguments used are the ones described above, inherited from the resource being tested and
overridden by the ones its data tests declare.

When a resource has unit tests but no data tests, Cosmos does not create a test task for it under
``TestBehavior.AFTER_EACH`` at all, so nothing runs its unit tests. Whether they run otherwise depends on the dbt
command the surrounding task issues - the project-wide ``dbt test`` of ``TestBehavior.AFTER_ALL``, or the
``dbt build`` of ``TestBehavior.BUILD``.

.. code-block:: yaml

    # dbt_project.yml - this has no effect, since Cosmos does not create a task for unit tests
    unit_tests:
      my_dbt_project:
        +meta:
          cosmos:
            operator_kwargs:
              retries: 0
