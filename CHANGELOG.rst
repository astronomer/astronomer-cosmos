Changelog
=========

1.0.0 (2022-12-14)
-------------------

* Initial release, with the following **6** workflow Operators/Parsers:

.. list-table::
   :header-rows: 1

   * - Operator/Sensor Class
     - Import Path
     - Example DAG

   * - ``DBTTestOperator``
     - .. code-block:: python

        from cosmos.providers.dbt.core.operators import DBTBaseOperator
     - N/A

   * - ``DBTSeedOperator``
     - .. code-block:: python

        from cosmos.providers.dbt.core.operators import DBTSeedOperator
     - `Example DAG <https://github.com/astronomer/astronomer-cosmos/blob/1.0.0/examples/dags/extract_dag.py>`__

   * - ``DBTRunOperator``
     - .. code-block:: python

        from cosmos.providers.dbt.core.operators import DBTRunOperator
     - N/A

   * - ``DBTTestOperator``
     - .. code-block:: python

        from cosmos.providers.dbt.core.operators import DBTTestOperator
     - N/A

   * - ``DbtDag``
     - .. code-block:: python

        from cosmos.providers.dbt.core.dag import DbtDag
     - `Example DAG <https://github.com/astronomer/astronomer-cosmos/blob/1.0.0/examples/dags/attribution-playbook.py>`__

   * - ``DbtTaskGroup``
     - .. code-block:: python

        from cosmos.providers.dbt.core.dag import DbtTaskGroup
     - `Example DAG <https://github.com/astronomer/astronomer-cosmos/blob/1.0.0/examples/dags/jaffle_shop.py>`__
