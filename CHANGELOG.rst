Changelog
=========

1.2.2 (2023-11-06)
------------------

Bug fixes

* Support ``ProjectConfig.dbt_project_path = None`` & different paths for Rendering and Execution by @MrBones757 in #634
* Fix adding test nodes to DAGs built using ``LoadMethod.DBT_MANIFEST`` and ``LoadMethod.CUSTOM`` by @edgga in #615

Others

* Add pre-commit hook for McCabe max complexity check and fix errors by @jbandoro in #629
* Update contributing docs for running integration tests by @jbandoro in #638
* Fix CI issue running integration tests by @tatiana in #640 and #644
* pre-commit updates in #637


1.2.1 (2023-10-25)
------------------

Bug fixes

* Resolve errors occurring when ``dbt_project_path`` is str and partial support ``dbt_project_path=None`` by @MrBones757 in #605
* Fix running dbt tests that depend on multiple models (support ``--indirect-selection buildable``) by @david-mag in #613
* Add tests to sources, snapshots and seeds when using ``TestBehavior.AFTER_EACH`` by @tatiana in #599
* Fix custom selector when select has a subset of tags of the models' tags by @david-mag in #606
* Fix ``LoadMode.AUTOMATIC`` behaviour to use ``LoadMode.DBT_LS`` when ``ProfileMapping`` is used by @tatiana in #625
* Fix failure if ``openlineage-common`` raises a jinja exception by @tatiana in #626

Others

* Update contributing guide docs by @raffifu in #591
* Remove unnecessary stack trace from Cosmos initialization by @tatiana in #624
* Fix running test that validates manifest-based DAGs by @tatiana in #619
* pre-commit updates in #604 and #621


1.2.0 (2023-10-13)
------------------

Features

* Add support to model versioning available since dbt 1.6 by @binhnq94 in #516
* Add AWS Athena profile mapping by @benjamin-awd in #578
* Support customizing how dbt nodes are converted to Airflow by @tatiana in #503
* Make the arg ``dbt_project_path`` in the ``ProjectConfig`` optional by @MrBones757 in #581

Bug fixes

* Fix Cosmos custom selector to support filtering a single model by @jlaneve and @harels in #576
* Fix using ``GoogleCloudServiceAccountDictProfileMapping`` together with ``LoadMethod.DBT_LS`` by @joppevos in #587
* Fix using the ``full_refresh`` argument in projects that contain tests by @EgorSemenov and @tatiana in #590
* Stop creating symbolic links for ``dbt_packages`` (solves ``LocalExecutor`` concurrency issue) by @tatiana in #600

Others

* Docs: add reference to original Jaffle Shop project by @erdos2n in #583
* Docs: retries & note about DagBag error by @TJaniF in #592
* pre-commit updates in #575 and #585


1.1.3 (2023-09-28)
------------------

Bug fixes

* Only create task group and test task only if the model has a test by @raffifu in #543
* Fix parsing test nodes when using the custom load method (LoadMethod.CUSTOM) by @raffifu in #563
* Fix ``DbtTestOperator`` when test does not have ``test_metadata`` by @javihernovoa and @tatiana in #565
* Support dbt 1.6 and apache-airflow-providers-cncf-kubernetes 7.3.0  by @tatiana in #564



1.1.2 (2023-09-27)
------------------

Bug fixes

* Fix using ``ExecutionMode.KUBERNETES`` by @pgoslatara and @tatiana in #554
* Add support to ``apache-airflow-providers-cncf-kubernetes < 7.4.0`` by @tatiana in #553
* Fix ``on_warning_callback`` behaviour on ``DbtTestLocalOperator`` by @edgga, @marco9663 and @tatiana in #558
* Use ``returncode`` instead of ``stderr`` to determine dbt graph loading errors by @cliff-lau-cloverhealth in #547
* Improve error message in ``config.py`` by @meyobagero in #532
* Fix ``DbtTestOperator`` when test does not have ``test_metadata`` by @tatiana in #558
* Fix ``target-path`` not specified issue in ``dbt-project.yml`` by @tatiana in #533

Others

* Docs: add reference links to dbt and Airflow columns by @TJaniF in #542
* pre-commit updates #552 and #546



1.1.1 (2023-09-14)
------------------

Bug fixes

* Fix attempt of emitting OpenLineage events if task execution fails by @tatiana in #526
* Fix Rust dependency for Windows users by @tatiana in #526
* Fix DbtRunOperationLocalOperator missing flags by @tatiana in #529
* Fix DbtRunLocalOperator to support the full refresh argument by @tatiana in #529
* Remove redundant prefix of task names when test_behavior = TestBehavior.AFTER_EACH by @binhnq94 in #524
* Fix rendering vars in ``DbtModel`` when using ``LoadMode.CUSTOM`` by @dojinkimm in #502

Others

* Docs: add `documentation comparing Airflow and dbt concepts <https://astronomer.github.io/astronomer-cosmos/getting_started/dbt-airflow-concepts.html>`_ by @tatiana in #523.
* Update PyPI project links by @tatiana in #528
* pre-commit updates


1.1.0 (2023-09-06)
------------------

Features

* Support dbt global flags (via dbt_cmd_global_flags in operator_args) by @tatiana in #469
* Support parsing DAGs when there are no connections by @jlaneve in #489

Enhancements

* Hide sensitive field when using BigQuery keyfile_dict profile mapping by @jbandoro in #471
* Consistent Airflow Dataset URIs, inlets and outlets with `Openlineage package <https://pypi.org/project/openlineage-integration-common/>`_ by @tatiana in #485. `Read more <https://astronomer.github.io/astronomer-cosmos/configuration/lineage.html>`_.
* Refactor ``LoadMethod.DBT_LS`` to run from a temporary directory with symbolic links by @tatiana in #488
* Run ``dbt deps`` when using ``LoadMethod.DBT_LS`` by @DanMawdsleyBA in #481
* Update Cosmos log color to purple by @harels in #494
* Change operators to log ``dbt`` commands output as opposed to recording to XCom by @tatiana in #513

Bug fixes

* Fix bug on select node add exclude selector subset ids logic by @jensenity in #463
* Refactor dbt ls to run from a temporary directory, to avoid Read-only file system errors during DAG parsing, by @tatiana in #414
* Fix profile_config arg in DbtKubernetesBaseOperator by @david-mag in #505
* Fix SnowflakePrivateKeyPemProfileMapping private_key reference by @nacpacheco in #501
* Fix incorrect temporary directory creation in VirtualenvOperator init by @tatiana in #500
* Fix log propagation issue by @tatiana in #498
* Fix PostgresUserPasswordProfileMapping to retrieve port from connection by @jlneve in #511

Others

* Docs: Fix RenderConfig load argument by @jbandoro in #466
* Enable CI integration tests from external forks by @tatiana in #458
* Improve CI tests runtime by @tatiana in #457
* Change CI to run coverage after tests pass by @tatiana in #461
* Fix forks code revision in code coverage by @tatiana in #472
* [pre-commit.ci] pre-commit autoupdate by @pre-commit-ci in #467
* Drop support to Python 3.7 in the CI test matrix by @harels in #490
* Add Airflow 2.7 to the CI test matrix by @tatiana in #487
* Add MyPy type checks to CI since we exceeded pre-commit disk quota usage by @tatiana in #510

1.0.5 (2023-08-09)
------------------

Enhancements

* Improve logs to include astornomer-cosmos identifier by @tatiana in #450
* Support OAuth authentication for Big Query by @MonideepDe in #431

Bug fixes

* Fix selector for config tags by @javihernovoa in #441
* Fix BigQuery keyfile_dict mapping for connection created from webserver UI by @jbandoro in #449

Others

* [pre-commit.ci] pre-commit autoupdate by @pre-commit-ci in #446
* Resolve MyPy errors when adding Airflow pre-commit dependency by @abhi12mohan in #434


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
