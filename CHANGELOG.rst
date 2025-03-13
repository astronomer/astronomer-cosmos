Changelog
=========

1.9.1 (2025-03-13)
--------------------

Bug Fixes

* Fix import error in dbt bigquery adapter mock for ``dbt-bigquery<1.8`` for ``ExecutionMode.AIRFLOW_ASYNC`` by @pankajkoti in #1548
* Fix ``operator_args`` override configuration by @ghjklw in #1558
* Fix missing ``install_dbt_deps`` in ``ProjectConfig`` ``__init__`` method by @ghjklw in #1556
* Fix dbt project parsing ``dbt_vars`` behavior passed via ``operator_args`` by @AlexandrKhabarov in #1543
* Avoid reading the connection during DAG parsing of the async BigQuery operator by @joppevos in #1582
* Fix: Workaround to incorrectly raised ``gcsfs.retry.HttpError`` (Invalid Credentials, 401) by @tatiana in #1598
* Fix the async execution mode read sql files for dbt packages by @pankajastro in #1588
* Improve BQ async error handling by @tatiana in #1597
* Fix path selector when ``manifest.json`` is created using MS Windows by @tatiana in #1601
* Fix log that prints 'Total filtered nodes' by @tatiana in #1603
* Fix select behaviour using ``LoadMode.MANIFEST`` and a path with star by @tatiana in #1602
* Support ``on_warning_callback`` with ``TestBehavior.BUILD`` and ``ExecutionMode.LOCAL`` by @corsettigyg in #1571
* Fix ``DbtRunLocalOperator.partial()`` support by @tatiana in #1609
* fix: ``container_name`` is null for ecs integration by @nicor88 in #1592

Docs

* Improve MWAA getting-started docs by removing unused imports by @jx2lee in #1562

Others

* Disable ``example_cosmos_dbt_build.py`` DAG in CI by @pankajastro in #1567
* Upgrade GitHub Actions Ubuntu version by @tatiana in #1561
* Update GitHub bug issue template by @pankajastro in #1586
* Enable DAG ``example_cosmos_dbt_build.py`` in CI by @pankajastro in #1573
* Run async DAG in DAG without setup/teardown task by @pankajastro in #1599
* Add test case that fully covers recent select issue by @tatiana in #1604
* Add CI job to test multiple dbt versions for the async DAG by @pankajkoti in #1535
* Improve unit tests speed from 89s to 14s by @tatiana in #1600
* Pre-commit updates: #1560, #1583, #1596


1.9.0 (2025-02-19)
--------------------

Breaking changes

* When using ``LoadMode.DBT_LS``, Cosmos will now attempt to use the ``dbtRunner`` as opposed to subprocess to run ``dbt ls``.
  While this represents significant performance improvements (half the vCPU usage and some memory consumption improvement), this may not work in
  scenarios where users had multiple Python virtual environments to manage different versions of dbt and its adaptors. In those cases,
  please, set ``RenderConfig(invocation_mode=InvocationMode.SUBPROCESS)`` to have the same behaviour Cosmos had in previous versions.
  Additional information `here <https://astronomer.github.io/astronomer-cosmos/configuration/parsing-methods.html#dbt-ls>`_ and `here <https://astronomer.github.io/astronomer-cosmos/configuration/render-config.html#how-to-run-dbt-ls-invocation-mode>`_.

Features

* Use ``dbtRunner`` in the DAG Processor when using ``LoadMode.DBT_LS`` if ``dbt-core`` is available by @tatiana in #1484. Additional information `here <https://astronomer.github.io/astronomer-cosmos/configuration/parsing-methods.html#dbt-ls>`_.
* Allow users to opt-out of ``dbtRunner`` during DAG parsing with ``InvocationMode.SUBPROCESS`` by @tatiana in #1495. Check out the `documentation <https://astronomer.github.io/astronomer-cosmos/configuration/render-config.html#how-to-run-dbt-ls-invocation-mode>`_.
* Add structure to support multiple db for async operator execution by @pankajastro in #1483
* Support overriding the ``profile_config`` per dbt node or folder using config by @tatiana in #1492. More information `here <https://astronomer.github.io/astronomer-cosmos/profiles/#profile-customise-per-node>`_.
* Create and run accurate SQL statements when using ``ExecutionMode.AIRFLOW_ASYNC`` by @pankajkoti, @tatiana and @pankajastro in #1474
* Add AWS ECS task run execution mode by @CarlosGitto and @aoelvp94 in #1507
* Add support for running ``DbtSourceOperator`` individually by @victormacaubas in #1510
* Add setup task for async executions by @pankajastro in #1518
* Add teardown task for async executions by @pankajastro in #1529
* Add ``ProjectConfig.install_dbt_deps`` & change operator ``install_deps=True`` as default by @tatiana in #1521
* Extend Virtualenv operator and mock dbt adapters for setup & teardown tasks in ``ExecutionMode.AIRFLOW_ASYNC`` by @pankajkoti, @tatiana and @pankajastro in #1544

Bug Fixes

* Fix select complex intersection of three tag-based graph selectors by @tatiana in #1466
* Fix custom selector behaviour when the model name contains periods by @yakovlevvs and @60098727 in #1499
* Filter dbt and non-dbt kwargs correctly for async operator by @pankajastro in #1526

Enhancement

* Fix OpenLineage deprecation warning by @CorsettiS in #1449
* Move ``DbtRunner`` related functions into ``dbt/runner.py`` module by @tatiana in #1480
* Add ``on_warning_callback`` to ``DbtSourceKubernetesOperator`` and refactor previous operators by @LuigiCerone in #1501
* Gracefully error when users set incompatible ``RenderConfig.dbt_deps`` and ``operator_args`` ``install_deps`` by @tatiana in #1505
* Store compiled SQL as template field for ``ExecutionMode.AIRFLOW_ASYNC`` by @pankajkoti in #1534

Docs

* Improve ``RenderConfig`` arguments documentation by @tatiana in #1514
* Improve callback documentation by @tatiana in #1516
* Improve partial parsing docs by @tatiana in #1520
* Fix typo in selecting & excluding docs by @pankajastro in #1523
* Document ``async_py_requirements`` added in ``ExecutionConfig`` for ``ExecutionMode.AIRFLOW_ASYNC`` by @pankajkoti in #1545

Others

* Ignore dbt package tests when running Cosmos tests by @tatiana in #1502
* Refactor to consolidate async dbt adapter code by @pankajkoti in #1509
* Log elapsed time for sql file(s) upload/download by @pankajastro in #1536
* Remove the fallback operator for async task by @pankajastro in #1538
* GitHub Actions Dependabot: #1487
* Pre-commit updates: #1473, #1493, #1503, #1531


1.8.2 (2025-01-15)
--------------------

Bug Fixes

* Fix ``httpx.get`` exception handling while emitting telemetry by @tatiana in #1439
* Fix (not) rendering detached tests in ``TestBehavior.NONE`` and ``AFTER_ALL`` by @tatiana in #1463
* Fix detached test tasks names so they do not exceed 250 chars by @tatiana in #1464

Enhancement

* Allow users to opt-in or out (default) of detached test nodes by @tatiana in #1470. Learn more about this `here <https://astronomer.github.io/astronomer-cosmos/configuration/testing-behavior.html>`_.

Docs

* Docs: Fix broken links and rendering by @pankajastro in #1437
* Update ``operator args`` docs to include ``install_deps`` by @tatiana in #1456
* Improve Cosmos ``select`` docs to include latest graph operator support by @tatiana in #1467

Others

* Upgrade GitHub action artifacts upload-artifact & download-artifact to v4  by @pankajkoti in #1445
* Enable Depandabot to scan outdated Github Actions dependencies by @tatiana in #1446
* Pre-commit hook updates in #1459, #1441
* Dependabot Github action updates in #1451, #1452, #1453, #1454, #1455


1.8.1 (2024-12-30)
--------------------

Bug Fixes

* Fix rendering dbt tests with multiple parents by @tatiana in #1433
* Add ``kwargs`` param in DocsOperator method ``upload_to_cloud_storage`` by @pankajastro in #1422

Docs

* Improve OpenLineage documentation by @tatiana in #1431

Others

* Enable Docs DAG in CI leveraging existing CI connections by @pankajkoti in #1428
* Install providers with airflow by @pankajkoti in #1432
* Remove unused docs dependency by @pankajastro in #1414
* Pre-commit hook updates in #1424


1.8.0 (2024-12-20)
--------------------

New Features

* Support customizing Airflow operator arguments per dbt node by @wornjs in #1339. `More information <https://astronomer.github.io/astronomer-cosmos/getting_started/custom-airflow-properties.html>`_.
* Support uploading dbt artifacts to remote cloud storages via callback by @pankajkoti in #1389. `Read more <https://astronomer.github.io/astronomer-cosmos/configuration/callbacks.html>`_.
* Add support to ``TestBehavior.BUILD`` by @tatiana in #1377. `Documentation <https://astronomer.github.io/astronomer-cosmos/configuration/testing-behavior.html>`_.
* Add support for the "at" operator when using ``LoadMode.DBT_MANIFEST`` or ``CUSTOM`` by @benjy44 in #1372
* Add dbt clone operator by @pankajastro in #1326, as documented in `here <https://astronomer.github.io/astronomer-cosmos/getting_started/operators.html>`_.
* Support rendering tasks with non-ASCII characters by @t0momi219 in #1278 `Read more <https://astronomer.github.io/astronomer-cosmos/configuration/task-display-name.html>`_.
* Add warning callback on source freshness by @pankajastro in #1400 `Read more <https://astronomer.github.io/astronomer-cosmos/configuration/source-nodes-rendering.html#on-warning-callback-callback>`_.
* Add Oracle Profile mapping by @slords and @pankajkoti in #1190 and #1404
* Emit telemetry to Scarf during DAG run by @tatiana in #1397
* Save tasks map as ``DbtToAirflowConverter`` property by @internetcoffeephone and @hheemskerk in #1362

Bug Fixes

* Fix the mock value of port in ``TrinoBaseProfileMapping`` to be an integer by @dwolfeu #1322
* Fix access to the ``dbt docs`` menu item outside of Astro cloud by @tatiana in #1312
* Add missing ``DbtSourceGcpCloudRunJobOperator`` in module ``cosmos.operators.gcp_cloud_run_job`` by @anai-s in #1290
* Support building ``DbtDag`` without setting paths in ``ProjectConfig`` by @tatiana in #1307
* Fix parsing dbt ls outputs that contain JSONs that are not dbt nodes by @tatiana in #1296
* Fix Snowflake Profile mapping when using AWS default region by @tatiana in #1406
* Fix dag rendering for taskflow + DbtTaskGroup combo by @pankajastro in #1360

Enhancements

* Improve dbt command execution logs to troubleshoot ``None`` values by @tatiana in #1392
* Add logging of stdout to dbt graph run_command by @KarolGongola in #1390
* Add ``profile_config`` for Docker by @andrewhlui in #1347
* Support rendering build operator task-id with non-ASCII characters by @pankajastro in #1415

Docs

* Remove extra ` char from docs by @pankajastro in #1345
* Add limitation about copying target dir files to remote by @pankajkoti in #1305
* Generalise example from README by @ReadytoRocc in #1311
* Add security policy by @tatiana, @chaosmaw and @lzdanski in # 1385
* Mention in documentation that the callback functionality is supported in ``ExecutionMode.VIRTUALENV`` by @pankajkoti in #1401

Others

* Restore Jaffle Shop so that ``basic_cosmos_dag`` works as documented by @tatiana in #1374
* Remove Pytest durations from tests scripts by @tatiana in #1383
* Remove typing-extensions as dependency by @pankajastro in #1381
* Pin dbt-databricks version to < 1.9 by @pankajastro in #1376
* Refactor ``dbt-sqlite`` tests to use ``dbt-postgres`` by @pankajastro in #1366
* Remove 'dbt-core<1.8.9' pin by @tatiana in #1371
* Remove dependency ``eval_type_backport`` by @tatiana in #1370
* Enable kubernetes tests for dbt>=1.8 by @pankajastro #1364
* CI Workaround: Pin dbt-core, Disable SQLite Tests, and Correctly Ignore Clone Test to Pass CI by @pankajastro in #1337
* Enable Azure task in the remote store manifest example DAG by @pankajkoti in #1333
* Enable GCP remote manifest task by @pankajastro in #1332
* Add exempt label option in GH action stale job by @pankajastro in #1328
* Add integration test for source node rendering by @pankajastro in #1327
* Fix vulnerability issue on docs dependency by @tatiana in #1313
* Add postgres pod status check for k8s tests in CI by @pankajkoti in #1320
* [CI] Reduce the amount taking to run tests in the CI from 5h to 11min by @tatiana in #1297
* Enable secret detection precommit check by @pankajastro in #1302
* Fix security vulnerability, by not pinning Airflow 2.10.0 by @tatiana in #1298
* Fix Netlify build timeouts by @tatiana in #1294
* Add stalebot to label/close stale PRs and issues by @tatiana in #1288
* Unpin dbt-databricks version by @pankajastro in #1409
* Fix source resource type tests by @pankajastro in #1405
* Increase performance tests models by @tatiana in #1403
* Drop running 1000 models in the CI by @pankajkoti in #1411
* Fix releasing package to PyPI by @tatiana in #1396
* Address review comments on PR 1347 regarding profile_config for ExecutionMode.Docker by @pankajkoti in #1413
* Pre-commit hook updates in #1394, #1373, #1358, #1340, #1331, #1314, #1301


1.7.1 (2024-10-29)
------------------

Bug fixes

* Fix ``DbtVirtualenvBaseOperator`` to use correct virtualenv Python path by @kesompochy in #1252
* Fix displaying dbt docs as menu item in Astro by @tatiana in #1280
* Fix: Replace login by user for clickhouse profile by @petershenri in #1255

Enhancements

* Improve dbt Docs Hosting Debugging -- Update dbt_docs_not_set_up.html by @johnmcochran in #1250
* Minor refactor on VirtualenvOperators & add test for PR #1253 by @tatiana in #1286

Docs

* Add Welcome Section and "What Is Cosmos" Blurb to Home Page by @cmarteepants and @yanmastin-astro in #1251
* Update the URL for sample dbt docs hosted in Astronomer S3 bucket by @pankajkoti in #1283
* Add dedicated scarf tracking pixel to readme by @cmarteepants in #1256


Others

* Update ``CODEOWNERS`` to track all files by @pankajkoti in #1284
* Fix release after the ``raw`` rst directive was disabled in PyPI by @tatiana in #1282
* Update issue template ``bug.yml`` - cosmos version update in the dropdown by @pankajkoti in #1275
* Pre-commit hook updates in #1285, #1274, #1254, #1244


1.7.0 (2024-10-04)
------------------

New Features

* Introduction of experimental support to run dbt BQ models using Airflow deferrable operators by @pankajkoti @pankajastro @tatiana in #1224 #1230.
  This is a first step in this journey and we would really appreciate feedback from the community.

  For more information, check the documentation: https://astronomer.github.io/astronomer-cosmos/getting_started/execution-modes.html#airflow-async-experimental

  This work has been inspired by the talk "Airflow at Monzo: Evolving our data platform as the bank scales" by
  @jonathanrainer @ed-sparkes given at Airflow Summit 2023: https://airflowsummit.org/sessions/2023/airflow-at-monzo-evolving-our-data-platform-as-the-bank-scales/.

* Support using ``DatasetAlias`` and fix orphaning unreferenced dataset by @tatiana in #1217 #1240

  Documentation: https://astronomer.github.io/astronomer-cosmos/configuration/scheduling.html#data-aware-scheduling

* Add GCP_CLOUD_RUN_JOB execution mode by @ags-de #1153

  Learn more about it: https://astronomer.github.io/astronomer-cosmos/getting_started/gcp-cloud-run-job.html

Enhancements

* Create single virtualenv when ``DbtVirtualenvBaseOperator`` has ``virtualenv_dir=None`` and ``is_virtualenv_dir_temporary=True`` by @kesompochy in #1200
* Consistently handle build and imports in ``cosmos/__init__.py`` by @tatiana in #1215
* Add enum constants to init for direct import by @fabiomx in #1184

Bug fixes

* URL encode dataset names to support multibyte characters by @t0momi219 in #1198
* Fix invalid argument (``full_refresh``) passed to DbtTestAwsEksOperator (and others) by @johnhoran in #1175
* Fix ``printer_width`` arg type in ``DbtProfileConfigVars`` by @jessicaschueler in #1191
* Fix task owner fallback by @jmaicher in #1195

Docs

* Add scarf to readme and docs for website analytics by @cmarteepants in #1221
* Add ``virtualenv_dir`` param to ``ExecutionConfig`` docs by @pankajkoti in #1173
* Give credits to @LennartKloppenburg in CHANGELOG.rst by @tatiana #1174
* Refactor docs for async mode execution by @pankajkoti in #1241

Others

* Remove PR branch added for testing a change in CI in #1224 by @pankajkoti in #1233
* Fix CI wrt broken coverage upload artifact @pankajkoti in #1210
* Fix CI issues - Upgrade actions/upload-artifact & actions/download-artifact to v4 and set min version for packaging by @pankajkoti in #1208
* Resolve CI failures for Apache Airflow 2.7 jobs by @pankajkoti in #1182
* CI: Update GCP manifest file path based on new secret update by @pankajkoti in #1237
* Pre-commit hook updates in #1176 #1186, #1186, #1201, #1219, #1231


1.6.0 (2024-08-20)
--------------------

New Features

* Add support for loading manifest from cloud stores using Airflow Object Storage by @pankajkoti in #1109
* Cache ``package-lock.yml`` file by @pankajastro in #1086
* Support persisting the ``LoadMode.VIRTUALENV`` directory @LennartKloppenburg and @tatiana in #1079 and #611
* Add support to store and fetch ``dbt ls`` cache in remote stores by @pankajkoti in #1147
* Add default source nodes rendering by @arojasb3 in #1107
* Add Teradata ``ProfileMapping`` by @sc250072 in #1077

Enhancements

* Add ``DatabricksOauthProfileMapping`` profile by @CorsettiS in #1091
* Use ``dbt ls`` as the default parser when ``profile_config`` is provided by @pankajastro in #1101
* Add task owner to dbt operators by @wornjs in #1082
* Extend Cosmos custom selector to support + when using paths and tags by @mvictoria in #1150
* Simplify logging by @dwreeves in #1108

Bug fixes

* Fix Teradata ``ProfileMapping`` target invalid issue by @sc250072 in #1088
* Fix empty tag in case of custom parser by @pankajastro in #1100
* Fix ``dbt deps`` of ``LoadMode.DBT_LS`` should use ``ProjectConfig.dbt_vars`` by @tatiana in #1114
* Fix import handling by lazy loading hooks introduced in PR #1109 by @dwreeves in #1132
* Fix Airflow 2.10 regression and add Airflow 2.10 in test matrix by @pankajastro in #1162

Docs

* Fix typo in azure-container-instance docs by @pankajastro in #1106
* Use Airflow trademark as it has been registered by @pankajastro in #1105

Others

* Run some example DAGs in Kubernetes execution mode in CI by @pankajastro in #1127
* Install requirements.txt by default during dev env spin up by @@CorsettiS in #1099
* Remove ``DbtGraph.current_version`` dead code by @tatiana in #1111
* Disable test for Airflow-2.5 and Python-3.11 combination in CI by @pankajastro in #1124
* Pre-commit hook updates in #1074, #1113, #1125, #1144, #1154, #1167


1.5.1 (2024-07-17)
------------------

Bug fixes

* Fix getting temporary AWS credentials with assume_role by @piotrkubicki in #1081
* Fix issue 'No such file or directory' by @tatiana in #1097

Others

* Change Cosmos dev status from alpha to prod by @tatiana in #1098
* Pre-commit hook updates in #1083, #1092


1.5.0 (2024-06-27)
------------------

New Features

* Speed up ``LoadMode.DBT_LS`` by caching dbt ls output in Airflow Variable by @tatiana in #1014
* Support to cache profiles created via ``ProfileMapping`` by @pankajastro in #1046
* Support for running dbt tasks in AWS EKS in #944 by @VolkerSchiewe
* Add Clickhouse profile mapping by @roadan and @pankajastro in #353 and #1016
* Add node config to TaskInstance Context by @linchun3 in #1044

Bug fixes

* Support partial parsing when cache is disabled by @tatiana in #1070
* Fix disk permission error in restricted env by @pankajastro in #1051
* Add CSP header to iframe contents by @dwreeves in #1055
* Stop attaching log adaptors to root logger to reduce logging costs by @glebkrapivin in #1047

Enhancements

* Support ``static_index.html`` docs by @dwreeves in #999
* Support deep linking dbt docs via Airflow UI by @dwreeves in #1038
* Add ability to specify host/port for Snowflake connection by @whummer in #1063

Docs

* Fix rendering for env ``enable_cache_dbt_ls`` by @pankajastro in #1069

Others

* Update documentation for DbtDocs generator by @arjunanan6 in #1043
* Use uv in CI by @dwreeves in #1013
* Cache hatch folder in the CI by @tatiana in #1056
* Change example DAGs to use ``example_conn`` as opposed to ``airflow_db`` by @tatiana in #1054
* Mark plugin integration tests as integration by @tatiana in #1057
* Ensure compliance with linting rule D300 by using triple quotes for docstrings by @pankajastro in #1049
* Pre-commit hook updates in #1039, #1050, #1064
* Remove duplicates in changelog by @jedcunningham in #1068


1.4.3 (2024-06-07)
------------------

Bug fixes

* Bring back ``dataset`` as a required field for BigQuery profile by @pankajkoti in #1033

Enhancements

* Only run ``dbt deps`` when there are dependencies by @tatiana and @AlgirdasDubickas in #1030

Docs

* Fix docs so it does not reference non-existing ``get_dbt_dataset`` by @tatiana in #1034


1.4.2 (2024-06-06)
------------------

Bug fixes

* Fix the invocation mode for ``ExecutionMode.VIRTUALENV`` by @marco9663 in #1023
* Fix Cosmos ``enable_cache`` setting by @tatiana in #1025
* Make ``GoogleCloudServiceAccountDictProfileMapping`` dataset profile arg optional by @oliverrmaa and @pankajastro in #839 and #1017
* Athena profile mapping set ``aws_session_token`` in profile only if it exists by @pankajastro in #1022

Others

* Update dbt and Airflow conflicts matrix by @tatiana in #1026
* Enable Python 3.12 unittest by @pankajastro in #1018
* Improve error logging in ``DbtLocalBaseOperator`` by @davidsteinar in #1004
* Add GitHub issue templates for bug reports and feature request by @pankajkoti in #1009
* Add more fields in bug template to reduce turnaround in issue triaging by @pankajkoti in #1027
* Fix ``dev/Dockerfile`` + Add ``uv pip install`` for faster build time by @dwreeves in #997
* Drop support for Airflow 2.3 by @pankajkoti in #994
* Update Astro Runtime image by @RNHTTR in #988 and #989
* Enable ruff F linting by @pankajastro in #985
* Move Cosmos Airflow configuration to settings.py by @pankajastro in #975
* Fix CI Issues by @tatiana in #1005
* Pre-commit hook updates in #1000, #1019


1.4.1 (2024-05-17)
------------------

Bug fixes

* Fix manifest testing behavior by @chris-okorodudu in #955
* Handle ValueError when unpacking partial_parse.msgpack by @tatiana in #972

Others

* Enable pre-commit run and fix type-check job by @pankajastro in #957
* Clean databricks credentials in test/CI by @tatiana in #969
* Update CODEOWNERS by @tatiana in #969 x
* Update emeritus contributors list by @tatiana in #961
* Promote @dwreeves to committer by @tatiana in #960
* Pre-commit hook updates in #956


1.4.0 (2024-05-13)
--------------------

Features

* Add dbt docs natively in Airflow via plugin by @dwreeves in #737
* Add support for ``InvocationMode.DBT_RUNNER`` for local execution mode by @jbandoro in #850
* Support partial parsing to render DAGs faster when using ``ExecutionMode.LOCAL``, ``ExecutionMode.VIRTUALENV`` and ``LoadMode.DBT_LS`` by @dwreeves in #800
* Improve performance by 22-35% or more by caching partial parse artefact by @tatiana in #904
* Add Azure Container Instance as Execution Mode by @danielvdende in #771
* Add dbt build operators by @dylanharper-qz in #795
* Add dbt profile config variables to mapped profile by @ykuc in #794
* Add more template fields to ``DbtBaseOperator`` by @dwreeves in #786
* Add ``pip_install_options`` argument to operators by @octiva in #808

Bug fixes

* Make ``PostgresUserPasswordProfileMapping`` schema argument optional by @FouziaTariq in #683
* Fix ``folder_dir`` not showing on logs for ``DbtDocsS3LocalOperator`` by @PrimOox in #856
* Improve ``dbt ls`` parsing resilience to missing tags/config by @tatiana in #859
* Fix ``operator_args`` modified in place in Airflow converter by @jbandoro in #835
* Fix Docker and Kubernetes operators execute method resolution by @jbandoro in #849
* Fix ``TrinoBaseProfileMapping`` required parameter for non method authentication by @AlexandrKhabarov in #921
* Fix global flags for lists by @ms32035 in #863
* Fix ``GoogleCloudServiceAccountDictProfileMapping`` when getting values from the Airflow connection ``extra__`` keys by @glebkrapivin in #923
* Fix using the dag as a keyword argument as ``specific_args_keys`` in DbtTaskGroup by @tboutaour in #916
* Fix ACI integration (``DbtAzureContainerInstanceBaseOperator``) by @danielvdende in #872
* Fix setting dbt project dir to the tmp dir by @dwreeves in #873
* Fix dbt docs operator to not use ``graph.gpickle`` file when ``--no-write-json`` is passed by @dwreeves in #883
* Make Pydantic a required dependency by @pankajkoti in #939
* Gracefully error if users try to ``emit_datasets`` with ``Airflow 2.9.0`` or ``2.9.1`` by @tatiana in #948
* Fix parsing tests that have no parents in #933 by @jlaneve
* Correct ``root_path`` in partial parse cache by @pankajkoti in #950

Docs

* Fix docs homepage link by @jlaneve in #860
* Fix docs ``ExecutionConfig.dbt_project_path`` by @jbandoro in #847
* Fix typo in MWAA getting started guide by @jlaneve in #846
* Fix typo related to exporting docs to GCS by @tboutaour in #922
* Improve partial parsing docs by @tatiana in #898
* Improve docs for datasets for airflow >= 2.4 by @SiddiqueAhmad in #879
* Improve test behaviour docs to highlight ``warning`` feature in the ``virtualenv`` mode by @mc51 in #910
* Fix docs typo by @SiddiqueAhmad in #917
* Improve Astro docs by @RNHTTR in #951

Others

* Add performance integration tests by @jlaneve in #827
* Enable ``append_env`` in ``operator_args`` by default by @tatiana in #899
* Change default ``append_env`` behaviour depending on Cosmos ``ExecutionMode`` by @pankajkoti and @pankajastro in #954
* Expose the ``dbt`` graph in the ``DbtToAirflowConverter`` class by @tommyjxl in #886
* Improve dbt docs plugin rendering padding by @dwreeves in #876
* Add ``connect_retries`` to databricks profile to fix expensive integration failures by @jbandoro in #826
* Add import sorting (isort) to Cosmos by @jbandoro in #866
* Add Python 3.11 to CI/tests by @tatiana and @jbandoro in #821, #824 and #825
* Fix failing ``test_created_pod`` for ``apache-airflow-providers-cncf-kubernetes`` after v8.0.0 update by @jbandoro in #854
* Extend ``DatabricksTokenProfileMapping`` test to include session properties by @tatiana in #858
* Fix broken integration test uncovered from Pytest 8.0 update by @jbandoro in #845
* Add Apache Airflow 2.9 to the test matrix by @tatiana in #940
* Replace deprecated ``DummyOperator`` by ``EmptyOperator`` if Airflow >=2.4.0 by @tatiana in #900
* Improve logs to troubleshoot issue in 1.4.0a2 with astro-cli by @tatiana in #947
* Fix issue when publishing a new release to PyPI by @tatiana in #946
* Pre-commit hook updates in #820, #834, #843 and #852, #890, #896, #901, #905, #908, #919, #931, #941


1.3.2 (2024-01-26)
------------------

Bug fixes

* Fix: ensure ``DbtGraph.update_node_dependency`` is called for all load methods by @jbandoro in #803
* Fix: ensure operator ``execute`` method is consistent across all execution base subclasses by @jbandoro in #805
* Fix custom selector when ``test`` node has no ``depends_on`` values by @tatiana in #814
* Fix forwarding selectors to test task when using ``TestBehavior.AFTER_ALL`` by @tatiana in #816

Others

* Docs: Remove incorrect docstring from ``DbtLocalBaseOperator`` by @jakob-hvitnov-telia in #797
* Add more logs to troubleshoot custom selector by @tatiana in #809
* Fix OpenLineage integration documentation by @tatiana in #810
* Fix test dependencies after Airflow 2.8 release by @jbandoro and @tatiana in #806
* Use Airflow constraint file for test environment setup by @jbandoro in #812
* pre-commit updates in #799, #807


1.3.1 (2023-01-10)
------------------

Bug fixes

* Fix disable event tracking throwing error by @jbandoro in #784
* Fix support for string path for ``LoadMode.DBT_LS_FILE`` and docs by @flinz in #788
* Remove stack trace to disable unnecessary K8s error by @tatiana in #790

Others

* Update examples to use the astro-runtime 10.0.0 by @RNHTTR in #777
* Docs: add missing imports for mwaa getting started by @Benjamin0313 in #792
* Refactor common executor constructors with test coverage by @jbandoro in #774
* pre-commit updates in #789


1.3.0 (2023-01-04)
------------------

Features

* Add new parsing method ``LoadMode.DBT_LS_FILE`` by @woogakoki in #733 (`documentation <https://astronomer.github.io/astronomer-cosmos/configuration/parsing-methods.html#dbt-ls-file>`_).
* Add support to select using (some) graph operators when using ``LoadMode.CUSTOM`` and ``LoadMode.DBT_MANIFEST`` by @tatiana in #728 (`documentation <https://astronomer.github.io/astronomer-cosmos/configuration/selecting-excluding.html#using-select-and-exclude>`_)
* Add support for dbt ``selector`` arg for DAG parsing by @jbandoro in #755 (`documentation <https://astronomer.github.io/astronomer-cosmos/configuration/render-config.html#render-config>`_).
* Add ``ProfileMapping`` for Vertica by @perttus in #540, #688 and #741 (`documentation <https://astronomer.github.io/astronomer-cosmos/profiles/VerticaUserPassword.html>`_).
* Add ``ProfileMapping`` for Snowflake encrypted private key path by @ivanstillfront in #608 (`documentation <https://astronomer.github.io/astronomer-cosmos/profiles/SnowflakeEncryptedPrivateKeyFilePem.html>`_).
* Add support for Snowflake encrypted private key environment variable by @DanMawdsleyBA in #649
* Add ``DbtDocsGCSOperator`` for uploading dbt docs to GCS by @jbandoro in #616, (`documentation <https://astronomer.github.io/astronomer-cosmos/configuration/generating-docs.html#upload-to-gcs>`_).
* Add cosmos/propagate_logs Airflow config support for disabling log propagation by @agreenburg in #648 (`documentation <https://astronomer.github.io/astronomer-cosmos/configuration/logging.html>`_).
* Add operator_args ``full_refresh`` as a templated field by @joppevos in #623
* Expose environment variables and dbt variables in ``ProjectConfig`` by @jbandoro in #735 (`documentation <https://astronomer.github.io/astronomer-cosmos/configuration/project-config.html#project-config-example>`_).
* Support disabling event tracking when using Cosmos profile mapping by @jbandoro in #768 (`documentation <https://astronomer.github.io/astronomer-cosmos/profiles/index.html#disabling-dbt-event-tracking>`_).

Enhancements

* Make Pydantic an optional dependency by @pixie79 in #736
* Create a symbolic link to ``dbt_packages`` when ``dbt_deps`` is False when using ``LoadMode.DBT_LS`` by @DanMawdsleyBA in #730
* Add ``aws_session_token`` for Athena mapping by @benjamin-awd in #663
* Retrieve temporary credentials from ``conn_id`` for Athena by @octiva in #758
* Extend ``DbtDocsLocalOperator`` with static flag by @joppevos  in #759

Bug fixes

* Remove Pydantic upper version restriction so Cosmos can be used with Airflow 2.8 by @jlaneve in #772

Others

* Replace flake8 for Ruff by @joppevos in #743
* Reduce code complexity to 8 by @joppevos in #738
* Speed up integration tests by @jbandoro in #732
* Fix README quickstart link in by @RNHTTR in #776
* Add package location to work with hatchling 1.19.0 by @jbandoro in #761
* Fix type check error in ``DbtKubernetesBaseOperator.build_env_args`` by @jbandoro in #766
* Improve ``DBT_MANIFEST`` documentation by @dwreeves in #757
* Update conflict matrix between Airflow and dbt versions by @tatiana in #731 and #779
* pre-commit updates in #775, #770, #762


1.2.5 (2023-11-23)
------------------

Bug fixes

* Fix running models that use alias while supporting dbt versions by @binhnq94 in #662
* Make ``profiles_yml_path`` optional for ``ExecutionMode.DOCKER`` and ``KUBERNETES`` by @MrBones757 in #681
* Prevent overriding dbt profile fields with profile args of "type" or "method" by @jbandoro in #702
* Fix ``LoadMode.DBT_LS`` fail when dbt outputs ``WarnErrorOptions`` by @adammarples in #692
* Add support for env vars in ``RenderConfig`` for dbt ls parsing by @jbandoro in #690
* Add support for Kubernetes ``on_warning_callback`` by @david-mag in #673
* Fix ``ExecutionConfig.dbt_executable_path`` to use ``default_factory`` by @jbandoro in #678

Others

* Docs fix: example DAG in the README and docs/index by @tatiana in #705
* Docs improvement: highlight DAG examples in README by @iancmoritz and @jlaneve in #695


1.2.4 (2023-11-14)
------------------

Bug fixes

* Store ``compiled_sql`` even when task fails by @agreenburg in #671
* Refactor ``LoadMethod.LOCAL`` to use symlinks instead of copying directory by @jbandoro in #660
* Fix 'Unable to find the dbt executable: dbt' error by @tatiana in #666
* Fix installing deps when using ``profile_mapping`` & ``ExecutionMode.LOCAL`` by @joppevos in #659

Others

* Docs: add execution config to MWAA code example by @ugmuka in #674
* Docs: highlight DAG examples in docs by @iancmoritz and @jlaneve in #695


1.2.3 (2023-11-09)
------------------

Bug fix

* Fix reusing config across TaskGroups/DAGs by @tatiana in #664


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
