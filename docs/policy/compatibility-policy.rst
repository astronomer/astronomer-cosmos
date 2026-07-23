.. _compatibility-policy:

Compatibility policy
====================

This document outlines Astronomer Cosmos's compatibility policy for Python,
Apache Airflow, and dbt Core versions. This policy provides transparency and
predictability for users and contributors regarding version support and removal
criteria.

Overview
~~~~~~~~

Astronomer Cosmos is committed to maintaining compatibility with a range of
Python, Apache Airflow, and dbt Core versions to support diverse user
environments. We remove support for versions when they reach End of Life (EOL)
or when our dependencies no longer support them.

This policy establishes:

- Currently supported versions for Python, Apache Airflow, and dbt Core
- Version removal criteria based on End of Life (EOL) and dependency support
- User guidance for planning upgrades and checking compatibility
- Contributor guidance for proposing version changes

Currently supported versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following versions are currently tested and supported.

Python
++++++

- **Minimum required**: Python 3.10
- **Supported versions**: 3.10, 3.11, 3.12, 3.13, 3.14

Apache Airflow
++++++++++++++

New minor or major releases of Cosmos may drop support for Apache Airflow versions that have reached **End of Basic Support**, as defined in the `Astro Runtime Lifecycle schedule <https://www.astronomer.io/docs/runtime/runtime-version-lifecycle-policy#astro-runtime-lifecycle-schedule>`_.

In some cases, Cosmos may continue to support older Airflow versions, depending on the Cosmos release cycle.

- **Minimum required version**: Apache Airflow 2.9.0
- **Supported versions**: 2.9, 2.10, 2.11, 3.0, 3.1, 3.2, 3.3

dbt Core
++++++++

- **Supported versions**: 1.5, 1.6, 1.7, 1.8, 1.9, 1.10, 1.11, 1.12, 2.0 (dbt Fusion)

.. note::

   Specific dbt adapter versions may have additional compatibility requirements.
   Refer to the `dbt documentation <https://docs.getdbt.com/docs/supported-data-platforms>`_
   for adapter-specific version compatibility.

**Verified Python × dbt Core support.** The declared range above is what
Cosmos tests in CI; whether a given dbt Core release actually installs and
imports on a given Python version is a property of dbt Core itself, not of
Cosmos. The table below reflects targeted verification — ``pip install``
into a plain Python image, then ``import dbt.cli.main`` — checked
2026-07-23:

.. list-table::
   :header-rows: 1

   * - dbt Core
     - 3.10
     - 3.11
     - 3.12
     - 3.13
     - 3.14
   * - 1.5 – 1.7
     - OK
     - OK
     - fails
     - fails
     - fails
   * - 1.8 – 1.11
     - OK
     - OK
     - OK
     - OK
     - fails
   * - 1.12
     - OK
     - OK
     - OK
     - OK
     - OK
   * - 2.0 (Fusion)
     - n/a
     - n/a
     - n/a
     - n/a
     - n/a

- dbt Core 1.5–1.7 fail on Python 3.12+ with ``ModuleNotFoundError: No
  module named 'distutils'`` — Python 3.12 removed the stdlib ``distutils``
  module those releases still import.
- dbt Core 1.8–1.11 fail on Python 3.14 inside ``dbt_common``'s
  ``mashumaro`` dependency (e.g. ``mashumaro.exceptions.UnserializableField``),
  which hasn't caught up to Python 3.14's typing changes.
- dbt Fusion (``2.0``) is **not** installed via ``pip install dbt-core==2.0``
  — there is no such release on PyPI. Fusion ships as a separate
  binary/installer; this table's pip-based check does not apply to it.

.. note::

   For the authoritative, always-current Python support of a specific
   release, check the package's PyPI project page — e.g. `dbt-core
   <https://pypi.org/project/dbt-core/>`_ or `astronomer-cosmos
   <https://pypi.org/project/astronomer-cosmos/>`_ — and look at that
   release's ``Requires-Python`` metadata and classifiers. The table above
   is a point-in-time verification, not a substitute for checking the
   package you're about to install.

Astronomer Runtime
+++++++++++++++++++

Astronomer Runtime bundles a specific Apache Airflow version with a supported
Python range. Because Cosmos's supported Airflow and Python ranges above track
Astronomer Runtime, any Runtime series still in **Maintenance** or **Basic
Support** is compatible with the current Cosmos release — provided the dbt
Core version you pair it with also supports that Python version (see the
verified Python × dbt Core table above).

The table below reflects Runtime series still in Maintenance or Basic Support
as of 2026-07-23, per the `Astronomer Runtime lifecycle policy
<https://www.astronomer.io/docs/runtime/runtime-version-lifecycle-policy>`_.
Runtime ships new patch and minor versions frequently — treat this table as a
snapshot and check the `Runtime release notes
<https://www.astronomer.io/docs/runtime/runtime-release-notes>`_ and lifecycle
policy pages directly for the current state.

.. list-table::
   :header-rows: 1

   * - Runtime series
     - Airflow
     - Python (bundled by Runtime)
   * - 3.3
     - 3.3
     - 3.12 – 3.14
   * - 3.2
     - 3.2
     - 3.12 – 3.14
   * - 3.1
     - 3.1
     - 3.11 – 3.12
   * - 3.0
     - 3.0
     - 3.11 – 3.12
   * - 13
     - 2.11
     - 3.10 – 3.12

.. note::

   Runtime 11 (Airflow 2.9) and Runtime 12 (Airflow 2.10) are omitted above
   because their Basic Support windows have already closed. Cosmos still
   supports Airflow 2.9/2.10 per the matrix above for users pinned to those
   Runtime series, but new Astro deployments can no longer be created on them.

.. note::

   Cosmos's dbt Core test matrix runs the full 1.5–1.12 range across every
   supported Airflow/Python combination only for dbt 1.12; older dbt minors
   are cross-tested against a narrower subset of Airflow/Python pairs (see
   `.github/workflows/test.yml
   <https://github.com/astronomer/astronomer-cosmos/blob/main/.github/workflows/test.yml>`_
   for the exact combinations). If you rely on an older dbt Core minor with an
   uncommon Airflow/Python pair, validate it in your own CI.

Cosmos and Astronomer Runtime compatibility (verified)
++++++++++++++++++++++++++++++++++++++++++++++++++++++

The ranges above describe what Cosmos declares and tests in CI. The table
below reflects targeted, empirical verification instead: each Cosmos release
from 1.5.1 to 1.15.0 was ``pip install``-ed into the official Astronomer
Runtime Docker image for each Runtime series, then checked by running
``airflow plugins`` — the same plugin-loading path Airflow itself uses at
startup — rather than a bare ``import cosmos``. Verified 2026-07-23 against
Runtime images 11.20.0, 12.12.0, 13.8.0, 3.0-16, 3.1-17, 3.2-6, and 3.3-2.

The 13 Cosmos releases tested, oldest to newest: 1.5.1, 1.6.0, 1.7.1, 1.8.2,
1.9.2, 1.10.3, 1.11.3, 1.12.1, 1.13.1, 1.14.0, 1.14.1, 1.14.2, 1.15.0.

.. list-table::
   :header-rows: 1

   * - Runtime series
     - Airflow
     - Cosmos versions confirmed working
     - Cosmos versions confirmed failing
   * - 11, 12, 13
     - 2.9, 2.10, 2.11
     - All 13 tested releases (1.5.1 – 1.15.0)
     - None, except on Runtime 11's ``python-3.9`` variant — see below.
   * - 3.0
     - 3.0
     - None
      - All 13 tested releases (1.5.1 – 1.15.0).
   * - 3.1
     - 3.1
     - 1.11.3, 1.12.1, 1.13.1, 1.14.0, 1.14.1, 1.14.2, 1.15.0
     - 1.5.1, 1.6.0, 1.7.1, 1.8.2, 1.9.2, 1.10.3 — see below.
   * - 3.2, 3.3
     - 3.2, 3.3
     - 1.14.0, 1.14.1, 1.14.2, 1.15.0
     - 1.5.1, 1.6.0, 1.7.1, 1.8.2, 1.9.2, 1.10.3, 1.11.3, 1.12.1, 1.13.1 — see
       below.

- **Runtime 3.1, Cosmos 1.5.1–1.9.2** fail with an explicit plugin-import
  error:

  .. code-block:: text

     ERROR - Failed to import plugin cosmos
     ImportError: cannot import name 'context_to_airflow_vars' from 'airflow...'

  **Cosmos 1.10.3** also fails on Runtime 3.1, but silently: no traceback is
  printed and the ``cosmos`` entry is simply absent from ``airflow plugins``
  output.

- **Runtime 3.2 / 3.3, Cosmos 1.5.1** fails with:

  .. code-block:: text

     AttributeError: partially initialized module 'cosmos.plugin' has no
     attribute 'CosmosPlugin' (most likely due to a circular import)

- **Runtime 3.2 / 3.3, Cosmos 1.6.0–1.13.1** crash the entire ``airflow``
  process — not only plugin loading — during Airflow's own provider-metadata
  discovery, which every ``airflow`` invocation runs at startup:

  .. code-block:: text

     File ".../cosmos/settings.py", line 8, in <module>
         from airflow.configuration import conf
     ImportError: cannot import name 'conf' from partially initialized module
     'airflow.configuration' (most likely due to a circular import)

  The cycle: Cosmos's ``apache_airflow_provider`` entry point imports
  ``cosmos.settings``, which imports ``airflow.configuration`` while Airflow
  is still in the middle of initializing that same module. Confirmed to
  reproduce from a bare ``python -c "import airflow"``, so this is not
  specific to plugins or to the ``airflow`` CLI — any process that imports
  Airflow with one of these Cosmos versions installed fails to start.

- **Cosmos >= 1.12.0 requires Python >= 3.10.** On Python 3.9 (e.g. Runtime
  11's ``python-3.9`` variant), ``pip install`` itself refuses to resolve —
  a plain dependency-version error, not a circular import.

Version removal policy
~~~~~~~~~~~~~~~~~~~~~~

Cosmos removes support for versions based on clear and objective criteria.
There is no deprecation period—versions are removed when they meet the removal
criteria.

Python version removal criteria
+++++++++++++++++++++++++++++++

Python versions are removed from support when **any** of the following
conditions are met:

- **Python EOL**: The Python version has reached End of Life according to the
  `Python release schedule <https://devguide.python.org/versions/>`_
- **Airflow incompatibility**: Apache Airflow no longer supports the Python
  version
- **Astronomer Runtime incompatibility**: Astronomer Runtime no longer supports
  the Python version

Apache Airflow version removal criteria
+++++++++++++++++++++++++++++++++++++++

Apache Airflow versions are removed from support when **both** of the
following conditions are met:

- **Airflow EOL**: The Apache Airflow version has reached End of Life according
  to the
  `Apache Airflow release policy <https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html>`_
- **Astronomer Runtime EOL**: The Astronomer Runtime version has reached End of
  Basic Support according to the
  `Astronomer Runtime lifecycle policy <https://www.astronomer.io/docs/runtime/runtime-version-lifecycle-policy>`_

dbt Core version removal criteria
+++++++++++++++++++++++++++++++++

dbt Core versions are removed from support when:

- **dbt Core EOL**: The dbt Core version has reached End of Life according to the
  `dbt Labs support policy <https://docs.getdbt.com/docs/dbt-versions/core>`_

Version removal process
~~~~~~~~~~~~~~~~~~~~~~~

When a version meets the removal criteria:

1. **Evaluation**: The maintainer team verifies that the version meets all
   removal criteria
2. **Documentation**: This compatibility policy document is updated to reflect
   the removal
3. **CHANGELOG**: The removal is documented in ``CHANGELOG.rst``
4. **Removal**: Support is removed from the test matrix and CI/CD pipeline in
   the next release

User guidance
~~~~~~~~~~~~~

Checking compatibility
++++++++++++++++++++++

Before upgrading Cosmos or its dependencies:

1. **Check this document**: Review the currently supported versions
2. **Review CHANGELOG.rst**: Look for version removal notices in recent releases
3. **Test in development**: Test your workflows with new versions before
   upgrading production
4. **Monitor GitHub issues and releases**: Check for known compatibility issues
   or migration guides

Planning upgrades
+++++++++++++++++

To minimize disruption:

- **Stay current**: Regularly update to supported versions within the same major
  version
- **Monitor EOL dates**: Track EOL dates for Python, Airflow, and dbt Core to
  anticipate removals
- **Plan ahead**: Allocate time for testing and migration before versions reach
  EOL
- **Use CI/CD**: Test compatibility in your CI/CD pipeline before deploying

Reporting compatibility issues
++++++++++++++++++++++++++++++

If you encounter compatibility issues:

1. **Check existing issues**: Search
   `GitHub issues <https://github.com/astronomer/astronomer-cosmos/issues>`_
   for similar reports
2. **Create an issue**: Provide details about versions, error messages, and
   reproduction steps
3. **Include versions**: Specify Python, Airflow, dbt Core, and Cosmos versions
   in your report

Contributor guidance
~~~~~~~~~~~~~~~~~~~~

Proposing version changes
+++++++++++++++++++++++++

When proposing to add or remove version support:

1. **Create a GitHub issue**: Start a discussion about the proposed change

2. **Justify the change**:

   - For removal: Provide evidence that the version meets removal criteria

        (EOL announcement, dependency incompatibility, etc.)
   - For addition: Explain the benefit and compatibility with existing supported versions

3. **Assess impact**: Evaluate how many users might be affected

4. **Verify criteria**: Ensure removal proposals meet the criteria outlined in
   this policy

Updating test matrix
++++++++++++++++++++

When adding or removing versions:

1. **Update pyproject.toml**: Modify the test matrix in
   ``[tool.hatch.envs.tests.matrix]``
2. **Update CI workflows**: Ensure GitHub Actions workflows reflect the new
   matrix
3. **Update documentation**: Update this compatibility policy document
4. **Update CHANGELOG.rst**: Document the change in the changelog

Testing requirements
++++++++++++++++++++

All supported version combinations should:

- Pass unit tests
- Pass integration tests
- Be tested in the CI/CD pipeline
- Have example DAGs validated (where applicable)

How do I know when a version will be removed?
+++++++++++++++++++++++++++++++++++++++++++++

- **Monitor EOL dates**: Check the official EOL dates for Python, Apache Airflow,
  and dbt Core
- **Watch this document**: This compatibility policy is updated when versions
  are removed
- **Check CHANGELOG.rst**: Version removals are documented in release notes
- **Subscribe to releases**: Get notified of new releases that may include
  version removals

Related documentation
+++++++++++++++++++++

- `CHANGELOG.rst <https://github.com/astronomer/astronomer-cosmos/blob/main/CHANGELOG.rst>`_
  – Release notes and version removal notices
- `Python Release Schedule <https://devguide.python.org/versions/>`_
  – Python version EOL dates
- `Apache Airflow EOL Policy <https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html>`_
  – Airflow version EOL dates
- `dbt Core Versions <https://docs.getdbt.com/docs/dbt-versions/core>`_
  – dbt Core version support policy
- `Astronomer Runtime Version Lifecycle Policy <https://www.astronomer.io/docs/runtime/runtime-version-lifecycle-policy>`_
  – Astronomer Runtime version EOL dates
- `Astronomer Runtime Release Notes <https://www.astronomer.io/docs/runtime/runtime-release-notes>`_
  – Runtime-to-Airflow-to-Python version mappings
