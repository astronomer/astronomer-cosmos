.. _compatibility-policy:

Compatibility Policy
====================

This document outlines Astronomer Cosmos's compatibility policy for Python,
Apache Airflow, and dbt Core versions. This policy provides transparency and
predictability for users and contributors regarding version support and removal
criteria.

Overview
--------

Astronomer Cosmos is committed to maintaining compatibility with a range of
Python, Apache Airflow, and dbt Core versions to support diverse user
environments. We remove support for versions when they reach End of Life (EOL)
or when our dependencies no longer support them.

This policy establishes:

- Currently supported versions for Python, Apache Airflow, and dbt Core
- Version removal criteria based on End of Life (EOL) and dependency support
- User guidance for planning upgrades and checking compatibility
- Contributor guidance for proposing version changes

Currently Supported Versions
----------------------------

The following versions are currently tested and supported.

Python
~~~~~~

- **Minimum required**: Python 3.10
- **Supported versions**: 3.10, 3.11, 3.12, 3.13

Apache Airflow
~~~~~~~~~~~~~~

- **Minimum required**: Apache Airflow 2.6.0
- **Supported versions**: 2.6, 2.7, 2.8, 2.9, 2.10, 2.11, 3.0, 3.1

dbt Core
~~~~~~~~

- **Supported versions**: 1.5, 1.6, 1.7, 1.8, 1.9, 1.10, 1.11

.. note::

   Specific dbt adapter versions may have additional compatibility requirements.
   Refer to the `dbt documentation <https://docs.getdbt.com/docs/supported-data-platforms>`_
   for adapter-specific version compatibility.

Version Removal Policy
----------------------

Cosmos removes support for versions based on clear and objective criteria.
There is no deprecation period—versions are removed when they meet the removal
criteria.

Python Version Removal Criteria
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Python versions are removed from support when **any** of the following
conditions are met:

- **Python EOL**: The Python version has reached End of Life according to the
  `Python release schedule <https://devguide.python.org/versions/>`_
- **Airflow incompatibility**: Apache Airflow no longer supports the Python
  version
- **Astronomer Runtime incompatibility**: Astronomer Runtime no longer supports
  the Python version

Apache Airflow Version Removal Criteria
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Apache Airflow versions are removed from support when **either** of the
following conditions are met:

- **Airflow EOL**: The Apache Airflow version has reached End of Life according
  to the
  `Apache Airflow release policy <https://airflow.apache.org/docs/apache-airflow/stable/security/end-of-life.html>`_
- **Astronomer Runtime EOL**: The Astronomer Runtime version has reached End of
  Life according to the
  `Astronomer Runtime release policy <https://www.astronomer.io/docs/runtime/runtime-version-lifecycle-policy>`_

dbt Core Version Removal Criteria
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

dbt Core versions are removed from support when:

- **dbt Core EOL**: The dbt Core version has reached End of Life according to the
  `dbt Labs support policy <https://docs.getdbt.com/docs/dbt-versions/core>`_

Version Removal Process
-----------------------

When a version meets the removal criteria:

1. **Evaluation**: The maintainer team verifies that the version meets one or
   more removal criteria
2. **Documentation**: This compatibility policy document is updated to reflect
   the removal
3. **CHANGELOG**: The removal is documented in ``CHANGELOG.rst``
4. **Removal**: Support is removed from the test matrix and CI/CD pipeline in
   the next release

User Guidance
-------------

Checking Compatibility
~~~~~~~~~~~~~~~~~~~~~~

Before upgrading Cosmos or its dependencies:

1. **Check this document**: Review the currently supported versions
2. **Review CHANGELOG.rst**: Look for version removal notices in recent releases
3. **Test in development**: Test your workflows with new versions before
   upgrading production
4. **Monitor GitHub issues and releases**: Check for known compatibility issues
   or migration guides

Planning Upgrades
~~~~~~~~~~~~~~~~~

To minimize disruption:

- **Stay current**: Regularly update to supported versions within the same major
  version
- **Monitor EOL dates**: Track EOL dates for Python, Airflow, and dbt Core to
  anticipate removals
- **Plan ahead**: Allocate time for testing and migration before versions reach
  EOL
- **Use CI/CD**: Test compatibility in your CI/CD pipeline before deploying

Reporting Compatibility Issues
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you encounter compatibility issues:

1. **Check existing issues**: Search
   `GitHub issues <https://github.com/astronomer/astronomer-cosmos/issues>`_
   for similar reports
2. **Create an issue**: Provide details about versions, error messages, and
   reproduction steps
3. **Include versions**: Specify Python, Airflow, dbt Core, and Cosmos versions
   in your report

Contributor Guidance
--------------------

Proposing Version Changes
~~~~~~~~~~~~~~~~~~~~~~~~~~

When proposing to add or remove version support:

1. **Create a GitHub issue**: Start a discussion about the proposed change
2. **Justify the change**:
   - For removal: Provide evidence that the version meets removal criteria
     (EOL announcement, dependency incompatibility, etc.)
   - For addition: Explain the benefit and compatibility with existing supported
     versions
3. **Assess impact**: Evaluate how many users might be affected
4. **Verify criteria**: Ensure removal proposals meet the criteria outlined in
   this policy

Updating Test Matrix
~~~~~~~~~~~~~~~~~~~~

When adding or removing versions:

1. **Update pyproject.toml**: Modify the test matrix in
   ``[tool.hatch.envs.tests.matrix]``
2. **Update CI workflows**: Ensure GitHub Actions workflows reflect the new
   matrix
3. **Update documentation**: Update this compatibility policy document
4. **Update CHANGELOG.rst**: Document the change in the changelog

Testing Requirements
~~~~~~~~~~~~~~~~~~~~

All supported version combinations should:

- Pass unit tests
- Pass integration tests
- Be tested in the CI/CD pipeline
- Have example DAGs validated (where applicable)

How do I know when a version will be removed?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Monitor EOL dates**: Check the official EOL dates for Python, Apache Airflow,
  and dbt Core
- **Watch this document**: This compatibility policy is updated when versions
  are removed
- **Check CHANGELOG.rst**: Version removals are documented in release notes
- **Subscribe to releases**: Get notified of new releases that may include
  version removals

Related Documentation
--------------------

- `CHANGELOG.rst <https://github.com/astronomer/astronomer-cosmos/blob/main/CHANGELOG.rst>`_
  – Release notes and version removal notices
- `Python Release Schedule <https://devguide.python.org/versions/>`_
  – Python version EOL dates
- `Apache Airflow EOL Policy <https://airflow.apache.org/docs/apache-airflow/stable/security/end-of-life.html>`_
  – Airflow version EOL dates
- `dbt Core Versions <https://docs.getdbt.com/docs/dbt-versions/core>`_
  – dbt Core version support policy
- `Astronomer Runtime Version Lifecycle Policy <https://www.astronomer.io/docs/runtime/runtime-version-lifecycle-policy>`_
  – Astronomer Runtime version EOL dates
