Cosmos Contributing Guide
=========================

All contributions, bug reports, bug fixes, documentation improvements, enhancements are welcome.

As contributors and maintainers to this project, you are expected to abide by the
`Contributor Code of Conduct <https://github.com/astronomer/astronomer-cosmos/blob/main/CODE_OF_CONDUCT.md>`_.

Overview
________

To contribute to the cosmos project:

#. Please create a `GitHub Issue <https://github.com/astronomer/astronomer-cosmos/issues>`_ describing your contribution
#. Open a feature branch off of the ``main`` branch and create a Pull Request into the ``main`` branch from your feature branch
#. Link your issue to the pull request
#. Once developments are complete on your feature branch, request a review and it will be merged once approved.


Using Hatch for local development
---------------------------------

We currently use :ref:`hatch <https://github.com/pypa/hatch>` for building and distributing ``astronomer-cosmos``.

The tool can also be used for local development. The :ref:`pyproject.toml <https://github.com/astronomer/astronomer-cosmos/blob/main/pyproject.toml>` file currently defines a matrix of supported versions of Python and Airflow for which a user can run the tests against.

For instance, to run the tests using Python 3.10 and Apache Airflow 2.5, use the following:

.. code-block:: bash

    hatch run tests.py3.10-2.5:test-cov

It is also possible to run the tests using all the matrix combinations, by using:

.. code-block:: bash

    hatch run tests:test-cov


Using Tilt for local development
________________________________

It is also possible to use `tilt <https://docs.tilt.dev>`_, a toolkit which helps on microservices local development.


Pre-requisites
++++++++++++++

#. `tilt <https://docs.tilt.dev>`_
#. `git <https://git-scm.com/book/en/v2/Getting-Started-Installing-Git>`_

Local Sandbox
+++++++++++++

For local development, we use `Tilt <https://docs.tilt.dev>`_. To use Tilt, first clone the ``astronomer-cosmos`` repo:

.. code-block:: bash

    git clone https://github.com/astronomer/astronomer-cosmos.git

Then, run the following from the ``astronomer-cosmos`` directory:

.. code-block:: bash

    tilt up

You can press ``space`` to open the Tilt UI and see the status of the sandbox. Once the sandbox is up, you can access the Airflow UI at ``http://localhost:8080``.


Pre-Commit
++++++++++

We use pre-commit to run a number of checks on the code before committing. To install pre-commit, run:

.. code-block:: bash

    pre-commit install

To run the checks manually, run:

.. code-block:: bash

    pre-commit run --all-files


Writing Docs
____________

You can run the docs locally by running the following:

.. code-block:: bash

    hatch run docs:serve


This will run the docs server in a virtual environment with the right dependencies. Note that it may take longer on the first run as it sets up the virtual environment, but will be quick on subsequent runs.


Building
________

We use ```hatch``` to build the project. To build the project, run:

.. code-block:: bash

    hatch build


Releasing
_________

We use GitHub actions to create and deploy new releases. To create a new release, first create a new version using:

.. code-block:: bash

    hatch version minor


```hatch``` will automatically update the version for you. Then, create a new release on GitHub with the new version. The release will be automatically deployed to PyPI.

.. note::
    You can update the version in a few different ways. Check out the `hatch docs <https://hatch.pypa.io/latest/version/#updating>`_ to learn more.
