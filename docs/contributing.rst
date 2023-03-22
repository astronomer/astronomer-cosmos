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

Creating a Sandbox to Test Changes
__________________________________

Pre-requisites
**************
#. `Astro CLI <https://docs.astronomer.io/astro/cli/install-cli>`_
#. `git <https://git-scm.com/book/en/v2/Getting-Started-Installing-Git>`_

Local Sandbox
************
To create a sandbox where you can do real-time testing for your proposed to changes to Cosmos, see the corresponding
development repository: `cosmos-dev <https://github.com/astronomer/cosmos-dev>`_.

Pre-Commit
************

We use pre-commit to run a number of checks on the code before committing. To install pre-commit, run the following from
your cloned ``astronomer-cosmos`` directory:

.. code-block:: bash

    python3 -m venv venv
    source venv/bin/activate
    pip install -r dev-requirements.txt
    pip install pre-commit
    pre-commit install


To run the checks manually, run:

.. code-block:: bash

    pre-commit run --all-files


Writing Docs
__________________________________

You can run the docs locally by running the following:

.. code-block:: bash

    hatch run docs:serve


This will run the docs server in a virtual environment with the right dependencies. Note that it may take longer on the first run as it sets up the virtual environment, but will be quick on subsequent runs.


Building
__________________________________

We use `hatch <https://hatch.pypa.io/latest/>`_ to build the project. To build the project, run:

.. code-block:: bash

    hatch build


Releasing
__________________________________

We use GitHub actions to create and deploy new releases. To create a new release, first create a new version using:

.. code-block:: bash

    hatch version minor


hatch will automatically update the version for you. Then, create a new release on GitHub with the new version. The release will be automatically deployed to PyPI.

.. note::
    You can update the version in a few different ways. Check out the `hatch docs <https://hatch.pypa.io/latest/version/#updating>`_ to learn more.
