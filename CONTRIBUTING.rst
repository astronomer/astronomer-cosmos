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
development repository: `astronomer-cosmos <https://github.com/astronomer/cosmos-dev>`.

Pre-Commit
__________

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
