We use pre-commit to run a number of checks on the code before committing. To install pre-commit, run:

.. code-block:: bash

    python3 -m venv venv
    source venv/bin/activate
    pip install -r dev-requirements.txt
    pip install pre-commit
    pre-commit install


To run the checks manually, run:

.. code-block:: bash

    pre-commit run --all-files
