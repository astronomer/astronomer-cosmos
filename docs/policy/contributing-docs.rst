.. _contributing-docs:

Contribute docs
================

Information about how to get started writing or updating docs, including specifics about Sphinx styling conventions.

Writing Docs
~~~~~~~~~~~~

After following the steps described in :ref:`setting-up-hatch`, you are ready to build and serve the documentation locally.

You can run the docs locally by running the following:

.. code-block:: bash

    hatch run docs:serve

If you want to run docs locally without auto-reload, which can sometimes make it difficult to identify build errors, you can use:

.. code-block:: bash

    hatch run docs:serve-no-reload

Use a docs template
~~~~~~~~~~~~~~~~~~~~

Need some help getting started writing a new docs page?

Check out the following:

- :ref:`index-template`
- :ref:`reference-template`
- :ref:`procedure-docs-template`

Cosmos style guide
~~~~~~~~~~~~~~~~~~

Apache Airflow®
++++++++++++++++

Cosmos docs follows the `Apache Airflow® <https://airflow.apache.org/>`_ registered trademark usage conventions.

- Use the full name and link to the Airflow site on the first mention on the page::

    `Apache Airflow® <https://airflow.apache.org/>`_

- Use ``Apache Airflow®`` in headers and titles.

The `Airflow docs <https://airflow.apache.org/docs/>`_ provides in-context usage examples, and you can read more about the `registered trademark use <https://lists.apache.org/thread/0fmp9olljfkm3vmg3ns5qd1o95sm7235>`_.

Code formatting
++++++++++++++++++

When code elements of Cosmos are mentioned in-text, for example, the ``ExecutionConfig``, ``ProfileConfig``, ``ProjectConfig``, and ``RenderConfig``, format as code.

Dags
++++

Cosmos follows the same capitalization conventions as the Airflow project: ``Dag``.

Open-source focus
+++++++++++++++++

When providing a list of options, aim to share open-source options, alphabetically, followed by commercial options, alphabetically.

Sphinx style guide
~~~~~~~~~~~~~~~~~~~

The Cosmos docs use Sphinx for documentation. Sphinx is highly extensible, and permits a variety of conventions for style formatting.
This makes writing Sphinx docs quick, but when working collaboratively, can lead to variations in convention that can make it confusing.

Sphinx primer
+++++++++++++

Sphinx is a markup language that has particular conventions that are different from Markdown.

The `reStructured Text primer <https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html>`_ is a helpful resource that covers general formatting and styling conventions.

Bullet points
+++++++++++++

Sphinx accepts both ``-`` and ``*`` for bullet points, but Cosmos docs preferentially use ``-``, because ``*`` can be used in the formatting for bold and italics.

.. code-block:: text

    *Italics*

    **Bold**

    - Bullet point


.. code-block:: text

    Sometimes, nesting and indentation can cause autoformatting because Sphinx is white-space sensitive.

    - Bolded bullet point text (autoformatted)
      - Normal weight, nested bullet point text (autoformatted)


- Bolded bullet point text (autoformatted)
    - Normal weight, nested bullet point text (autoformatted)


Diagrams
++++++++

Cosmos uses `Mermaid <https://mermaid.js.org/>`_ to create diagrams within the docs with the `sphinxcontrib-mermaid <https://sphinxcontrib-mermaid-demo.readthedocs.io/en/latest/>`_ extension.


Headers
++++++++

Unlike Markdown, Sphinx allows for many symbols to be used to format header levels, and mostly works within the same page.

.. code-block:: text

    Page title
    ===========

     Uses ``=```

    Header 1
    ~~~~~~~~~

     Uses ``~``

    Header 2
    +++++++++

    Header 3
    ^^^^^^^^^


Image formatting
++++++++++++++++

Cosmos docs use the ``image`` tag to call figures and diagrams.

.. tip::

    Be sure to add alt-text to your images! If you're not familiar with writing alt-text, `WebAIM <https://webaim.org/techniques/alttext>`_ offers an overview.

.. code-block:: text

    .. image:: ../_static/image.png
       :alt: Add the image description.

Adding images and figures
^^^^^^^^^^^^^^^^^^^^^^^^^

For resolution purposes, try to use image files that are ``.svg`` when possible.
However, ``.png`` at sufficient resolution usually provides adequate quality.


Links
++++++

How you link to material within the docs, and how you link externally are different.

- **Internal links**: Use the ``ref`` attribute. If you need the link to appear with a title different from the page title, use the structure::

    :ref:`link title <target>`

- **External links**: Use::

    `Link title <link target.html>`_


Redirects
++++++++++++++++++++

Cosmos uses the `Sphinx reredirect package <https://documatt.com/sphinx-reredirects/>`_ to manage redirects. When you update a page directory location, ``ref`` label, or file name, you also need to add a redirect.

1. Open the ``docs/conf.py`` file and go to the ``Begin docs redirect section``. The redirect section is structured as ``source:target`` pairs, where the old URL is the source and new URL is the target.
- This section is organized **Alphabetically** by **source**, because multiple sources might redirect to the same **target**.
2. **Sources** are formatted without the file-type. **Targets** must prepend the filepath with ``../`` and append ``.html`` to the end.

For example, if we wanted to redirect this file, ``contributing-docs.rst`` to the main ``contributing.rst`` page, the syntax would be:

.. code-block::

    "policy/contributing-docs": "../policy/contributing.html",


3. To test locally if your redirects look, paste ``/policy/contributing-docs.html`` after the localhost address.

Sidebar generation and formatting
++++++++++++++++++++++++++++++++++

Sphinx uses the ``toctree`` directive to generate indices instead of using a central navigation file.

The Cosmos docs contain subfolders and subdirectories, which appear with and without a collapsible sidebar menu, using the `Pydata theme <https://pydata-sphinx-theme.readthedocs.io/en/stable/user_guide/navigation.html>`_.

For example, the following sidebar example from the Cosmos Docs Guides can be represented as::

    |-docs
       ├── guides
            ├── Set up dbt with Airflow
                ├── dbt fusion support
                └── Airflow and dbt dependencies conflicts
            ├── ...
            ├── How Cosmos runs dbt
                ├── ...
                ├── Callbacks
                ├── Operators
                └── Additional customization
                    ├── Callbacks
                    ├── Operators
                    └── Additional customization


This sidebar is rendered based on a combination of the file directory structure and the contents of different directory and
subdirectory ``index.rst`` contents. It creates a series of nested index pages. For example, the **Set up dbt with Airflow** section in this example does not collapse in the sidebar.
However, the **How Cosmos runs dbt** does.

The file directory to generate this menu::

    |-docs
       ├── guides/
            ├── index.rst
            ├──/dbt_setup (sidebar label generated from guides/index.rst toctree caption)
                ├── dbt-fusion.rst
                └── execution-modes-local-conflicts.rst
            ├── ...
            ├── /run_dbt (sidebar label generated from guides/index.rst toctree caption)
                ├── ...
                ├── callbacks/
                    └── callbacks.rst
                ├── operators/
                    └── operators.rst
                └── customization/
                    ├── index.rst (contains toctree)
                    ├── scheduling.rst
                    └── ...


Limitations
^^^^^^^^^^^

Generating the nested table of contents has some limitations, some of which are undocumented.

- All ``toctree`` with collapsible subfolder nesting must include a ``caption``.
- If a subfolder only has one entry in its ``toctree``, it will not display as a collapsible option.
- Your subfolder index page must have a title for it to render correctly as a collapsible option.


