:orphan:

.. _index-template:

Index, overview, or landing page template
==========================================

Introduce the section, explaining why this section of the docs has been grouped together.

Most often, when you create a landing page, it will usually be an ``index.rst`` page and include the ``toctree`` to generate the section:

.. code-block:: rst

    .. toctree::
       :hidden:
       :caption: Name of the section
       :maxdepth: 2

       procedure-template
       reference-template
       Index template <index-template.html>

The ``toctree`` options and entries above mean:

- ``:hidden:`` controls whether the table of contents is rendered on the index page itself. Remove it if you want the TOC displayed inline.
- ``:caption:`` is the section title shown in the sidebar, and is required for a subsection.
- ``:maxdepth:`` sets how many header levels appear in the generated TOC.
- Listing a page by name (for example ``procedure-template``) automatically uses that page's title in the sidebar.
- Using the ``label <link>`` format (for example ``Index template <index-template.html>``) shows ``label`` in the sidebar instead of the page title.

If you are not creating a section landing page, like :ref:`execution-modes`, then it still follows similar conventions.

Next, introduce the subsections or explain the variety of topics.


Section caption
~~~~~~~~~~~~~~~~

If the section is large enough, it might make sense to group content on the landing page into sections, with headers that reflect the ``toctree`` caption they relate to.

Then, instead of listing all the links, you can call the ``toctree``, without the ``:hidden:`` parameter.

See the :ref:`getting_started` index as an example.
