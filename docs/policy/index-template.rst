:orphan:

.. _index-template:

Index, overview, or landing page template
==========================================

Introduce the section, explaining why this section of the docs has been grouped together.

Most often, when you create a landing page, it will usually be an ``index.rst`` page and include the ``toctree`` to generate the section.::

    .. toctree::
       :hidden: # this controls whether or not the TOC is displayed on the index page. Remove if you want the TOC rendered.
       :caption: Name of the section #This is required for a subsection.
       :maxdepth: 2 # How many header levels appear in the TOC displayed on the index page.

        procedure-template # This way of calling the pages automatically uses the title of the page in the sidebar
        reference-template
        Index template <index-template.html> # When you use the ``label <link>`` format, the ``label` appears in the sidebar

If you are not creating a section landing page, like :ref:`execution-modes`, then it still follows similar conventions.

Next, introduce the subsections or explain the variety of topics.


Section caption
~~~~~~~~~~~~~~~~

If the section is large enough, it might make sense to group content on the landing page into sections, with headers that reflect the ``toctree`` caption they relate to.

Then, instead of listing all the links, you can call the ``toctree``, without the ``:hidden:`` parameter.

See the :ref:`getting-started` index as an example.
