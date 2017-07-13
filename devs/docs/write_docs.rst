=====================
Writing Documentation
=====================

The docs live under the ``blackbox/docs`` directory.

The docs are written with `reStructuredText`_ and built with Sphinx_.

Line length must not exceed 80 characters (except for literals that cannot be
wrapped). Most text editors support automatic line breaks or hard wrapping at a
certain line width if you don't want to do this by hand.

To start working on the docs locally, you will need Python_ 3 in addition to
Java_ (needed for the doctests_). Make sure that ``python3`` is on your
``$PATH``.

Before you can get started, you need to bootstrap the docs::

    $ cd blackbox
    $ ./bootstrap.sh

Once this runs, you can build the docs and start the docs web server like so::

    $ ./bin/sphinx dev

Once the web server running, you can view your local copy of the docs by
visiting http://127.0.0.1:8000 in a web browser.

This command also watches the file system and rebuilds the docs when changes
are detected. Even better, it will automatically refresh the browser tab for
you.

Many of the examples in the documentation are executable and function as
doctests_.

You can run the doctests like so::

    $ ./bin/test

If you want to test the doctests in a specific file, run this::

    $ ./bin/test -1vt <filename>

There is also a Gradle task called ``itest`` which will execute all of the
above steps.

*Note*: Your network connection should be up and running, or some of the tests
will fail.

The docs are automatically built from Git by `Read the Docs`_ and there is
nothing special you need to do to get the live docs to update.


.. _reStructuredText: http://docutils.sourceforge.net/rst.html
.. _Sphinx: http://sphinx-doc.org/
.. _Java: http://www.java.com/
.. _Python: http://www.python.org/
.. _doctests: http://www.sphinx-doc.org/en/stable/ext/doctest.html
.. _Read the Docs: http://readthedocs.org
