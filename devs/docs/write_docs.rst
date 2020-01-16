=====================
Writing Documentation
=====================

The docs live under the ``blackbox/docs`` directory.

The docs are written with `reStructuredText`_ and built with `Sphinx`_.

Line length must not exceed 80 characters (except for literals that cannot be
wrapped). Most text editors support automatic line breaks or hard wrapping at a
particular line width if you don't want to do this by hand.

To start working on the docs locally, you will need `Python`_ 3 in addition to
`Java`_ (required for the `doctests`_). Make sure that ``python3`` is on your
``$PATH``.


Sphinx (basic)
==============

You can build the docs without setting up your Java environment or running the
doctests.

Change into the ``blackbox`` directory::

    $ cd blackbox

Bootstrap the Python environment::

    $ ./bootstrap.sh

Then, run the Sphinx development server, like so::

    $ bin/sphinx dev

Once the webserver is running, you can view your local copy of the docs by
visiting http://127.0.0.1:8000 in a web browser.

This command watches the file system and rebuilds the docs when changes are
detected. When a change is detected, it will automatically refresh the browser
tab for you.

*Note*: The CrateDB docs include many `doctests`_. The method described above is
useful for previewing quick changes. However, to properly test the
documentation, you must follow the instructions in the next section, `Gradle
(including full doctests)`_.


Troubleshooting
---------------

If you run into something that looks like an issue with Python or a missing
library, you can usually resolve the issue by resetting your Python
environment.

First, remove the virtual environment directory::

    $ rm -rf .venv

Then, rerun the bootstrap script::

    $ ./bootstrap.sh


Gradle (including full doctests)
================================

Run the Sphinx development server, like so::

    $ ./gradlew :blackbox:developDocs

Once the webserver is running, you can view your local copy of the docs by
visiting http://127.0.0.1:8000 in a web browser.

This command watches the file system and rebuilds the docs when changes are
detected. When a change is detected, it will automatically refresh the browser
tab for you.

Alternatively, you can build the docs without starting the webserver. You may
want to do this if you want to test the docs without previewing your changes.
You can do so like this::

    $ ./gradlew :blackbox:buildDocs

Many of the examples in the documentation are executable and function as
`doctests`_.

You can run the doctests like so::

    $ ./gradlew :blackbox:itest

*Note*: Your network connection should be up and running, or some of the tests
will fail.

`Read the Docs`_ automatically builds and deploys the docs directly from Git,
and there is nothing special you need to do to get the live docs to update. We
do, however, use caching. If the caching appears to be broken, or you want to
force an update, speak to a sysadmin and ask them to clear the docs web cache
(we have a Jenkins job for this purpose).


.. _reStructuredText: http://docutils.sourceforge.net/rst.html
.. _Sphinx: http://sphinx-doc.org/
.. _Java: http://www.java.com/
.. _Python: http://www.python.org/
.. _doctests: http://www.sphinx-doc.org/en/stable/ext/doctest.html
.. _Read the Docs: http://readthedocs.org
