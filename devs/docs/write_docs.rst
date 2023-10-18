===================
Write Documentation
===================

The docs live under the ``blackbox/docs`` directory.

The docs are written with `reStructuredText`_ and built with `Sphinx`_.

Line length must not exceed 79 characters (except for literals that cannot be
wrapped). Most text editors support automatic line breaks or hard wrapping at a
particular line width if you do not want to do this by hand.

To work on the docs locally, you will need `Python`_ 3 in addition to `Java`_
(required for the `doctests`_). Make sure that ``python3`` is on your ``$PATH``.

.. contents::


Run the Sphinx dev server
=========================

Run ``./blackbox/bin/sphinx dev``.

This bootstraps the python environment and launches ``sphinx-autobuild``, which
automatically rebuilds the docs when changes are detected.

Once the web server is running, you can view your local copy of the docs by
visiting http://127.0.0.1:8000 in a web browser.


Build the docs
==============

You can build the docs without starting the web server using::

    $ ./blackbox/bin/sphinx

The build output is located in `/docs/_out/html`.


Run the doctests
================

Many of the code examples in the documentation are executable and function as
`doctests`_.

You can run the doctests with::

    $ ./blackbox/bin/test-docs


This will implicitly use `Maven`_ to build a tarball from source if it is
missing and then run all executable examples within the documentation against a
local CrateDB instance..

**Note**: Your network connection should be up and running, or some of the tests
will fail.

You can run the doctests for a specific file (e.g., ``filename.rst``)::

    $ ITEST_FILE_NAME_FILTER=filename.rst ./blackbox/bin/test-docs

.. TIP::

    If you receive an exception like ``java.lang.IllegalStateException: jar hell!``,
    it might be coming from outdated builds in your working tree. Resolving the
    situation mostly works by running ``./mvnw clean``.


Reset the docs build
====================

To force recreation of the CrateDB tarball, run::

    $ ./mvnw clean

To clear the docs, run::

    $ rm -rf blackbox/docs/_out/

Link checker
============

Sphinx includes a link checker, which will also validate the documentation on CI.
You can run it on your local documentation tree by invoking::

    $ ./blackbox/bin/sphinx linkcheck

Troubleshoot
============

If you run into something that looks like an issue with Python or a missing
library, you can usually resolve the issue by resetting your Python
environment.

First, remove the virtual environment directory::

    $ rm -rf blackbox/.venv


Continuous Integration and Deployment (CI/CD)
=============================================

`Read the Docs`_ automatically builds and deploys the docs directly from Git,
and there is nothing you need to do to get the live docs to update. We do,
however, use caching. If the caching appears to be broken, or you want to
force an update, speak to a sysadmin (or create an issue) and ask to clear the
docs web cache (we have a Jenkins job for this purpose).


.. _doctests: https://github.com/crate/crate/blob/master/blackbox/test_docs.py
.. _Maven: https://maven.apache.org
.. _Java: http://www.java.com
.. _Python virtual environment: https://docs.python.org/3/tutorial/venv.html
.. _Python: http://www.python.org
.. _Read the Docs: http://readthedocs.org
.. _reStructuredText: http://docutils.sourceforge.net/rst.html
.. _script: https://github.com/crate/crate/blob/master/blackbox/bin/sphinx
.. _Sphinx: http://sphinx-doc.org
