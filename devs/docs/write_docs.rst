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


Build the docs with Gradle (including doctests)
===============================================

We have a `Gradle`_ `build script`_ that can also be used to build and, more
importantly, test the docs.

To bootstrap Gradle on your system, you can navigate to the home directory
of this repository and run::

    $ ./gradlew


Run the Sphinx dev server
-------------------------

Run the Gradle ``developDocs`` task::

    $ ./gradlew :blackbox:developDocs

This invokes the running of a script which watches the file system and
rebuilds the docs when changes are detected. When a change is detected, it
will automatically refresh the browser tab for you.

Once the web server is running, you can view your local copy of the docs by
visiting http://127.0.0.1:8000 in a web browser.


Build the docs
--------------

You can build the docs without starting the web server. You may want to do
this if you want to test the docs without previewing your changes.

To do this, run the Gradle ``buildDocs`` task::

    $ ./gradlew :blackbox:buildDocs

The build output is located in `/docs/_out/html`.


Run the doctests
----------------

Many of the code examples in the documentation are executable and function as
`doctests`_.

You can run the doctests with the Gradle ``itest`` task::

    $ ./gradlew :blackbox:itest

**Note**: Your network connection should be up and running, or some of the tests
will fail.

You can run the doctests for a specific file (e.g., ``filename.rst``):

    $ ITEST_FILE_NAME_FILTER=filename.rst ./gradlew itest

You can also ``export`` ``ITEST_FILE_NAME_FILTER`` to your shell environment
(e.g., export ITEST_FILE_NAME_FILTER=filename.rst``) if you want to set the
value for the remainder of your terminal session.

.. TIP::

    If you receive an exception like ``java.lang.IllegalStateException: jar hell!``,
    it might be coming from outdated builds in your working tree. Resolving the
    situation mostly works by running ``./gradlew clean``.


Reset the docs build
--------------------

To clean or reset the docs build, run the Gradle ``cleanDocs`` task::

    $ ./gradlew :blackbox:cleanDocs


Build the docs with Sphinx
==========================

You can use Sphinx to build the docs *without* setting up your Java environment
or running the `doctests`_.

From the root directory of this repository, change into the ``blackbox``
directory::

    $ cd blackbox

Run the provided bootstrap script to create a `Python virtual environment`_
and install the necessary Python packages required by the docs::

    $ ./bootstrap.sh

Then, run the Sphinx development server with the provided `script`_::

    $ bin/sphinx dev

The command above watches the file system and rebuilds the docs when changes
are detected. When a change is detected, it will automatically refresh the
browser tab for you.

Once the web server is running, you can view your local copy of the docs by
visiting http://127.0.0.1:8000 in a web browser.

**Note**: The CrateDB docs include many `doctests`_ to test code snippets in the
documentation. The method described above is useful for previewing quick
changes. However, to properly test the documentation, you must follow the
instructions in the section, `Build the docs with Gradle (including full
doctests)`_.

Link checker
------------

Sphinx includes a link checker, which will also validate the documentation on CI.
You can run it on your local documentation tree by invoking::

    $ bin/sphinx linkcheck

Troubleshoot
------------

If you run into something that looks like an issue with Python or a missing
library, you can usually resolve the issue by resetting your Python
environment.

First, remove the virtual environment directory::

    $ rm -rf .venv

Then, rerun the bootstrap script::

    $ ./bootstrap.sh


Continuous Integration and Deployment (CI/CD)
=============================================

`Read the Docs`_ automatically builds and deploys the docs directly from Git,
and there is nothing you need to do to get the live docs to update. We do,
however, use caching. If the caching appears to be broken, or you want to
force an update, speak to a sysadmin (or create an issue) and ask to clear the
docs web cache (we have a Jenkins job for this purpose).


.. _build script: https://github.com/crate/crate/blob/master/blackbox/build.gradle
.. _doctests: https://github.com/crate/crate/blob/master/blackbox/test_docs.py
.. _Gradle: https://gradle.org
.. _Java: http://www.java.com
.. _Python virtual environment: https://docs.python.org/3/tutorial/venv.html
.. _Python: http://www.python.org
.. _Read the Docs: http://readthedocs.org
.. _reStructuredText: http://docutils.sourceforge.net/rst.html
.. _script: https://github.com/crate/crate/blob/master/blackbox/bin/sphinx
.. _Sphinx: http://sphinx-doc.org
