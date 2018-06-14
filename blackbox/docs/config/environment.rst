.. _conf-env:

=====================
Environment Variables
=====================

CrateDB can be configured with some `environment variables`_.

There are many different ways to set environment variables, depending on how
CrateDB is being deployed.

Here is a trivial example::

    sh$ export CRATE_HOME=/tmp/crate
    sh$ ./bin/crate

Here, we set ``CRATE_HOME`` to ``/tmp/crate``, export it so that
sub-processes of the shell have access, and then start CrateDB.

.. rubric:: Table of Contents

.. contents::
   :local:

.. _conf-env-app:

Application Variables
=====================

.. _conf-env-crate-home:

``CRATE_HOME``: *directory path*
  The home directory of the CrateDB installation.

  This directory is used as the root for the :ref:`configuration directory
  <config>`, data directory, log directory, and so on.

  If you have installed CrateDB from a package, this variable should be set
  for you.

  If you are installing manually, in most cases, this should be set to the
  directory from which you would normally execute ``bin/crate``, i.e. the root
  directory of the `expanded tarball`_.

.. _conf-env-java:

Java Variables
==============

.. _conf-env-java-general:

General
-------

.. _conf-env-java-opts:

``CRATE_JAVA_OPTS``: *Java options*
  The Java options to use when running CrateDB.

  For example, you could change the stack size like this::

      CRATE_JAVA_OPTS=-Xss500k

  .. SEEALSO::

      For more information about Java options, consult the documentation for
      `Microsoft Windows`_  or `Unix-like operating systems`_.

.. _Unix-like operating systems: https://docs.oracle.com/javase/8/docs/technotes/tools/unix/java.html
.. _Microsoft Windows: https://docs.oracle.com/javase/8/docs/technotes/tools/windows/java.html

.. _conf-env-heap-size:

``CRATE_HEAP_SIZE``: *size*
  The Java heap size, i.e. the amount of memory that can be used.

  You can set the heap size to four gigabytes like this::

      CRATE_HEAP_SIZE=4g

  .. TIP::

      Use ``g`` for gigabytes or ``m`` for megabytes.

.. _conf-env-dump-path:

``CRATE_HEAP_DUMP_PATH``: *file or directory path* (default: varies)
  The directory to be used for heap dumps in the case of a crash.

  If a directory path is configured, new heap dumps will be written to that
  directory every time CrateDB crashes.

  If a file path is configured (i.e. the last node of the path is non-existent
  or exists and is a file) CrateDB will overwrite that file with a heap dump
  every time it crashes.

  Default values are as follows:

  - For `basic installations`_, the process working directory

  - If you have installed `a CrateDB Linux package`_, ``/var/lib/crate``

  - When running `CrateDB on Docker`_, ``/data/data``

  .. WARNING::

      Make sure that there is enough disk space available for heap dumps.

.. _basic installations: https://crate.io/docs/crate/getting-started/en/latest/install-run/basic.html
.. _a CrateDB Linux package: https://crate.io/docs/crate/getting-started/en/latest/install-run/special/linux.html
.. _CrateDB on Docker: https://crate.io/docs/crate/getting-started/en/latest/install-run/special/docker.html
.. _Java options: http://docs.oracle.com/javase/7/docs/technotes/tools/windows/java.html#CBBIJCHG
.. _environment variables: https://en.wikipedia.org/wiki/Environment_variable
.. _expanded tarball: https://crate.io/docs/crate/getting-started/en/latest/install-run/basic.html
