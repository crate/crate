.. _conf-env:

=====================
Environment variables
=====================

CrateDB can be configured with some `environment variables`_.

There are many different ways to set environment variables, depending on how
CrateDB is being deployed.

Here is a trivial example::

    sh$ export CRATE_HOME=/tmp/crate
    sh$ ./bin/crate

Here, we set ``CRATE_HOME`` to ``/tmp/crate``, export it so that
sub-processes of the shell have access, and then start CrateDB.

CrateDB supports two kinds of environment variables:

- Application variables
- *Java Virtual Machine* (JVM) variables

We use "application" here to distinguish between CrateDB running as a Java
application and the JVM itself, which runs CrateDB.

.. rubric:: Table of contents

.. contents::
   :local:

.. _conf-env-app:

Application variables
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

JVM variables
=============

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

  Use ``g`` for gigabytes or ``m`` for megabytes.

  .. SEEALSO::

     `Appropriate memory configuration`_ is important for optimal performance.

.. _appropriate memory configuration: https://crate.io/docs/crate/guide/en/latest/performance/memory.html

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

      Make sure there is enough disk space available for heap dumps.

.. _garbage-collection:

Garbage collection
------------------

Collector
~~~~~~~~~

CrateDB uses the `G1`_ garbage collector by default.

Before CrateDB 4.1 it defaulted to use the `Concurrent Mark Sweep` garbage
collector. If you'd like to continue using CMS, you can switch setting the
following :ref:`CRATE_JAVA_OPTS <conf-env-java-opts>`::


  export CRATE_JAVA_OPTS="-XX:-UseG1GC -XX:+UseCMSInitiatingOccupancyOnly -XX:+UseConcMarkSweepGC"


Logging
~~~~~~~

CrateDB logs JVM garbage collection times using the built-in garbage collection
logging of the JVM.

.. SEEALSO::

   The :ref:`logging configuration <conf-logging-gc>` documentation has
   the complete list of garbage collection logging environment variables.

.. _basic installations: https://crate.io/docs/crate/getting-started/en/latest/install-run/basic.html
.. _a CrateDB Linux package: https://crate.io/docs/crate/getting-started/en/latest/install-run/special/linux.html
.. _CrateDB on Docker: https://crate.io/docs/crate/getting-started/en/latest/install-run/special/docker.html
.. _Java options: https://docs.oracle.com/javase/7/docs/technotes/tools/windows/java.html#CBBIJCHG
.. _environment variables: https://en.wikipedia.org/wiki/Environment_variable
.. _expanded tarball: https://crate.io/docs/crate/getting-started/en/latest/install-run/basic.html
.. _Concurrent Mark Sweep: https://docs.oracle.com/javase/10/gctuning/concurrent-mark-sweep-cms-collector.htm
.. _G1: https://docs.oracle.com/javase/10/gctuning/garbage-first-garbage-collector.htm
