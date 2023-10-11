.. _conf-logging:

=======
Logging
=======

CrateDB supports two kinds of logging:

- Application logging with `Log4j`_
- *Java Virtual Machine* (JVM) garbage collection logging

We use "application" here to distinguish between CrateDB running as a Java
application and the JVM itself, which runs CrateDB.

Because garbage collection logging is a native feature of the JVM it behaves
differently and is configured differently.

.. rubric:: Table of contents

.. contents::
   :local:

.. _conf-logging-app:

Application logging
===================

.. _conf-logging-log4j:

Log4j
-----

CrateDB uses `Log4j`_.

.. _conf-logging-log4j-file:

Configuration file
..................

You can configure Log4j with the ``log4j2.properties`` file in the CrateDB
:ref:`configuration directory <config>`.

The ``log4j2.properties`` file is formatted using `YAML`_ and simplifies Log4j
configuration by allowing you to use the `PropertyConfigurator`_ but without
having to tediously repeat the ``log4j`` prefix.

Here's one example:

.. code-block:: yaml

  rootLogger.level = info
  rootLogger.appenderRef.console.ref = console

  # log query execution errors for easier debugging
  logger.action.name = org.crate.action.sql
  logger.action.level = debug

  appender.console.type = Console
  appender.console.name = console
  appender.console.layout.type = PatternLayout
  appender.console.layout.pattern = [%d{ISO8601}][%-5p][%-25c{1.}] %marker%m%n

And here is a snippet of the generated properties ready for use with log4j.
You get the point.

.. SEEALSO::

    Consult the `PropertyConfigurator`_ documentation or the `configuration
    section`_ of the Log4j documentation for more information.

.. _conf-logging-log4j-loggers:

Log levels
..........

Possible log levels are the same as for Log4j_, in order of increasing
importance:

- ``TRACE``
- ``DEBUG``
- ``INFO``
- ``WARN``
- ``ERROR``

Log levels must be provided as string literals in the ``SET`` statement.

.. NOTE::

   Be careful using the ``TRACE`` log level because it's extremely verbose,
   can obscure other important log messages and even fill up entire data disks
   in some cases.

.. _conf-logging-log4j-run-time:

Run-time configuration
......................

It's possible to set the log level of loggers at runtime using :ref:`SET
<ref-set>`, like so:

.. code-block:: sql

  SET GLOBAL TRANSIENT "logger.action" = 'INFO';

In this example, the log level ``INFO`` is applied to the ``action`` logger.

In addition to being able to configure any of the standard :ref:`loggers
<conf-logging-log4j-loggers>`, you can configure the root (i.e. default) logger
using ``logger._root``.

As with any setting, you can inspect the current configuration by querying the
:ref:`sys.cluster <sys-cluster-settings>` table.

.. TIP::

    Run-time logging configuration is particularly useful if you are debugging
    a problem and you want to increase the log level without restarting nodes.

Run-time logging configuration is applied across the whole cluster, and
overrides the start-up configuration defined in each respective
``log4j2.properties`` file.

.. CAUTION::

    The :ref:`RESET<ref-set>` statement is supported but logging configuration
    is only reset when the whole cluster is restarted.

.. _conf-logging-jvm:

JVM logging
===========

CrateDB exposes some native JVM logging functionality.

.. _conf-logging-gc:

Garbage collection
------------------

CrateDB logs JVM garbage collection times using the built-in garbage
collection logging of the JVM.

Environment variables
.....................

The following :ref:`environment variables <config>` can be used to configure
garbage collection logging.

.. _conf-logging-gc-logging:

``CRATE_DISABLE_GC_LOGGING``: *boolean integer* (default: ``0``)
  Whether to disable garbage collection logging.

  Set to ``1`` to disable.

  .. NOTE::

      Since CrateDB 3.0, Garbage collection logging is enabled by default.

``CRATE_GC_LOG_DIR``: *path to logs directory* (default: varies)
  The log file directory.

  For a `basic installation`_, the ``logs`` directory in the
  :ref:`CRATE_HOME <conf-env-crate-home>` directory is default.

  If you have installed `a CrateDB Linux package`_, the default directory is
  ``/var/log/crate`` instead.

.. _basic installation: https://crate.io/docs/crate/tutorials/en/latest/basic/
.. _a CrateDB Linux package: https://crate.io/docs/crate/tutorials/en/latest/basic/index.html#linux

.. _conf-logging-gc-log-size:

``CRATE_GC_LOG_SIZE``: *file size* (default: ``64m``)
  Maximum file size of log files before they are `rotated`_.

.. _conf-logging-gc-log-files:

``CRATE_GC_LOG_FILES``: *number* (default: ``16``)
  The amount of files kept in rotation.

.. CAUTION::

    With the default configuration of 16 rotated 64 megabyte log files, garbage
    collection logs will grow to occupy one gigabyte on disk.

    Make sure you have enough available disk space for configuration.

.. _configuration section: https://logging.apache.org/log4j/1.2/manual.html#Configuration
.. _Log4j: https://logging.apache.org/log4j/1.2/
.. _PropertyConfigurator: https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/PropertyConfigurator.html
.. _rotated: https://en.wikipedia.org/wiki/Log_rotation
.. _YAML: https://yaml.org/
