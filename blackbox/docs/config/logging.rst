.. highlight:: sh

.. _conf-logging:

=======
Logging
=======

.. rubric:: Table of Contents

.. contents::
   :local:

Application Logging
===================

CrateDB comes, out of the box, with Log4j_ 1.2.x. It tries to simplify log4j
configuration by using YAML to configure it. The logging configuration file is
located at ``$CRATE_HOME/config/log4j2.properties``.

The yaml file is used to prepare a set of properties used for logging
configuration using the `PropertyConfigurator`_ but without the tediously
repeating ``log4j`` prefix. Here is a small example of a working logging
configuration.

.. code-block:: yaml

  rootLogger.level = info
  rootLogger.appenderRef.console.ref = console

  # log action execution errors for easier debugging
  logger.action.name = org.crate.action.sql
  logger.action.level = debug

  appender.console.type = Console
  appender.console.name = console
  appender.console.layout.type = PatternLayout
  appender.console.layout.pattern = [%d{ISO8601}][%-5p][%-25c{1.}] %marker%m%n

And here is a snippet of the generated properties ready for use with log4j.
You get the point.

.. code-block:: yaml

  log4j.rootLogger=INFO, console

  log4j.logger.action=DEBUG

  log4j.appender.console=org.elasticsearch.common.logging.log4j.ConsoleAppender
  log4j.appender.console.layout=org.apache.log4j.PatternLayout
  log4j.appender.console.layout.conversionPattern=[%d{ISO8601}][%-5p][%-25c] %m%n

   ...

.. _PropertyConfigurator: https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/PropertyConfigurator.html

Settings
--------

It's possible to set the log level of loggers at runtime. This is particularly
useful when debugging problems and there is a need to increase the log level
without wanting to restart nodes. Logging settings are cluster wide and
override the logging configuration of nodes defined in their
``log4j2.properties``.

The :ref:`RESET<ref-set>` statement is also supported, however only with the
limitation that the reset of the logging override only takes affect after
cluster restart.

To set the log level you can use the regular :ref:`SET<ref-set>` statement, for
example:

.. code-block:: sql

  SET GLOBAL TRANSIENT "logger.action" = 'INFO';

The logging setting consists of the prefix ``logger`` and a variable suffix
which defines the name of the logger that the log level should be applied to.

In addition to `hierarchical named loggers`_ you can also change the log level
of the root logger using the ``_root`` suffix.

In the example above the log level ``INFO`` is applied to the logger
``action``.

Possible log levels are the same as for Log4j_: ``TRACE``, ``DEBUG``, ``INFO``,
``WARN``, and ``ERROR``. They must be provided as string literals in the
``SET`` statement.

.. WARNING::

   Be careful using the ``TRACE`` log level because it's extremely verbose,
   can obscure other important log messages and even fill up entire data disks
   in some cases.

It is also possible to inspect the current "logging overrides" in a cluster by
querying the ``sys.cluster`` table (see :ref:`Cluster
Settings<sys-cluster-settings>`).

.. _Log4j: https://logging.apache.org/log4j/1.2/
.. _`hierarchical named loggers`: https://logging.apache.org/log4j/1.2/manual.html

.. _conf-logging-gc:

Garbage Collection Logging
==========================

Addition to the regular application logging, CrateDB also logs garbage
collection times of the Java Virtual Machine using the built-in garbage
collection logging of the JVM.

Garbage collection logging is enabled by default since CrateDB 3.0. It can be
disabled by setting the environment variable ``CRATE_DISABLE_GC_LOGGING``.

The default location for the garbage collection log files is
``$CRATE_HOME/logs``. The files are rotated_ at 64MB in size, kept for 16
rotations, and prefixed with ``gc.log``.  This means, that the garbage
collection log files may take up to 1GB of space on your disk.  However, both
the maximum file size and the amount of kept files can be controlled with the
environment variables described below.

.. TIP::

   The default garbage collection log directory differ for different CrateDB
   distributions. See :ref:`CRATE_GC_LOG_DIR<conf-logging-gc-logdir>` below.

Configuration
-------------

The following environment variables are available to configure garbage
collection logging.

:CRATE_DISABLE_GC_LOGGING:
  | Disable garbage collection logging.
  | *Default:* not set

.. _conf-logging-gc-logdir:

:CRATE_GC_LOG_DIR:
  | Log file directory.
  | *Default for .tar.gz:* :ref:`CRATE_HOME <conf-env-crate-home>`/logs
  | *Default for .rpm:* /var/log/crate
  | *Default for .deb:* /var/log/crate

:CRATE_GC_LOG_SIZE:
  | Maximum file size of log files before they are rotated.
  | *Default:* 64m

:CRATE_GC_LOG_FILES:
  | Amount of files kept in rotation.
  | *Default:* 16

.. _rotated: https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6941923
