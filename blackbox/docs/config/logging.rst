.. highlight:: sh

.. _conf-logging:

=======
Logging
=======

.. rubric:: Table of Contents

.. contents::
   :local:

Introduction
============

CrateDB comes, out of the box, with Log4j_ 1.2.x. It tries to simplify log4j
configuration by using YAML to configure it. The logging configuration file is
at ``config/log4j2.properties``.

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
========

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

.. NOTE::

   Be careful using the ``TRACE`` log level because it's extremely verbose,
   can obscure other important log messages and even fill up entire data disks
   in some cases.

It is also possible to inspect the current "logging overrides" in a cluster by
querying the ``sys.cluster`` table (see :ref:`Cluster
Settings<sys-cluster-settings>`).

.. _Log4j: https://logging.apache.org/log4j/1.2/
.. _`hierarchical named loggers`: https://logging.apache.org/log4j/1.2/manual.html
