.. _create-ingest-rule:

======================
``CREATE INGEST RULE``
======================

Defines a new ingestion rule.

   .. WARNING::

      This statement has been deprecated and will be removed in the future.

.. rubric:: Table of Contents

.. contents::
   :local:

Synopsis
========
.. highlight:: psql

::

    CREATE INGEST RULE rule_name
    ON source_ident
    [WHERE condition]
    INTO table_ident


Description
===========

``CREATE INGEST RULE`` creates a new ingestion rule on an ingest source
(``source_ident``) that filters data (via the ``WHERE`` condition) into a target
table (``table_ident``).

Parameters
==========

:rule_name:
  The rule name.
  
:source_ident:
  The ingestion source identifier.
  
:condition:
  A boolean expression using references specific to the ingestion
  implementation.
  
:table_ident:
  The target table identifier.

Notes
=====

When :ref:`administration_user_management` is enabled, this statement can only
be issued by a superuser.

By default, all incoming data will be discarded by an ingestion source. You
must create an ingestion rule to write the data anywhere.

Ingestion rules can be created for ingestion sources that do not exist yet. In
such cases, the ingestion rule will have no effect until the ingestion source
is installed.

The target table needs to be created manually before the ``CREATE INGEST RULE``
statement is issued, and it needs to have the same structure as the ingestion
source data. Otherwise, the statement will fail and an error will be logged.

The ``WHERE`` clause applies to the fields of the incoming data structure.
Therefore, the details of what can be queried in the ``WHERE`` clause is
specific to each ingestion source. In the example below, ``topic`` is a
field of the incoming MQTT messages.

Example
========

To create an ingestion rule on the ``mqtt`` source that filters messages that
match ``v4/%`` into the ``mqtt_v4`` table, you could do::

    CREATE INGEST RULE mqtt_v4_rule
    ON mqtt
    WHERE topic like 'v4/%'
    INTO mqtt_v4


.. SEEALSO::

    :ref:`administration-ingestion-rules`

    :ref:`drop-ingest-rule`

.. _MQTT: http://mqtt.org/
