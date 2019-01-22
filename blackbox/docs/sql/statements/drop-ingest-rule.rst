.. highlight:: psql

.. _drop-ingest-rule:

====================
``DROP INGEST RULE``
====================

Deletes an existing ingestion rule.

   .. WARNING::

      This statement has been deprecated and will be removed in the future.

.. rubric:: Table of Contents

.. contents::
   :local:

Synopsis
========

::

    DROP INGEST RULE [ IF EXISTS ] rule_name


Description
===========

``DROP INGEST RULE`` drops an existing ingestion rule.

Parameters
==========

:IF EXISTS:
  Do not fail if the rule doesn't exist.

:rule_name:
  The name of the rule to drop.

Note
=====

When :ref:`administration_user_management` is enabled, this statement can only
be issued by a superuser.

Examples
========

Drop Ingestion rule ``mqtt_v4_rule``::

    DROP INGEST RULE rule_v4

.. SEEALSO::

   :ref:`administration-ingestion-rules`

   :ref:`create-ingest-rule`
