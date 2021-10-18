.. highlight:: psql

.. _sql-drop-publication:

====================
``DROP PUBLICATION``
====================

.. SEEALSO::

    :ref:`CREATE PUBLICATION <sql-create-publication>`

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Synopsis
========

::

    DROP PUBLICATION [ IF EXISTS ] name

.. _sql-drop-publication-desc:

Description
===========

Removes an existing publication from the cluster. Stops the replication for all
existing subscriptions.

.. _sql-drop-publication-params:

Parameters
===========

.. _sql-drop-publication-name:

**name**
  The name of the publication to be deleted.