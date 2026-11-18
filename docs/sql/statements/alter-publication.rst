.. highlight:: psql

.. _sql-alter-publication:

=====================
``ALTER PUBLICATION``
=====================

You can use the ``ALTER PUBLICATION`` :ref:`statement <gloss-statement>` to
update the list of published tables on the current cluster.

.. SEEALSO::

    :ref:`CREATE PUBLICATION <sql-create-publication>`

    :ref:`DROP PUBLICATION <sql-drop-publication>`


.. _sql-alter-publication-synopsis:

Synopsis
========

::

    ALTER PUBLICATION name ADD TABLE table_name [, ...]
    ALTER PUBLICATION name SET TABLE table_name [, ...]
    ALTER PUBLICATION name DROP TABLE table_name [, ...]

.. _sql-alter-publication-desc:

Description
===========

Update the list of published tables according to the command. If a table gets
deleted from the publication and it has existing subscriptions, the replication
of the table stops for all subscribers. Already replicated data remains on
the subscribed clusters, therefore subscribers can not re-subscribe again to
the tables removed from the publication.

.. NOTE::

  Replicated tables on a subscriber cluster will turn into regular writable
  tables after excluding them from a publication on a publishing cluster.

``ALTER PUBLICATION`` isn't supported for publications that were created with
``FOR ALL TABLES``.

Parameters
==========

**name**
  The name of the publication to be updated.

**ADD TABLE**
  Add one or more tables to the publication.

**DROP TABLE**
  Remove one or more tables from the publication.

**SET TABLE**
  Replace the tables in the publication with the given tables.
