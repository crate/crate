.. highlight:: psql

.. _sql-create-publication:

======================
``CREATE PUBLICATION``
======================

You can use the ``CREATE PUBLICATION`` :ref:`statement <gloss-statement>` to
add a new publication to the current cluster.

.. SEEALSO::

    :ref:`ALTER PUBLICATION <sql-alter-publication>`
    :ref:`DROP PUBLICATION <sql-drop-publication>`


.. _sql-create-publication-synopsis:

Synopsis
========

::

    CREATE PUBLICATION name
    { FOR TABLE table_name [ PARTITION (partition_column = value [, ...]) ]
        [, ...] | FOR ALL TABLES }

.. _sql-create-publication-desc:

Description
===========

Add a new :ref:`publication <logical-replication-publication>` to the current
cluster. The publication name must be distinct from the name of any existing
publication in the current cluster. A publication represents a group of tables
or individual partitions whose data changes can be replicated by other clusters
(subscribers) by
creating a :ref:`subscription <logical-replication-subscription>`.

If neither ``FOR TABLE`` nor ``FOR ALL TABLES`` is specified, then the publication
starts out with an empty set of tables. That is useful if tables are to be
added later. The creation of a publication does not start any replication.

It is not possible to use system tables or foreign tables in a publication.
Using them in an explicit ``FOR TABLE`` clause results in an error. With the
``FOR ALL TABLES`` clause they're excluded.

.. _sql-create-publication-params:

Parameters
===========

**name**
  The name of the new publication.

**FOR TABLE**
  Specifies tables or individual partitions to add to the publication.
  Without a ``PARTITION`` clause, all partitions of a partitioned table are
  included, including partitions created in the future.

  Use ``PARTITION (partition_column = value [, ...])`` after a table name to
  publish only one partition. Specify every partition column exactly once with
  its value. The partition must already exist. To publish several individual
  partitions, repeat the table name with a different ``PARTITION`` clause for
  each partition. For example::

      CREATE PUBLICATION pub_partitions FOR TABLE
          doc.events PARTITION (year = 2025),
          doc.events PARTITION (year = 2026);

  See :ref:`logical-replication-partitions` for subscription requirements and
  limitations.

  .. NOTE::

     Dropping a table or a schema with ``CASCADE`` on the publisher cluster also
     removes the dropped tables from publications that explicitly list them.

**FOR ALL TABLES**
  Marks the publication as one that replicates changes for all tables in the
  cluster, including tables created in the future.
