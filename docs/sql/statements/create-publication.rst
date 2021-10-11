.. highlight:: psql

.. _sql-create-publication:

======================
``CREATE PUBLICATION``
======================

You can use the ``CREATE PUBLICATION`` :ref:`statement <gloss-statement>` to
add a new publication into the current cluster.

.. SEEALSO::

    :ref:`ALTER PUBLICATION <sql-alter-publication>`
    :ref:`DROP PUBLICATION <sql-drop-publication>`

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


.. _sql-create-publication-synopsis:

Synopsis
========

::

    CREATE PUBLICATION name
    { FOR TABLE table_name [, ...] | FOR ALL TABLES }

.. _sql-create-publication-desc:

Description
===========

Add a new :ref:`publication <logical-replication-publication>` into the current
cluster. The publication name must be distinct from the name of any existing
publication in the current cluster. A publication represents a group of tables
whose data changes can be replicated by other clusters (subscribers) by
creating a :ref:`subscription <logical-replication-subscription>`.

If neither FOR TABLE nor FOR ALL TABLES is specified, then the publication
starts out with an empty set of tables. That is useful if tables are to be
added later. The creation of a publication does not start any replication.

.. _sql-create-publication-params:

Parameters
===========

**name**
  The name of the new publication.

**FOR TABLE**
  Specifies a list of tables to add to the publication. The partitions of a
  partitioned table are always implicitly considered part of the publication.

**FOR ALL TABLES**
  Marks the publication as one that replicates changes for all tables in the
  cluster, including tables created in the future.

