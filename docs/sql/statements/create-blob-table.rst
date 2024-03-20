.. highlight:: psql

.. _sql-create-blob-table:

=====================
``CREATE BLOB TABLE``
=====================

Create a new table for storing *Binary Large OBjects* (BLOBS).

.. rubric:: Table of contents

.. contents::
   :local:


.. _sql-create-blob-table-synopsis:

Synopsis
========

::

    CREATE BLOB TABLE table_name
    [CLUSTERED INTO num_shards SHARDS]
    [ WITH ( storage_parameter [= value] [, ... ] ) ]


.. _sql-create-blob-table-description:

Description
===========

``CREATE BLOB TABLE`` will create a new table for holding BLOBS.

.. SEEALSO::

    :ref:`BLOB support <blob_support>`


.. _sql-create-blob-table-clauses:

Clauses
=======


.. _sql-create-blob-table-clustered:

``CLUSTERED``
-------------

Follows the same syntax as the :ref:`CREATE TABLE ... CLUSTERED
<sql-create-table-clustered>` clause.


.. _sql-create-blob-table-with:

``WITH``
--------

Follows the same syntax as the :ref:`CREATE TABLE ... WITH
<sql-create-table-with>` clause with the following additional parameter.


.. _sql-create-blob-table-blobs-path:

``blobs_path``
..............

Specifies a custom path for storing blob data of a blob table.

:blobs_path:
  The custom path for storing blob data as a string literal value or
  string parameter.

  The path can be either absolute or relative and must be
  creatable/writable by the user CrateDB is running as. A relative path
  value is relative to :ref:`CRATE_HOME <conf-env-crate-home>`. This path take
  precedence over any global configured value.
