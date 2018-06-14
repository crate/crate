.. highlight:: psql
.. _copy_to:

===========
``COPY TO``
===========

Export table contents to files on CrateDB node machines.

.. rubric:: Table of Contents

.. contents::
   :local:

Synopsis
========

::

    COPY table_ident [ PARTITION ( partition_column = value [ , ... ] ) ]
                     [ ( column [ , ...] ) ]
                     [ WHERE condition ]
                     TO DIRECTORY output_uri
                     [ WITH ( copy_parameter [= value] [, ... ] ) ]

Description
===========

The ``COPY TO`` command exports the contents of a table to one or more files
into a given directory with unique filenames. Each node with at least one shard
of the table will export its contents onto their local disk.

The created files are JSON formatted and contain one table row per line and,
due to the distributed nature of CrateDB, *will remain on the same nodes*
*where the shards are*.

.. NOTE::

   Currently only user tables can be exported. System tables like ``sys.nodes``
   and blob tables don't work with the ``COPY TO`` statement.

Parameters
==========

:table_ident:
  The name (optionally schema-qualified) of the table to be exported.

:column:
  (optional) A list of column expressions that should be exported.

.. NOTE::

   Declaring columns changes the output to JSON list format, which is
   currently not supported by the COPY FROM statement.

Clauses
-------

``WHERE``
.........

``WHERE`` clauses use the same syntax as ``SELECT`` statements, allowing partial
exports. (see :ref:`sql_dql_where_clause` for more information).

Output URI
==========

The ``output_uri`` can be any expression evaluating to a string.

The resulting string should be a valid URI of one of the supporting schemes:

 * ``file://``
 * ``s3://[<accesskey>:<secretkey>@]<bucketname>/<path>``

If no scheme is given (e.g.: '/path/to/dir') the default uri-scheme ``file://``
will be used.

If the s3 scheme is used without specifying any credentials an attempt is made
to read these information from the AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
environment variables. In addition to that the Java System properties
aws.accessKeyId and aws.secretKey are also used as a fallback.

.. NOTE::

   A ``secretkey`` provided by Amazon Web Service can contain characters such
   as '/', '+' or '='. Such characters must be URI encoded. The same encoding
   as in :ref:`copy_from_s3` applies.

   Additionally, versions prior to 0.51.x use HTTP for connections to S3. Since
   0.51.x these connections are using the HTTPS protocol. Please make sure you
   update your firewall rules to allow outgoing connections on port ``443``.

Clauses
=======

``PARTITION``
-------------

If the table is partitioned this clause can be used to only export data from a
specific partition.

The exported data doesn't contain the partition columns or values as they are
not part of the partitioned tables.

::

    [ PARTITION ( partition_column = value [ , ... ] ) ]

:partition_column:
  The name of the column by which the table is partitioned. All
  partition columns that were part of the :ref:`partitioned_by_clause` of the
  :ref:`ref-create-table` statement must be specified.

:value:
  The columns value.

.. NOTE::

   If ``COPY TO`` is used on a partitioned table without the
   ``PARTITION`` clause, the partition columns and values will be
   included in the rows of the exported files. If a partition column is
   a generated column, it will not be included even if the ``PARTITION``
   clause is missing.

``WITH``
--------

The optional WITH clause can specify parameters for the copy statement.

::

    [ WITH ( copy_parameter [= value] [, ... ] ) ]

Possible copy_parameters are:

.. _compression:

``compression``
...............

Define if and how the exported data should be compressed.

By default the output is not compressed.

Possible values for the ``compression`` setting are:

:gzip:
  Use gzip_ to compress the data output.

.. _format:

``format``
..........

Optional parameter to override default output behavior.

Possible values for the ``format`` settings are:

:json_object:
  Each row in the result set is serialized as JSON object and written to
  an output file where one line contains one object. This is the default
  behavior if no columns are defined. Use this format to import with
  :ref:`copy_from`.

:json_array:
  Each row in the result set is serialized as JSON array, storing one
  array per line in an output file. This is the default behavior if
  columns are defined.

.. _gzip: http://www.gzip.org/
.. _`Amazon S3`: http://aws.amazon.com/s3/
.. _NFS: http://en.wikipedia.org/wiki/Network_File_System
