.. highlight:: psql

.. _sql-copy-to:

===========
``COPY TO``
===========

Export table contents to files on CrateDB node machines.

.. rubric:: Table of contents

.. contents::
   :local:


.. _sql-copy-to-synopsis:

Synopsis
========

::

    COPY table_ident [ PARTITION ( partition_column = value [ , ... ] ) ]
                     [ ( column [ , ...] ) ]
                     [ WHERE condition ]
                     TO DIRECTORY output_uri
                     [ WITH ( copy_parameter [= value] [, ... ] ) ]


.. _sql-copy-to-description:

Description
===========

The ``COPY TO`` command exports the contents of a table to one or more files
into a given directory with unique filenames. Each node with at least one shard
of the table will export its contents onto their local disk.

The created files are JSON formatted and contain one table row per line and,
due to the distributed nature of CrateDB, *will remain on the same nodes*
*where the shards are*.

Here's an example:

::

    cr> COPY quotes TO DIRECTORY '/tmp/' with (compression='gzip');
    COPY OK, 3 rows affected ...

.. NOTE::

   Currently only user tables can be exported. System tables like ``sys.nodes``
   and blob tables don't work with the ``COPY TO`` statement.


.. _sql-copy-to-parameters:

Parameters
==========

:table_ident:
  The name (optionally schema-qualified) of the table to be exported.

:column:
  (optional) A list of column :ref:`expressions <gloss-expression>` that should
  be exported.

.. NOTE::

   When declaring columns, this changes the output to JSON list format, which
   is currently not supported by the ``COPY FROM`` statement.


.. _sql-copy-to-clauses:

Clauses
=======


.. _sql-copy-to-partition:

``PARTITION``
-------------

.. EDITORIAL NOTE
   ##############

   Multiple files (in this directory) use the same standard text for
   documenting the ``PARTITION`` clause. (Minor verb changes are made to
   accomodate the specifics of the parent statement.)

   For consistency, if you make changes here, please be sure to make a
   corresponding change to the other files.

If the table is :ref:`partitioned <partitioned-tables>`, the optional
``PARTITION`` clause can be used to export data from a one partition
exclusively.

::

    [ PARTITION ( partition_column = value [ , ... ] ) ]

:partition_column:
  One of the column names used for table partitioning.

:value:
  The respective column value.

All :ref:`partition columns <gloss-partition-column>` (specified by the
:ref:`sql-create-table-partitioned-by` clause) must be listed inside the
parentheses along with their respective values using the ``partition_column =
value`` syntax (separated by commas).

Because each partition corresponds to a unique set of :ref:`partition column
<gloss-partition-column>` row values, this clause uniquely identifies a single
partition to export.

.. TIP::

    The :ref:`ref-show-create-table` statement will show you the complete list
    of partition columns specified by the
    :ref:`sql-create-table-partitioned-by` clause.

.. CAUTION::

    The exported data doesn't contain the partition columns or the
    corresponding values because they are not part of the partitioned tables.

    If ``COPY TO`` is used on a partitioned table without the ``PARTITION``
    clause, the partition columns and values will be included in the rows of
    the exported files. If a partition column is a generated column, it will
    not be included even if the ``PARTITION`` clause is missing.


.. _sql-copy-to-where:

``WHERE``
---------

The ``WHERE`` clauses use the same syntax as ``SELECT`` statements, allowing
partial exports. (see :ref:`sql_dql_where_clause` for more information).


.. _sql-copy-to-to:

``TO``
------

The ``TO`` clause allows you to specify an output location.
::

    TO DIRECTORY output_uri

:output_uri:
  The output URI.

The output URI can be any :ref:`expression <gloss-expression>` that
:ref:`evaluates <gloss-evaluation>` to a string. The string must be a valid URI
that uses the ``file://`` or ``s3://`` URI scheme.

For example:

  - ``file:///path/to/dir``
  - ``s3://[<accesskey>:<secretkey>@]<bucketname>/<path>``

If no URI scheme is given (e.g., ``/path/to/dir``) the default scheme
``file://`` will be used.


.. _sql-copy-to-containers:

Containers
..........

If you are running CrateDB inside a container (e.g., you are running CrateDB on
*Docker*) the URI must point to a file inside the container.

You may have to configure a new `Docker volume`_ to accomplish this.


.. _sql-copy-to-windows:

Microsoft Windows
.................

If you are using *Microsoft Windows*, you must include the drive letter in the
file URI.

For example, the above file URI should instead be written as
``file:///C://tmp/import_data/quotes.json``.

Consult the `Windows documentation`_ for more information.


.. _sql-copy-to-aws:

Amazon Web Services
...................

A ``secretkey`` provided by *Amazon Web Services* (AWS) can contain characters
such as ``/``, ``+`` or ``=``. Such characters must be URI encoded. The same
encoding as in :ref:`sql-copy-from-s3` applies.

Additionally, versions prior to 0.51.x use HTTP for connections to S3. Since
0.51.x these connections are using the HTTPS protocol. Please make sure you
update your firewall rules to allow outgoing connections on port ``443``.


.. _sql-copy-to-with:

``WITH``
--------

The optional ``WITH`` clause can specify parameters for the copy statement.

::

    [ WITH ( copy_parameter [= value] [, ... ] ) ]

Possible copy_parameters are:


.. _sql-copy-to-compression:

``compression``
...............

Define if and how the exported data should be compressed.

By default the output is not compressed.

Possible values for the ``compression`` setting are:

:gzip:
  Use gzip_ to compress the data output.


.. _sql-copy-to-format:

``format``
..........

Optional parameter to override default output behavior.

Possible values for the ``format`` settings are:

:json_object:
  Each row in the result set is serialized as JSON object and written to an
  output file where one line contains one object. This is the default behavior
  if no columns are defined. Use this format to import with
  :ref:`sql-copy-from`.

:json_array:
  Each row in the result set is serialized as JSON array, storing one array per
  line in an output file. This is the default behavior if columns are defined.


.. _Amazon S3: https://aws.amazon.com/s3/
.. _Docker volume: https://docs.docker.com/storage/volumes/
.. _gzip: https://www.gzip.org/
.. _NFS: https://en.wikipedia.org/wiki/Network_File_System
.. _Windows documentation: https://docs.microsoft.com/en-us/dotnet/standard/io/file-path-formats
