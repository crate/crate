.. highlight:: psql
.. _copy_from:

=============
``COPY FROM``
=============

Copy data from files into a table.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

::

    COPY table_ident [ PARTITION (partition_column = value [ , ... ]) ]
    FROM uri [ WITH ( option = value [, ...] ) ] [ RETURN SUMMARY ]

where ``option`` can be one of:

- ``bulk_size`` *integer*
- ``shared`` *boolean*
- ``num_readers`` *integer*
- ``compression`` *text*
- ``overwrite_duplicates`` *boolean*

Description
===========

``COPY FROM`` copies data from a URI to the specified table as a raw data
import.

The nodes in the cluster will attempt to read the files available at the URI
and import the data.

Here's an example:

::

    cr> COPY quotes FROM 'file:///tmp/import_data/quotes.json';
    COPY OK, 3 rows affected (... sec)

Supported formats
-----------------

CrateDB accepts both JSON and CSV inputs. The format is inferred from the file
extension (``.json`` or ``.csv`` respectively) if possible. The format can also
be provided as an option (see :ref:`with_option`). If a format is not specified
and the format cannot be inferred, the file will be processed as JSON.

Files must be UTF-8 encoded. Any keys in the object will be added as columns,
regardless of the previously defined table. Empty lines are skipped.

JSON files must contain a single JSON object per line.

Example JSON data::

    {"id": 1, "quote": "Don't panic"}
    {"id": 2, "quote": "Ford, you're turning into a penguin. Stop it."}

CSV files must contain a header with comma-separated values, which will
be added as columns.

Example CSV data::

    id,quote
    1,"Don't panic"
    2,"Ford, you're turning into a penguin. Stop it."

See also: :ref:`importing_data`.

Type casts and constraints
--------------------------

CrateDB does not check if the column's data types match the types from the
import file. It does not cast the types but will always import the data as in
the source file. Furthermore CrateDB will only check for primary key duplicates
but not for other :ref:`column_constraints` like ``NOT NULL``.

For example a `WKT`_ string cannot be imported into a column of ``geo_shape``
or ``geo_point`` type, since there is no implict cast to the `GeoJSON`_ format.

.. NOTE::

   In case the ``COPY FROM`` statement fails, the log output on the node will
   provide an error message. Any data that has been imported until then has
   been written to the table and should be deleted before restarting the
   import.

URI
===

A string literal or array of string literals containing URIs. Each URI must be
formatted according to the `URI Scheme`_.

In case the URI scheme is missing the value is assumed to be a file path and
will be converted to a ``file://`` URI implicitly.

For example:

.. code-block:: text

    '/tmp folder/file.json'

Will be converted to:

.. code-block:: text

    'file:///tmp%20folder/file.json'

Supported schemes
-----------------

``file``
........

You can use the ``file://`` schema to specify an absolute path to one or more
files accessible via the local filesystem of one or more CrateDB nodes.

The files must be accessible on at least one node and the system user running
the ``crate`` process must have read access to every file specified.

The ``file://`` schema supports wildcard expansion using the ``*`` character.
So, ``file:///tmp/import_data/*.json`` would expand to include every JSON file
in the ``/tmp/import_data`` directory.

By default, every node will attempt to import every file. If the file is
accessible on multiple nodes, you can set the `shared`_ option to true in
order to avoid importing duplicates.

Use :ref:`return_summary` to get information about what actions were performed
on each node.

.. TIP::

    If you are running CrateDB inside a container, the file must be inside the
    container. If you are using Docker, you may have to configure a `Docker
    volume`_ to accomplish this.

.. NOTE::

    If you are using Microsoft Windows, you must include the drive letter in
    the file URI.

    For example:

    .. code-block:: text

        file://C:\/tmp/import_data/quotes.json

    Consult the `Windows documentation`_ for more information.

.. _copy_from_s3:

``s3``
......

The ``s3://`` schema can be used to access buckets on the Amazon AWS S3 Service:

.. code-block:: text

    s3://[<accesskey>:<secretkey>@]<bucketname>/<path>

If no credentials are set the s3 client will operate in anonymous mode, see
`AWS Java Documentation`_.

Using the ``s3://`` schema automatically sets the `shared`_ to true.

.. NOTE::

   A ``secretkey`` provided by Amazon Web Services can contain characters such
   as '/', '+' or '='. These characters must be `URL encoded`_. For a detailed
   explanation read the official `AWS documentation`_.

   To escape a secret key, you can use a snippet like this:

   .. code-block:: console

      sh$ python -c "from getpass import getpass; from urllib.parse import quote_plus; print(quote_plus(getpass('secret_key: ')))"

   This will prompt for the secret key and print the encoded variant.

   Additionally, versions prior to 0.51.x use HTTP for connections to S3. Since
   0.51.x these connections are using the HTTPS protocol. Please make sure you
   update your firewall rules to allow outgoing connections on port ``443``.

``http``, ``https``, and ``jar`` (Java URL protocols)
.....................................................

In addition to the schemes above, CrateDB supports all protocols supported by
the `URL`_ implementation of its JVM (typically ``http``, ``https``, ``ftp``,
and ``jar``). Please refer to the documentation of the JVM vendor for an
accurate list of supported protocols.

These schemes *do not* support wildcard expansion.

Parameters
==========

:table_ident:
  The name (optionally schema-qualified) of an existing table where the
  data should be put.

:uri:
  An expression which evaluates to a URI as defined in `RFC2396`_. The
  supported schemes are listed above. The last part of the path may also
  contain ``*`` wildcards to match multiple files.

Clauses
=======

``PARTITION``
-------------

For partitioned tables this clause can be used to import data into the
specified partition. This clause takes one or more partition columns and for
each column a value.

::

    [ PARTITION ( partition_column = value [ , ... ] ) ]

:partition_column:
  The name of the column by which the table is partitioned. All partition
  columns that were part of the :ref:`partitioned_by_clause` of the
  :ref:`ref-create-table` statement must be specified.

:value:
  The column's value.

.. NOTE::

   Partitioned tables do not store the value for the partition column in each
   row, hence every row will be imported into the specified partition
   regardless of the value provided for the partition columns.


.. _with_option:

``WITH``
--------

The optional ``WITH`` clause can specify options for the COPY FROM statement.

::

    [ WITH ( option = value [, ...] ) ]

Options
.......

``bulk_size``
'''''''''''''

CrateDB will process the lines it reads from the ``path`` in bulks. This option
specifies the size of one batch. The provided value must be greater than 0, the
default value is 10000.

``shared``
''''''''''

This option should be set to true if the URI's location is accessible by more
than one CrateDB node to prevent them from importing the same file.

The default value depends on the scheme of each URI.

If an array of URIs is passed to ``COPY FROM`` this option will overwrite the
default for *all* URIs.

``node_filters``
''''''''''''''''

A filter expression to select the nodes to run the *read* operation.

It's an object in the form of::

    {
        name = '<node_name_regex>',
        id = '<node_id_regex>'
    }

Only one of the keys is required.

The ``name`` regular expression is applied on the ``name`` of all execution
nodes, whereas the ``id`` regex is applied on the ``node id``.

If both keys are set, *both* regular expressions have to match for a node to be
included.

If the `shared`_ option is false, a strict node filter might exclude nodes with
access to the data leading to a partial import.

To verify which nodes match the filter, run the statement with
:doc:`EXPLAIN <explain>`.

``num_readers``
'''''''''''''''

The number of nodes that will read the resources specified in the URI. Defaults
to the number of nodes available in the cluster. If the option is set to a
number greater than the number of available nodes it will still use each node
only once to do the import. However, the value must be an integer greater than
0.

If `shared`_ is set to false this option has to be used with caution. It might
exclude the wrong nodes, causing COPY FROM to read no files or only a subset of
the files.

``compression``
'''''''''''''''

The default value is ``null``, set to ``gzip`` to read gzipped files.

``overwrite_duplicates``
''''''''''''''''''''''''

Default: false

``COPY FROM`` by default won't overwrite rows if a document with the same
primary key already exists. Set to true to overwrite duplicate rows.

``empty_string_as_null``
''''''''''''''''''''''''

If set to ``true`` the ``empty_string_as_null`` option enables conversion
of un-/quoted empty strings into ``NULL``. The default value is ``false``
meaning that no action will be taken on empty strings during the COPY FROM
execution.

The option is only supported when using the ``CSV`` format,
otherwise, it will be ignored.

``delimiter``
'''''''''''''

Specifies a single one-byte character that separates columns within each line
of the file. The default delimiter is ``,``.

The option is only supported when using the ``CSV`` format, otherwise, it will
be ignored.

``format``
''''''''''

This option specifies the format of the input file. Available formats are
``csv`` or ``json``. If a format is not specified and the format cannot be
guessed from the file extension, the file will be processed as JSON.

.. _return_summary:

``RETURN SUMMARY``
------------------

By using the optional ``RETURN SUMMARY`` clause, a per-node result set will be
returned containing information about possible failures and successfully
inserted records.

::

    [ RETURN SUMMARY ]

.. rubric:: Schema

+---------------------------------------+------------------------------------------------+---------------+
| Column Name                           | Description                                    |  Return Type  |
+=======================================+================================================+===============+
| ``node``                              | Information about the node that has processed  | ``OBJECT``    |
|                                       | the URI resource.                              |               |
+---------------------------------------+------------------------------------------------+---------------+
| ``node['id']``                        | The id of the node.                            | ``TEXT``      |
+---------------------------------------+------------------------------------------------+---------------+
| ``node['name']``                      | The name of the node.                          | ``TEXT``      |
+---------------------------------------+------------------------------------------------+---------------+
| ``uri``                               | The URI the node has processed.                | ``TEXT``      |
+---------------------------------------+------------------------------------------------+---------------+
| ``error_count``                       | The total number of records which failed.      | ``BIGINT``    |
|                                       | A NULL value indicates a general URI reading   |               |
|                                       | error, the error will be listed inside the     |               |
|                                       | ``errors`` column.                             |               |
+---------------------------------------+------------------------------------------------+---------------+
| ``success_count``                     | The total number of records which were         | ``BIGINT``    |
|                                       | inserted.                                      |               |
|                                       | A NULL value indicates a general URI reading   |               |
|                                       | error, the error will be listed inside the     |               |
|                                       | ``errors`` column.                             |               |
+---------------------------------------+------------------------------------------------+---------------+
| ``errors``                            | Contains detailed information about all        | ``OBJECT``    |
|                                       | errors.                                        |               |
+---------------------------------------+------------------------------------------------+---------------+
| ``errors[ERROR_MSG]``                 | Contains information about a type of an error. | ``OBJECT``    |
+---------------------------------------+------------------------------------------------+---------------+
| ``errors[ERROR_MSG]['count']``        | The number records failed with this error.     | ``BIGINT``    |
+---------------------------------------+------------------------------------------------+---------------+
| ``errors[ERROR_MSG]['line_numbers']`` | The line numbers of the source URI where the   | ``ARRAY``     |
|                                       | error occurred, limited to the first 50        |               |
|                                       | errors, to avoid buffer pressure on clients.   |               |
+---------------------------------------+------------------------------------------------+---------------+

.. _AWS documentation: https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html
.. _AWS Java Documentation: https://docs.aws.amazon.com/AmazonS3/latest/dev/AuthUsingAcctOrUserCredJava.html
.. _Docker volume: https://docs.docker.com/storage/volumes/
.. _GeoJSON: https://geojson.org/
.. _RFC2396: https://www.ietf.org/rfc/rfc2396.txt
.. _URI Scheme: https://en.wikipedia.org/wiki/URI_scheme
.. _URL encoded: https://en.wikipedia.org/wiki/Percent-encoding
.. _URL: https://docs.oracle.com/javase/8/docs/api/java/net/URL.html
.. _Windows documentation: https://docs.microsoft.com/en-us/dotnet/standard/io/file-path-formats
.. _WKT: https://en.wikipedia.org/wiki/Well-known_text
