.. highlight:: psql

.. _sql-copy-from:

=============
``COPY FROM``
=============

You can use the ``COPY FROM`` :ref:`statement <gloss-statement>` to copy data
from a file into a table.

.. SEEALSO::

    :ref:`Data manipulation: Import and export <dml-import-export>`

    :ref:`SQL syntax: COPY TO <sql-copy-to>`

Import and export
=================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

.. _sql-copy-from-synopsis:

Synopsis
========

::

    COPY table_identifier
      [ ( column_ident [, ...] ) ]
      [ PARTITION (partition_column = value [ , ... ]) ]
      FROM uri [ WITH ( option = value [, ...] ) ] [ RETURN SUMMARY ]


.. _sql-copy-from-desc:

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


.. _sql-copy-from-formats:

File formats
------------

CrateDB accepts both JSON and CSV inputs. The format is inferred from the file
extension (``.json`` or ``.csv`` respectively) if possible. The :ref:`format
<sql-copy-from-format>` can also be set as an option. If a format is not
specified and the format cannot be inferred, the file will be processed as
JSON.

Files must be UTF-8 encoded. Any keys in the object will be added as columns,
regardless of the previously defined table. Empty lines are skipped.

JSON files must contain a single JSON object per line.

Example JSON data::

    {"id": 1, "quote": "Don't panic"}
    {"id": 2, "quote": "Ford, you're turning into a penguin. Stop it."}

CSV files must contain a header with comma-separated values, which will be
added as columns.

Example CSV data::

    id,quote
    1,"Don't panic"
    2,"Ford, you're turning into a penguin. Stop it."

Example CSV data with no header::

    1,"Don't panic"
    2,"Ford, you're turning into a penguin. Stop it."

See also: :ref:`dml-importing-data`.


.. _sql-copy-from-type-checks:

Data type checks
----------------

CrateDB checks if the column's data types match the types from the import file.
It casts the types and will always import the data as in the source file.
Furthermore CrateDB will check for all :ref:`column_constraints`.

For example a `WKT`_ string cannot be imported into a column of ``geo_shape``
or ``geo_point`` type, since there is no implicit cast to the `GeoJSON`_ format.

.. NOTE::

   In case the ``COPY FROM`` statement fails, the log output on the node will
   provide an error message. Any data that has been imported until then has
   been written to the table and should be deleted before restarting the
   import.


.. _sql-copy-from-params:

Parameters
==========

.. _sql-copy-from-table_ident:

``table_ident``
  The name (optionally schema-qualified) of an existing table where the data
  should be put.

.. _sql-copy-from-column_ident:

``column_ident``
  Used in an optional columns declaration, each ``column_ident`` is the name of a column in the ``table_ident`` table.

  This currently only has an effect if using the CSV file format. See the ``header`` section for how it behaves.

.. _sql-copy-from-uri:

``uri``
  An expression or array of expressions. Each :ref:`expression
  <gloss-expression>` must :ref:`evaluate <gloss-evaluation>` to a string
  literal that is a `well-formed URI`_.

  URIs must use one of the supported :ref:`URI schemes
  <sql-copy-from-schemes>`. CrateDB supports :ref:`globbing
  <sql-copy-from-globbing>` for the :ref:`file <sql-copy-from-file>` and
  :ref:`s3 <sql-copy-from-s3>` URI schemes.

  .. NOTE::

      If the URI scheme is missing, CrateDB assumes the value is a pathname and
      will prepend the :ref:`file <sql-copy-from-file>` URI scheme (i.e.,
      ``file://``). So, for example, CrateDB will convert ``/tmp/file.json`` to
      ``file:///tmp/file.json``.


.. _sql-copy-from-globbing:

URI globbing
------------

With :ref:`file <sql-copy-from-file>` and :ref:`s3 <sql-copy-from-s3>` URI
schemes, you can use pathname `globbing`_ (i.e., ``*`` wildcards) with the
``COPY FROM`` statement to construct URIs that can match multiple directories
and files.

Suppose you used ``file:///tmp/import_data/*/*.json`` as the URI. This URI
would match all JSON files located in subdirectories of the
``/tmp/import_data`` directory.

So, for example, these files would match:

- ``/tmp/import_data/foo/1.json``
- ``/tmp/import_data/bar/2.json``
- ``/tmp/import_data/1/boz.json``

.. CAUTION::

    A file named ``/tmp/import_data/foo/.json`` would also match the
    ``file:///tmp/import_data/*/*.json`` URI. The ``*`` wildcard matches any
    number of characters, including none.

However, these files would not match:

- ``/tmp/import_data/1.json`` (two few subdirectories)
- ``/tmp/import_data/foo/bar/2.json`` (too many subdirectories)
- ``/tmp/import_data/1/boz.js`` (file extension mismatch)


.. _sql-copy-from-schemes:

URI schemes
-----------

CrateDB supports the following URI schemes:

.. contents::
   :local:
   :depth: 1


.. _sql-copy-from-file:

``file``
''''''''

You can use the ``file://`` scheme to specify an absolute path to one or more
files accessible via the local filesystem of one or more CrateDB nodes.

For example:

.. code-block:: text

    file:///path/to/dir

The files must be accessible on at least one node and the system user running
the ``crate`` process must have read access to every file specified.

By default, every node will attempt to import every file. If the file is
accessible on multiple nodes, you can set the `shared`_ option to true in order
to avoid importing duplicates.

Use :ref:`sql-copy-from-return-summary` to get information about what actions
were performed on each node.

.. TIP::

    If you are running CrateDB inside a container, the file must be inside the
    container. If you are using *Docker*, you may have to configure a `Docker
    volume`_ to accomplish this.

.. TIP::

    If you are using *Microsoft Windows*, you must include the drive letter in
    the file URI.

    For example:

    .. code-block:: text

        file://C:\/tmp/import_data/quotes.json

    Consult the `Windows documentation`_ for more information.


.. _sql-copy-from-s3:

``s3``
''''''

You can use the ``s3://`` scheme to access buckets on the `Amazon Simple
Storage Service`_ (Amazon S3).

For example:

.. code-block:: text

    s3://[<accesskey>:<secretkey>@][<host>:<port>/]<bucketname>/<path>

S3 compatible storage providers can be specified by the optional pair of host
and port, which defaults to Amazon S3 if not provided.

Here is a more concrete example:

.. code-block:: text

    COPY t FROM 's3://accessKey:secretKey@s3.amazonaws.com:443/myBucket/key/a.json' with (protocol = 'https')

If no credentials are set the s3 client will operate in anonymous mode.
See `AWS Java Documentation`_.

Using the ``s3://`` scheme automatically sets the `shared`_ to true.

.. TIP::

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


.. _sql-copy-from-other-schemes:

Other schemes
'''''''''''''

In addition to the schemes above, CrateDB supports all protocols supported by
the `URL`_ implementation of its JVM (typically ``http``, ``https``, ``ftp``,
and ``jar``). Please refer to the documentation of the JVM vendor for an
accurate list of supported protocols.

.. NOTE::

    These schemes *do not* support wildcard expansion.


.. _sql-copy-from-clauses:

Clauses
=======

The ``COPY FROM`` :ref:`statement <gloss-statement>` supports the following
clauses:

.. contents::
   :local:
   :depth: 1


.. _sql-copy-from-partition:

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
``PARTITION`` clause can be used to import data into one partition exclusively.

::

    [ PARTITION ( partition_column = value [ , ... ] ) ]

:partition_column:
  One of the column names used for table partitioning

:value:
  The respective column value.

All :ref:`partition columns <gloss-partition-column>` (specified by the
:ref:`sql-create-table-partitioned-by` clause) must be listed inside the
parentheses along with their respective values using the ``partition_column =
value`` syntax (separated by commas).

Because each partition corresponds to a unique set of :ref:`partition column
<gloss-partition-column>` row values, this clause uniquely identifies a single
partition for import.

.. TIP::

    The :ref:`ref-show-create-table` statement will show you the complete list
    of partition columns specified by the
    :ref:`sql-create-table-partitioned-by` clause.

.. CAUTION::

    Partitioned tables do not store the row values for the partition columns,
    hence every row will be imported into the specified partition regardless of
    partition column values.


.. _sql-copy-from-with:

``WITH``
--------

You can use the optional ``WITH`` clause to specify option values.

::

    [ WITH ( option = value [, ...] ) ]

The ``WITH`` clause supports the following options:

.. contents::
   :local:
   :depth: 1


.. _sql-copy-from-bulk_size:

``bulk_size``
'''''''''''''

CrateDB will process the lines it reads from the ``path`` in bulks. This option
specifies the size of one batch. The provided value must be greater than 0, the
default value is 10000.


.. _sql-copy-from-fail_fast:

``fail_fast``
'''''''''''''

A boolean value indicating if the ``COPY FROM`` operation should abort early
after an error. This is best effort and due to the distributed execution, it
may continue processing some records before it aborts.
Defaults to ``false``.

.. _sql-copy-from-wait_for_completion:

``wait_for_completion``
'''''''''''''''''''''''

A boolean value indicating if the ``COPY FROM`` should wait for
the copy operation to complete. If set to ``false`` the request
returns at once and the copy operation runs in the background.
Defaults to ``true``.

.. _sql-copy-from-shared:

``shared``
''''''''''

This option should be set to true if the URI's location is accessible by more
than one CrateDB node to prevent them from importing the same file.

The default value depends on the scheme of each URI.

If an array of URIs is passed to ``COPY FROM`` this option will overwrite the
default for *all* URIs.


.. _sql-copy-from-node_filters:

``node_filters``
''''''''''''''''

A filter :ref:`expression <gloss-expression>` to select the nodes to run the
*read* operation.

It's an object in the form of::

    {
        name = '<node_name_regex>',
        id = '<node_id_regex>'
    }

Only one of the keys is required.

The ``name`` :ref:`regular expression <gloss-regular-expression>` is applied on
the ``name`` of all execution nodes, whereas the ``id`` regex is applied on the
``node id``.

If both keys are set, *both* regular expressions have to match for a node to be
included.

If the `shared`_ option is false, a strict node filter might exclude nodes with
access to the data leading to a partial import.

To verify which nodes match the filter, run the statement with
:ref:`EXPLAIN <ref-explain>`.


.. _sql-copy-from-num_readers:

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


.. _sql-copy-from-compression:

``compression``
'''''''''''''''

The default value is ``null``, set to ``gzip`` to read gzipped files.


.. _sql-copy-from-protocol:

``protocol``
'''''''''''''''

Used for :ref:`s3 <sql-copy-from-s3>` scheme only. It is set to HTTPS by
default.


.. _sql-copy-from-overwrite_duplicates:

``overwrite_duplicates``
''''''''''''''''''''''''

Default: false

``COPY FROM`` by default won't overwrite rows if a document with the same
primary key already exists. Set to true to overwrite duplicate rows.


.. _sql-copy-from-empty_string_as_null:

``empty_string_as_null``
''''''''''''''''''''''''

If set to ``true`` the ``empty_string_as_null`` option enables conversion of
empty strings into ``NULL``. The default value is ``false`` meaning that no
action will be taken on empty strings during the COPY FROM execution.

The option is only supported when using the ``CSV`` format, otherwise, it will
be ignored.


.. _sql-copy-from-delimiter:

``delimiter``
'''''''''''''

Specifies a single one-byte character that separates columns within each line
of the file. The default delimiter is ``,``.

The option is only supported when using the ``CSV`` format, otherwise, it will
be ignored.


.. _sql-copy-from-format:

``format``
''''''''''

This option specifies the format of the input file. Available formats are
``csv`` or ``json``. If a format is not specified and the format cannot be
guessed from the file extension, the file will be processed as JSON.


.. _sql-copy-from-header:

``header``
''''''''''

Used to indicate if the first line of a CSV file contains a header with the
column names. Defaults to ``true``.

If set to ``false``, the CSV must not contain column names in the first line
and instead the columns declared in the statement are used. If no columns are
declared in the statement, it will default to all columns present in the table
in their ``CREATE TABLE`` declaration order.

If set to ``true`` the first line in the CSV file must contain the column
names. You can use the optional column declaration in addition to import only a
subset of the data.

If the statement contains no column declarations, all fields in the CSV are
read and if it contains fields where there is no matching column in the table,
the behavior depends on the ``column_policy`` table setting. If ``dynamic`` it
implicitly adds new columns, if ``strict`` the operation will fail.

An example of using input file with no header

::

    cr> COPY quotes FROM 'file:///tmp/import_data/quotes.csv' with (format='csv', header=false);
    COPY OK, 3 rows affected (... sec)


.. _sql-copy-from-validation:

``validation``
''''''''''''''

Default: ``true``

Enables or disables parsing and validation of source data.

If set to ``false`` CrateDB will not validate the data in any way. Use this
with caution if you're sure the data is in the right format - for example if
you exported the data using ``COPY TO``.

All constraints - including type value constraints are bypassed. If you import
invalid data this way, subsequent queries can fail.

If the table has generated columns or default expressions, validation
will always take place and the parameter is ignored.


.. _sql-copy-from-return-summary:

``RETURN SUMMARY``
------------------

By using the optional ``RETURN SUMMARY`` clause, a per-node result set will be
returned containing information about possible failures and successfully
inserted records.

::

    [ RETURN SUMMARY ]

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


.. _Amazon Simple Storage Service: https://aws.amazon.com/s3/
.. _AWS documentation: https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html
.. _AWS Java Documentation: https://docs.aws.amazon.com/AmazonS3/latest/dev/AuthUsingAcctOrUserCredJava.html
.. _Docker volume: https://docs.docker.com/storage/volumes/
.. _GeoJSON: https://geojson.org/
.. _globbing: https://en.wikipedia.org/wiki/Glob_(programming)
.. _percent-encoding: https://en.wikipedia.org/wiki/Percent-encoding
.. _URI Scheme: https://en.wikipedia.org/wiki/URI_scheme
.. _URL encoded: https://en.wikipedia.org/wiki/Percent-encoding
.. _URL: https://docs.oracle.com/javase/8/docs/api/java/net/URL.html
.. _well-formed URI: https://www.ietf.org/rfc/rfc2396.txt
.. _Windows documentation: https://docs.microsoft.com/en-us/dotnet/standard/io/file-path-formats
.. _WKT: https://en.wikipedia.org/wiki/Well-known_text
