.. highlight:: psql
.. _copy_from:

=============
``COPY FROM``
=============

Copy data from files into a table.

.. rubric:: Table of Contents

.. contents::
   :local:

Synopsis
========

::

    COPY table_ident [ PARTITION (partition_column = value [ , ... ]) ]
    FROM uri [ WITH ( option = value [, ...] ) ]

where ``option`` can be one of:

- ``bulk_size`` *integer*
- ``shared`` *boolean*
- ``num_readers`` *integer*
- ``compression`` *string*
- ``overwrite_duplicates`` *boolean*

Description
===========

``COPY FROM`` copies data from a URI to the specified table as a raw data
import.

The nodes in the cluster will attempt to read the files available at the URI
and import the data. These files have to be UTF-8 encoded and contain a single
JSON object per line. Any keys in the object will be added as columns,
regardless of the previously defined table. Empty lines are simply sikpped.

Example JSON data::

    {"id": 1, "quote": "Don't panic"}
    {"id": 2, "quote": "Ford, you're turning into a penguin. Stop it."}

See also: :ref:`importing_data`.

Type Casts and Constraints
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

    /tmp folder/file.json

Is converted to:

.. code-block:: text

    'file:///tmp%20folder/file.json'

Supported Schemes
-----------------

``file``
........

The provided (absolute) path should point to files available *on at least one
node* with read access to the CrateDB process (with its default user 'crate')
there.

By default each node will attempt to read the files specified. In case the URI
points to a shared folder (where other CrateDB nodes also have access) the
``shared`` option must be set to true in order to avoid importing duplicates.

.. _copy_from_s3:

``s3``
......

Can be used to access buckets on the Amazon AWS S3 Service:

.. code-block:: text

    s3://[<accesskey>:<secretkey>@]<bucketname>/<path>

If ``accesskey`` and ``secretkey`` are ommited, CrateDB attempts to load the
credentials from the environment or Java settings.

Environment Variables - ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY``

Java System Properties - ``aws.accessKeyId`` and ``aws.secretKey``

If no credentials are set the s3 client will operate in anonymous mode, see
`AWS Java Documentation`_.

Using the 's3://' URI scheme sets the ``shared`` option implicitly.

.. NOTE::

   A ``secretkey`` provided by Amazon Web Services can contain characters such
   as '/', '+' or '='. These characters must be `URL encoded`_. For a detailed
   explanation read the official `AWS documentation`_.

.. NOTE::

   Versions prior to 0.51.x use HTTP for connections to S3. Since 0.51.x these
   connections are using the HTTPS protocol. Please make sure you update your
   firewall rules to allow outgoing connections on port ``443``.

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

If the ``shared`` option if false, a strict node filter might exclude nodes
with access to the data leading to a partial import.

To verify which nodes match the filter, run the statement with
:doc:`EXPLAIN <explain>`.

``num_readers``
'''''''''''''''

The number of nodes that will read the resources specified in the URI. Defaults
to the number of nodes available in the cluster. If the option is set to a
number greater than the number of available nodes it will still use each node
only once to do the import. However, the value must be an integer greater than
0.

If ``shared`` is set to false this option has to be used with caution. It might
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

.. _`AWS documentation`: http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html
.. _`AWS Java Documentation`: http://docs.aws.amazon.com/AmazonS3/latest/dev/AuthUsingAcctOrUserCredJava.html
.. _`RFC2396`: http://www.ietf.org/rfc/rfc2396.txt
.. _`URI Scheme`: https://en.wikipedia.org/wiki/URI_scheme
.. _GeoJSON: http://geojson.org/
.. _WKT: http://en.wikipedia.org/wiki/Well-known_text
.. _URL: http://docs.oracle.com/javase/8/docs/api/java/net/URL.html
.. _`URL encoded`: https://en.wikipedia.org/wiki/Percent-encoding
