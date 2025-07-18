.. highlight:: psql

.. _sql-copy-to:

===========
``COPY TO``
===========

You can use the ``COPY TO`` :ref:`statement <gloss-statement>` to export table
data to a file.

.. SEEALSO::

    :ref:`Data manipulation: Import and export <dml-import-export>`

    :ref:`SQL syntax: COPY FROM <sql-copy-from>`


.. _sql-copy-to-synopsis:

Synopsis
========

::

    COPY table_ident [ PARTITION ( partition_column = value [ , ... ] ) ]
                     [ ( column [ , ...] ) ]
                     [ WHERE condition ]
                     TO DIRECTORY output_uri
                     [ WITH ( copy_parameter [= value] [, ... ] ) ]


.. _sql-copy-to-desc:

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

   The ``COPY`` statements use :ref:`Overload Protection <overload_protection>` to ensure other
   queries can still perform. Please change these settings during large inserts if needed.

.. _sql-copy-to-params:

Parameters
==========

.. _sql-copy-to-table_ident:

``table_ident``
  The name (optionally schema-qualified) of the table to be exported.

.. _sql-copy-to-column:

``column``
  (optional) A list of column :ref:`expressions <gloss-expression>` that should
  be exported. E.g.

  ::

    cr> COPY quotes (quote, author) TO DIRECTORY '/tmp/';
    COPY OK, 3 rows affected ...


  .. NOTE::

      When declaring columns, this changes the output to JSON list format,
      which is currently not supported by the ``COPY FROM`` statement.


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


.. _sql-copy-to-where:

``WHERE``
---------

The ``WHERE`` clauses use the same syntax as ``SELECT`` statements, allowing
partial exports. (see :ref:`sql_dql_where_clause` for more information).


Example of using ``WHERE`` clause with
:ref:`comparison operators <comparison-operators-where>` for partial export:

::

  cr> COPY quotes WHERE category = 'philosophy' TO DIRECTORY '/tmp/';
  COPY OK, 3 rows affected ...


.. _sql-copy-to-to:

``TO``
------

The ``TO`` clause allows you to specify an output location.

::

    TO DIRECTORY output_uri


.. _sql-copy-to-to-params:

Parameters
''''''''''

``output_uri``
  An :ref:`expression <gloss-expression>` must :ref:`evaluate
  <gloss-evaluation>` to a string literal that is a `well-formed URI`_. URIs
  must use one of the supported :ref:`URI schemes <sql-copy-from-schemes>`.

  .. NOTE::

      If the URI scheme is missing, CrateDB assumes the value is a pathname and
      will prepend the :ref:`file <sql-copy-from-file>` URI scheme (i.e.,
      ``file://``). So, for example, CrateDB will convert ``/tmp/file.json`` to
      ``file:///tmp/file.json``.


.. _sql-copy-to-schemes:

URI schemes
-----------

CrateDB supports the following URI schemes:

.. contents::
   :local:
   :depth: 1


.. _sql-copy-to-file:

``file``
''''''''

You can use the ``file://`` scheme to specify an absolute path to an output
location on the local file system.

For example:

.. code-block:: text

    file:///path/to/dir

.. TIP::

    If you are running CrateDB inside a container, the location must be inside
    the container. If you are using *Docker*, you may have to configure a
    `Docker volume`_ to accomplish this.

.. TIP::

    If you are using *Microsoft Windows*, you must include the drive letter in
    the file URI.

    For example:

    .. code-block:: text

        file://C:\/tmp/import_data/quotes.json

    Consult the `Windows documentation`_ for more information.


.. _sql-copy-to-s3:

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

    COPY t TO DIRECTORY 's3://myAccessKey:mySecretKey@s3.amazonaws.com:80/myBucket/key1' with (protocol = 'http')

If no credentials are set the s3 client will operate in anonymous mode.
See `AWS Java Documentation`_.

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

.. _sql-copy-to-az:

``az``
''''''

You can use the ``az://`` scheme to access files on the `Azure Blob Storage`_.

URI must look like ``az:://<account>.<endpoint_suffix>/<container>/<blob_path>``.

For example:

.. code-block:: text

    az://myaccount.blob.core.windows.net/my-container/dir1/dir2/file1.json

One of the authentication parameters (:ref:`sql-copy-to-key` or :ref:`sql-copy-to-sas-token`)
must be provided in the ``WITH`` clause.

Protocol can be provided in the ``WITH`` clause, otherwise ``https`` is used by default.

For example:

.. code-block:: text

    COPY source
    TO DIRECTORY 'az://myaccount.blob.core.windows.net/my-container/dir1/dir2/file1.json'
    WITH (
        key = 'key'
    )

.. _sql-copy-to-with:

``WITH``
--------

You can use the optional ``WITH`` clause to specify copy parameter values.

::

    [ WITH ( copy_parameter [= value] [, ... ] ) ]


The ``WITH`` clause supports the following copy parameters:

.. contents::
   :local:
   :depth: 1


.. _sql-copy-to-compression:

**compression**
  | *Type:*    ``text``
  | *Values:*  ``gzip``
  | *Default:* By default the output is not compressed.
  | *Optional*

  Define if and how the exported data should be compressed.

.. _sql-copy-to-protocol:

**protocol**
  | *Type:*    ``text``
  | *Values:*  ``http``, ``https``
  | *Default:* ``https``
  | *Optional*

  Protocol to use.
  Used only by the :ref:`s3 <sql-copy-to-s3>` and :ref:`az <sql-copy-to-az>` schemes.

.. _sql-copy-to-format:

**format**
  | *Type:*    ``text``
  | *Values:*  ``json_object``, ``json_array``
  | *Default:* Depends on defined columns. See description below.
  | *Optional*

  Possible values for the ``format`` settings are:

  ``json_object``
    Each row in the result set is serialized as JSON object and written to an
    output file where one line contains one object. This is the default behavior
    if no columns are defined. Use this format to import with
    :ref:`COPY FROM <sql-copy-from>`.

  ``json_array``
    Each row in the result set is serialized as JSON array, storing one array per
    line in an output file. This is the default behavior if columns are defined.


.. _sql-copy-to-wait_for_completion:

**wait_for_completion**
  | *Type:*    ``boolean``
  | *Default:* ``true``
  | *Optional*

  A boolean value indicating if the ``COPY TO`` should wait for
  the copy operation to complete. If set to ``false`` the request
  returns at once and the copy operation runs in the background.

.. _sql-copy-to-key:

**key**
  | *Type:*    ``text``
  | *Optional*

  Used for :ref:`azblob <sql-copy-to-az>` scheme only.
  The Azure Storage `Account Key`_.

  .. NOTE::

      It must be provided if :ref:`sql-copy-to-sas-token` is not provided.

.. _sql-copy-to-sas-token:

**sas_token**
  | *Type:*    ``text``
  | *Optional*

  Used for :ref:`azblob <sql-copy-to-az>` scheme only.
  The Shared Access Signatures (`SAS`_) token used for authentication for the
  Azure Storage account. This can be used as an alternative to the The Azure
  Storage `Account Key`_.

  The SAS token must have read, write, and list permissions for the
  container base path and all its contents. These permissions need to be
  granted for the blob service and apply to resource types service, container,
  and object.

  .. NOTE::

      It must be provided if :ref:`sql-copy-to-key` is not provided.


.. _Amazon S3: https://aws.amazon.com/s3/
.. _Amazon Simple Storage Service: https://aws.amazon.com/s3/
.. _AWS documentation: https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html
.. _AWS Java Documentation: https://docs.aws.amazon.com/AmazonS3/latest/dev/AuthUsingAcctOrUserCredJava.html
.. _Azure Blob Storage: https://learn.microsoft.com/en-us/azure/storage/blobs/
.. _SAS: https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview
.. _Account Key: https://learn.microsoft.com/en-us/purview/sit-defn-azure-storage-account-key-generic#format
.. _Docker volume: https://docs.docker.com/storage/volumes/
.. _gzip: https://www.gzip.org/
.. _NFS: https://en.wikipedia.org/wiki/Network_File_System
.. _URL encoded: https://en.wikipedia.org/wiki/Percent-encoding
.. _well-formed URI: https://www.rfc-editor.org/rfc/rfc2396
.. _Windows documentation: https://docs.microsoft.com/en-us/dotnet/standard/io/file-path-formats
