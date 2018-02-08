.. highlight:: sh
.. _blob_support:

=====
Blobs
=====

CrateDB includes support to store `binary large objects`_. By utilizing
CrateDB's cluster features the files can be replicated and sharded just like
regular data.

.. rubric:: Table of Contents

.. contents::
   :local:

Creating a Table for Blobs
==========================

Before adding blobs a ``blob table`` must be created. Blob tables can be
sharded. This makes it possible to distribute binaries over multiple nodes.
Lets use the CrateDB shell crash to issue the SQL statement::

    sh$ crash -c "create blob table myblobs clustered into 3 shards with (number_of_replicas=0)"
    CREATE OK, 1 row affected (... sec)

Now CrateDB is configured to allow blobs to be management under the
``/_blobs/myblobs`` endpoint.

Custom Location for Storing Blob Data
=====================================

It is possible to define a custom directory path for storing blob data which
can be completely different than the normal data path. Best use case for this
is storing normal data on a fast SSD and blob data on a large cheap spinning
disk.

The custom blob data path can be set either globally by config or while
creating a blob table. The path can be either absolute or relative and must be
creatable/writable by the user CrateDB is running as. A relative path value is
relative to :ref:`env-crate-home`.

Blob data will be stored under this path with the following layout::

  /<blobs.path>/nodes/<NODE_NO>/indices/<INDEX_UUID>/<SHARD_ID>/blobs

Global by Config
----------------

Just uncomment or add following entry at the CrateDB config in order to define a
custom path globally for all blob tables::

  blobs.path: /path/to/blob/data

Also see :ref:`config`.

Per Blob Table Setting
----------------------

It is also possible to define a custom blob data path per table instead of
global by config. Also per table setting take precedence over the config
setting.

See :ref:`ref-create-blob-table` for details.

Creating a blob table with a custom blob data path::

    sh$ crash -c "create blob table myblobs clustered into 3 shards with (blobs_path='/tmp/crate_blob_data')" # doctest: +SKIP
    CREATE OK, 1 row affected (... sec)

List
====

.. Hidden: Add a blob entry to list it afterwards::

    sh$ curl -isSX PUT '127.0.0.1:4200/_blobs/myblobs/4a756ca07e9487f482465a99e8286abc86ba4dc7' -d 'contents'
    HTTP/1.1 201 Created
    content-length: 0

To list all blobs inside a blob table a ``SELECT`` statement can be used::

    sh$ crash -c "select digest, last_modified from blob.myblobs"
    +------------------------------------------+---------------+
    | digest                                   | last_modified |
    +------------------------------------------+---------------+
    | 4a756ca07e9487f482465a99e8286abc86ba4dc7 | ...           |
    +------------------------------------------+---------------+
    SELECT 1 row in set (... sec)

.. NOTE::

    To query blob tables it is necessary to always specify the schema name
    ``blob``.

.. Hidden: Delete the blob entry::

    sh$ curl -isS -XDELETE '127.0.0.1:4200/_blobs/myblobs/4a756ca07e9487f482465a99e8286abc86ba4dc7'
    HTTP/1.1 204 No Content


Altering a Blob Table
=====================

The number of replicas a blob table has can be changed using the ``ALTER BLOB
TABLE`` clause::

    sh$ crash -c "alter blob table myblobs set (number_of_replicas=0)"
    ALTER OK, 1 row affected (... sec)

Deleting a Blob Table
=====================

Blob tables can be deleted similar to normal tables::

    sh$ crash -c "drop blob table myblobs"
    DROP OK, 1 row affected (... sec)

.. Hidden: Re-create the blob table so information_schema will show it::

    sh$ crash -c "create blob table myblobs clustered into 3 shards with (number_of_replicas=0)"
    CREATE OK, 1 row affected (... sec)


Using Blob Tables
=================

The usage of Blob Tables is only supported using the HTTP/HTTPS protocol. This
section describes how binaries can be stored, fetched and deleted.

.. NOTE::

    For the reason of internal optimization any successful request could lead to
    a 307 Temporary Redirect response.


Uploading
---------

To upload a blob the sha1 hash of the blob has to be known upfront since this
has to be used as the ID of the new blob. For this example we use a fancy
Python one-liner to compute the shasum::

    sh$ python -c 'import hashlib;print(hashlib.sha1("contents".encode("utf-8")).hexdigest())'
    4a756ca07e9487f482465a99e8286abc86ba4dc7

The blob can now be uploaded by issuing a PUT request::

    sh$ curl -isSX PUT '127.0.0.1:4200/_blobs/myblobs/4a756ca07e9487f482465a99e8286abc86ba4dc7' -d 'contents'
    HTTP/1.1 201 Created
    content-length: 0

If a blob already exists with the given hash a 409 Conflict is returned::

    sh$ curl -isSX PUT '127.0.0.1:4200/_blobs/myblobs/4a756ca07e9487f482465a99e8286abc86ba4dc7' -d 'contents'
    HTTP/1.1 409 Conflict
    content-length: 0

Download
--------

To download a blob simply use a GET request::

    sh$ curl -sS '127.0.0.1:4200/_blobs/myblobs/4a756ca07e9487f482465a99e8286abc86ba4dc7'
    contents

If the blob doesn't exist a 404 Not Found error is returned::

    sh$ curl -isS '127.0.0.1:4200/_blobs/myblobs/e5fa44f2b31c1fb553b6021e7360d07d5d91ff5e'
    HTTP/1.1 404 Not Found
    content-length: 0

To determine if a blob exists without downloading it, a HEAD request can be
used::

    sh$ curl -sS -I '127.0.0.1:4200/_blobs/myblobs/4a756ca07e9487f482465a99e8286abc86ba4dc7'
    HTTP/1.1 200 OK
    content-length: 8
    accept-ranges: bytes
    expires: Thu, 31 Dec 2037 23:59:59 GMT
    cache-control: max-age=315360000

.. NOTE::

    The cache headers for blobs are static and basically allows clients to
    cache the response forever since the blob is immutable.

Delete
------

To delete a blob simply use a DELETE request::

    sh$ curl -isS -XDELETE '127.0.0.1:4200/_blobs/myblobs/4a756ca07e9487f482465a99e8286abc86ba4dc7'
    HTTP/1.1 204 No Content

If the blob doesn't exist a 404 Not Found error is returned::

    sh$ curl -isS -XDELETE '127.0.0.1:4200/_blobs/myblobs/4a756ca07e9487f482465a99e8286abc86ba4dc7'
    HTTP/1.1 404 Not Found
    content-length: 0


.. _`binary large objects`: http://en.wikipedia.org/wiki/Binary_large_object
