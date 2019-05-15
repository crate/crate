.. highlight:: psql
.. _ref-create-repository:

=====================
``CREATE REPOSITORY``
=====================

Register a new repository used to store, manage and restore snapshots.

.. rubric:: Table of Contents

.. contents::
   :local:

Synopsis
========

::

    CREATE REPOSITORY repository_name TYPE type
    [ WITH (repository_parameter [= value], [, ...]) ]

Description
===========

``CREATE REPOSITORY`` will register a new repository in the cluster.

.. NOTE::

   If the repository configuration points to a location with existing
   snapshots, these are made available to the cluster.

Repositories are declared using a ``repository_name`` and ``type``.

Further configuration parameters are given in the WITH Clause.

.. rubric:: Parameters

:repository_name:
  The name of the repository as identifier

:type:
  The type of the repository, see :ref:`ref-create-repository-types`.

Clauses
=======

``WITH``
--------

::

    [ WITH (repository_parameter [= value], [, ...]) ]

The following configuration parameters apply to repositories of all types.

For further configuration options see the documentation of the repository
``type`` (e.g. type :ref:`ref-create-repository-types-fs`).

:max_restore_bytes_per_sec:
  The maximum rate at which snapshots are restored on a single node from
  this repository.

  Default: ``40mb`` per second.

:max_snapshot_bytes_per_sec:
  The maximum rate at which snapshots are created on a single node to
  this repository.

  Default: ``40mb`` per second.

.. _ref-create-repository-types:

Types
=====

A type determines how and where a repository stores its snapshots.

The supported types are the following. More types are supported via `plugins`_.

.. _plugins: https://github.com/crate/crate/blob/master/devs/docs/plugins.rst

.. _ref-create-repository-types-fs:

``fs``
------

A repository storing its snapshots to a shared filesystem that must be
accessible by all master and data nodes in the cluster.

.. NOTE::

   To create repositories of this type, it's necessary to configure the
   possible locations for repositories inside the ``crate.yml`` file under
   ``path.repo`` as list of strings.

.. rubric:: Parameters

.. _ref-create-repository-types-fs-location:

**location**
  | *Type:*    ``text``
  | *Required*

  An absolute or relative path to the directory where snapshots get stored. If
  the path is relative, it will be appended to the first entry in the
  :ref:`path.repo <conf-path-repo>` setting.

  Windows UNC paths are allowed, if server name and shares are specified and
  backslashes properly escaped.

  Only paths starting with an entry from :ref:`path.repo <conf-path-repo>` are
  possible.

**compress**
  | *Type:*    ``boolean``
  | *Default:* ``true``

  Whether the metadata part of the snapshot should be compressed or not.

  The actual table data is not compressed.

**chunk_size**
  | *Type:*    ``bigint`` or ``text``
  | *Default:* ``null``

  Defines the maximum size of a single file that gets created during snapshot
  creation. If set to ``null`` big files will not be split into smaller chunks.
  The chunk size can be either specified in bytes or using size value notation
  (e.g. ``1g``, ``5m``, or ``9k``).

.. _ref-create-repository-types-hdfs:

``hdfs``
--------

A repository that stores its snapshot inside an HDFS file-system.

.. rubric:: Parameters

**uri**
  | *Type:*    ``text``
  | *Default:* default filesystem URI for the given Hadoop HDFS configuration

  HDFS uri of the form ``hdfs:// <host>:<port>/``.

**security.principal**
  | *Type:*    ``text``

  A qualified kerberos principal used to authenticate against HDFS.

**path**
  | *Type:*    ``text``

  HDFS filesystem path to where the data gets stored.

**load_defaults**
  | *Type:*    ``boolean``
  | *Default:* ``true``

  Whether to load the default Hadoop Configuration.

**conf.<key>**
  | *Type:*    various

  Dynamic config values added to the Hadoop configuration.

**concurrent_streams**
  | *Type:*    ``integer``
  | *Default:* ``5``

  The number of concurrent streams to use for backup and restore.

**compress**
  | *Type:*    ``boolean``
  | *Default:* ``true``

  Whether the metadata part of the snapshot should be compressed or not.

  The actual table data is not compressed.

**chunk_size**
  | *Type:*    ``bigint`` or ``text``
  | *Default:* ``null``

  Defines the maximum size of a single file that gets created during snapshot
  creation. If set to ``null`` big files will not be split into smaller chunks.
  The chunk size can be either specified in bytes or using size value notation
  (e.g. ``1g``, ``5m``, or ``9k``).

.. _ref-create-repository-types-s3:

``s3``
------

A repository that stores its snapshot on the Amazon S3 service.

.. rubric:: Parameters

**bucket**
  | *Type:*    ``text``

  Name of the S3 bucket used for storing snapshots. If the bucket
  does not yet exist, a new bucket will be created on S3 (assuming the
  required permissions are set).

**endpoint**
  | *Type:*    ``text``
  | *Default:* Default AWS API endpoint

  Endpoint to the S3 API. If a specific region is desired, specify it by using
  this setting.

**protocol**
  | *Type:*    ``text``
  | *Values:*  ``http``, ``https``
  | *Default:* ``https``

  Protocol to be used.

**base_path**
  | *Type:*    ``text``

  Path within the bucket to the repository.

**access_key**
  | *Type:*    ``text``
  | *Default:* Value defined through :ref:`s3.client.default.access_key
        <s3-credentials-access-key>` setting.

  Access key used for authentication against AWS.

  .. WARNING::

     If the secret key is set explicitly (not via :ref:`configuration setting
     <s3-credentials-access-key>`) it will be visible in plain text when
     querying the ``sys.repositories`` table.

**secret_key**
  | *Type:*    ``text``
  | *Default:* Value defined through :ref:`s3.client.default.secret_key
     <s3-credentials-secret-key>` setting.

  Secret key used for authentication against AWS.

  .. WARNING::

     If the secret key is set explicitly (not via :ref:`configuration setting
     <s3-credentials-secret-key>`) it will be visible in plain text when
     querying the ``sys.repositories`` table.

**chunk_size**
  | *Type:*    ``bigint`` or ``text``
  | *Default:* ``null``

  Defines the maximum size of a single file that gets created during snapshot
  creation. If set to ``null`` big files will not be split into smaller chunks.
  The chunk size can be either specified in bytes or using size value notation
  (e.g. ``1g``, ``5m``, or ``9k``).

**compress**
  | *Type:*    ``boolean``
  | *Default:* ``true``

  Whether the metadata part of the snapshot should be compressed.

  The actual table data is not compressed.

**server_side_encryption**
  | *Type:*    ``boolean``
  | *Default:* ``false``

  If set to ``true``, files are encrypted on the server side using the
  ``AES256`` algorithm.

**buffer_size**
  | *Type:*    ``text``
  | *Default:* ``5mb``
  | *Minimum:* ``5mb``

  Minimum threshold below which chunks are uploaded with a single request. If
  the threshold is exceeded, the chunks will be split into multiple parts of
  ``buffer_size`` length. Each chunk will be uploaded separately.

**max_retries**
  | *Type:*    ``integer``
  | *Default:* ``3``

  Number of retries in case of errors.

**use_throttle_retries**
  | *Type:*    ``boolean``
  | *Default:* ``true``

  Whether retries should be throttled (ie use backoff).

**read_only**
  | *Type:*    ``boolean``
  | *Default:* ``false``

  If set to ``true`` the repository is made read-only.

**canned_acl**
  | *Type:*    ``text``
  | *Values:*  ``private``, ``public-read``, ``public-read-write``,
               ``authenticated-read``, ``log-delivery-write``,
               ``bucket-owner-read``, or ``bucket-owner-full-control``
  | *Default:* ``private``

  When the repository creates buckets and objects, the specified canned ACL is
  added.

.. _ref-create-repository-types-azure:

``azure``
---------

A repository type that stores its snapshots on the Azure Storage service.

.. rubric:: Parameters

**container**
  | *Type:*    ``text``
  | *Default:* ``crate-snapshots``

  The Azure Storage container name. You must create the Azure Storage container
  before creating the repository.

**base_path**
  | *Type:* ``text``

  The path within the Azure Storage container to repository data.

**chunk_size**
  | *Type:*    ``bigint`` or ``text``
  | *Default:* ``256mb``
  | *Maximum:* ``256mb``
  | *Minimum:* ``1b``

  Defines the maximum size of a single file that gets created during snapshot
  creation. The chunk size can be either specified in bytes or using size value
  notation (e.g. ``128mb``, ``5m``, or ``9k``).

**compress**
  | *Type:*    ``boolean``
  | *Default:* ``true``

  When set to true metadata files are stored in compressed format.
  The actual table data is not compressed.

**readonly**
  | *Type:*    ``boolean``
  | *Default:* ``false``

  If set to ``true`` the repository is made read-only.

**location_mode**
  | *Type:*    ``text``
  | *Values:*  ``primary_only``, ``secondary_only``
  | *Default:* ``primary_only``

  The location mode for storing data on the Azure Storage.
  Note that if you set it to ``secondary_only``, it will force readonly to true.

Azure Client Settings
.....................

All the setting values are specified via ``azure.client.default.``

**account**
  | *Type:*    ``text``

  The Azure Storage account name.

**key**
  | *Type:*    ``text``

  The Azure Storage account secret key.

**endpoint_suffix**
  | *Type:*    ``text``
  | *Default:* ``core.windows.net``

  The Azure Storage account endpoint suffix.

**max_retries**
  | *Type:*    ``integer``
  | *Default:* ``3``

  The number of retries in case of failures before considering the
  snapshot to be failed.

**timeout**
  | *Type:*    ``text``
  | *Default:* ``30s``

  The initial backoff period. Time to wait before retrying after a first
  timeout or failure.

**proxy.type**
  | *Type:*    ``text``
  | *Values:* ``http``, ``socks``, or ``direct``
  | *Default:* ``direct``

  The type of the proxy to connect to the Azure Storage account through.

**proxy.host**
  | *Type:* ``text``

  The host name of a proxy to connect to the Azure Storage account through.

**proxy.port**
  | *Type:* ``integer``
  | *Default:* ``0``

  The port of a proxy to connect to the Azure Storage account through.

.. _ref-create-repository-types-url:

``url``
-------

A read-only repository that points to the location of a
:ref:`ref-create-repository-types-fs` repository via ``http``, ``https``,
``ftp``, ``file`` and ``jar`` urls. It only allows for
:ref:`ref-restore-snapshot` operations.

.. rubric:: Parameters

**read_only**
  | *Type:*    ``text``

  This url must point to the root of the shared
  :ref:`ref-create-repository-types-fs` repository.

  Due to security reasons only whitelisted URLs can be used. URLs can be
  whitelisted in the ``crate.yml`` configuration file. See
  :ref:`ref-configuration-repositories`.
