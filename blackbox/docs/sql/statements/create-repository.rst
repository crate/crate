.. highlight:: psql
.. highlight:: sh
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
  | *Type:*    ``string``
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
  | *Type:*    ``long`` or ``string``
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
  | *Type:*    ``string``
  | *Default:* default filesystem URI for the given Hadoop HDFS configuration

  HDFS uri of the form ``hdfs:// <host>:<port>/``.

**user**
  | *Type:*    ``string``

  The HDFS user as string.

**path**
  | *Type:*    ``string``

  HDFS filesystem path to where the data gets stored.

**load_defaults**
  | *Type:*    ``boolean``
  | *Default:* ``true``

  Whether to load the default Hadoop Configuration.

**conf_location**
  | *Type:*    ``string``

  Comma separated string of files to Hadoop XML configuration files to load.

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
  | *Type:*    ``long`` or ``string``
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
  | *Type:*    ``string``

  Name of the S3 bucket used for storing snapshots. If the bucket
  does not yet exist, a new bucket will be created on S3 (assuming the
  required permissions are set).

**region**
  | *Type:*    ``string``
  | *Default:* ``us-east-1``

  Region where the bucket is located.

**endpoint**
  | *Type:*    ``string``
  | *Default:* Default AWS API endpoint

  Endpoint to the S3 API. Setting a region will override the endpoint setting.

**protocol**
  | *Type:*    ``string``
  | *Values:*  ``http``, ``https``
  | *Default:* ``https``

  Protocol to be used.

**base_path**
  | *Type:*    ``string``

  Path within the bucket to the repository.

**access_key**
  | *Secure setting*
  | *Type:*    ``string``

  Access key used for authentication against AWS. 
  As this is a :ref:`secure setting <conf-secure-settings>` it must be stored 
  in the keystore as so: 

  .. code-block:: sh

     sh$ ./bin/crate-keystore add s3.client.default.access_key

**secret_key**
  | *Secure setting*
  | *Type:*    ``string``

  Secret key used for authentication against AWS.
  As this is a :ref:`secure setting <conf-secure-settings>` it must be stored 
  in the keystore as so: 

  .. code-block:: sh

     sh$ ./bin/crate-keystore add s3.client.default.secret_key

**concurrent_streams**
  | *Type:*    ``integer``
  | *Default:* ``5``

  The number of concurrent streams to use for backup and restore.

**chunk_size**
  | *Type:*    ``long`` or ``string``
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
  | *Type:*    ``string``
  | *Default:* ``5mb``
  | *Minimum:* ``5mb``

  Minimum threshold below which chunks are uploaded with a single request. If
  the threshold is exceeded, the chunks will be split into multiple parts of
  ``buffer_size`` length. Each chunk will be uploaded separately.

**max_retries**
  | *Type:*    ``integer``
  | *Default:* ``3``

  Number of retries in case of errors.

**read_only**
  | *Type:*    ``boolean``
  | *Default:* ``false``

  If set to ``true`` the repository is made read-only.

**canned_acl**
  | *Type:*    ``string``
  | *Values:*  ``private``, ``public-read``, ``public-read-write``,
               ``authenticated-read``, ``log-delivery-write``,
               ``bucket-owner-read``, or ``bucket-owner-full-control``
  | *Default:* ``private``

  When the repository creates buckets and objects, the specified canned ACL is
  added.

.. _ref-create-repository-types-url:

``url``
-------

A read-only repository that points to the location of a
:ref:`ref-create-repository-types-fs` repository via ``http``, ``https``,
``ftp``, ``file`` and ``jar`` urls. It only allows for
:ref:`ref-restore-snapshot` operations.

.. rubric:: Parameters

**read_only**
  | *Type:*    ``string``

  This url must point to the root of the shared
  :ref:`ref-create-repository-types-fs` repository.

  Due to security reasons only whitelisted URLs can be used. URLs can be
  whitelisted in the ``crate.yml`` configuration file. See
  :ref:`ref-configuration-repositories`.
