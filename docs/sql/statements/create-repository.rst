.. highlight:: psql

.. _sql-create-repository:

=====================
``CREATE REPOSITORY``
=====================


You can use the ``CREATE REPOSITORY`` :ref:`statement <gloss-statement>` to
register a new repository that you can use to create, manage, and restore
:ref:`snapshots <snapshot-restore>`.

.. SEEALSO::

    :ref:`DROP REPOSITORY <sql-drop-repository>`

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


.. _sql-create-repo-synopsis:

Synopsis
========

::

    CREATE REPOSITORY repository_name TYPE type
    [ WITH (parameter_name [= value], [, ...]) ]


.. _sql-create-repo-desc:

Description
===========

The ``CREATE REPOSITORY`` statement creates a repository with a
:ref:`repository name <sql-create-repo-repository_name>` and :ref:`repository
type <sql-create-repo-type>`. You can configure the different :ref:`types
<sql-create-repo-types>` of repositories :ref:`WITH <sql-create-repo-with>`
additional parameters.

.. NOTE::

    If the back-end data storage (specific to the :ref:`repository type
    <sql-create-repo-types>`) already contains CrateDB snapshots, they will
    become available to the cluster.

.. SEEALSO::

    :ref:`System information: Repositories <sys-repositories>`

.. vale off

.. _sql-create-repo-params:

Parameters
----------

.. _sql-create-repo-repository_name:

**repository_name**
  The name of the repository to register.

.. _sql-create-repo-type:

**type**
  The :ref:`repository type <sql-create-repo-types>`.

.. CAUTION::

    You cannot change any repository parameters after creating the repository
    (including parameters set by the :ref:`WITH <sql-create-repo-with>`
    clause).

    Suppose you want to use new parameters for an existing repository. In that
    case, you must first drop the repository using the :ref:`DROP REPOSITORY
    <sql-drop-repository>` statement and then recreate it with a new ``CREATE
    REPOSITORY`` statement.

    When you drop a repository, CrateDB deletes the corresponding record from
    :ref:`sys.repositories <sys-repositories>` but does not delete any
    snapshots from the corresponding backend data storage. If you create a new
    repository using the same backend data storage, any existing snapshots will
    become available again.


.. _sql-create-repo-clauses:

Clauses
=======


.. _sql-create-repo-with:

``WITH``
--------

You can use the ``WITH`` clause to specify one or more repository parameter
values:

::

    [ WITH (parameter_name [= value], [, ...]) ]


.. _sql-create-repo-with-params:

Parameters
''''''''''

The following parameters apply to all repository types:

.. _sql-create-repo-max_restore_bytes_per_sec:

**max_restore_bytes_per_sec**
  The maximum rate (bytes per second) at which a single CrateDB node will read
  snapshot data from this repository.

  Default: ``40mb``

.. _sql-create-repo-max_snapshot_bytes_per_sec:

**max_snapshot_bytes_per_sec**
  The maximum rate (bytes per second) at which a single CrateDB node will write
  snapshot data to this repository.

  Default: ``40mb``

All other parameters (see the :ref:`next section <sql-create-repo-types>`) are
specific to the repository type.


.. _sql-create-repo-types:

Types
=====

CrateDB includes built-in support for the following types:

.. contents::
   :local:
   :depth: 1

CrateDB can support additional types via `plugins`_.


.. _sql-create-repo-fs:

``fs``
------

An ``fs`` repository stores snapshots on the local file system. If a cluster
has multiple nodes, you must use a shared data storage volume mounted locally
on all master nodes and data nodes.

.. NOTE::

   To create ``fs`` repositories, you must configure the list of allowed file
   system paths using the :ref:`path.repo <path.repo>` setting.


.. _sql-create-repo-fs-params:

Parameters
''''''''''

.. _sql-create-repo-fs-location:

**location**
  | *Type:*    ``text``
  | *Required*

  An absolute or relative path to the directory where CrateDB will store
  snapshots. If the path is relative, CrateDB will append it to the first entry
  in the :ref:`path.repo <path.repo>` setting.

  Windows UNC paths are allowed if the server name and shares are specified and
  backslashes are escaped.

  The path must be allowed by the :ref:`path.repo <path.repo>` setting.

.. _sql-create-repo-fs-compress:

**compress**
  | *Type:*    ``boolean``
  | *Default:* ``true``

  Whether CrateDB should compress the metadata part of the snapshot or not.

  CrateDB does not compress the actual table data.

.. _sql-create-repo-fs-chunk_size:

**chunk_size**
  | *Type:*    ``bigint`` or ``text``
  | *Default:* ``null``

  Defines the maximum size of any single file that comprises the snapshot. If
  set to ``null``, CrateDB will not split big files into smaller chunks.  You
  can specify the chunk size with units (e.g., ``1g``, ``5m``, or ``9k``). If
  no unit is specified, the unit defaults to bytes.


.. _sql-create-repo-s3:

``s3``
------


An ``s3`` repository stores snapshot on the `Amazon Simple Storage Service`_
(Amazon S3).

.. NOTE::

    If you are using Amazon S3 in conjunction with `IAM roles`_, the
    ``access_key`` and ``secret_key`` parameters must be left undefined.

    Additionally, make sure to `attach the IAM to each EC2 instance`_ that will
    run a CrateDB master node or data node. The attached IAM role will provide
    the necessary credentials when required.


.. _sql-create-repo-s3-params:

Parameters
''''''''''

.. _sql-create-repo-s3-access_key:

**access_key**
  | *Type:*    ``text``
  | *Required:* ``false``

  Access key used for authentication against `Amazon Web Services`_ (AWS).

  .. NOTE::

      CrateDB masks this parameter. You cannot query the parameter value from
      the :ref:`sys.repositories <sys-repositories>` table.

.. _sql-create-repo-s3-secret_key:

**secret_key**
  | *Type:*    ``text``
  | *Required:* ``false``

  The secret key used for authentication against AWS.

  .. NOTE::

      CrateDB masks this parameter. You cannot query the parameter value from
      the :ref:`sys.repositories <sys-repositories>` table.

.. _sql-create-repo-s3-endpoint:

**endpoint**
  | *Type:*    ``text``
  | *Default:* The default AWS API endpoint

  The `AWS API endpoint`_ to use.

  .. TIP::

      You can specify a `regional endpoint`_ to force the use of a specific
      `AWS region`_.

.. _sql-create-repo-s3-protocol:

**protocol**
  | *Type:*    ``text``
  | *Values:*  ``http``, ``https``
  | *Default:* ``https``

  Protocol to use.

.. _sql-create-repo-s3-bucket:

**bucket**
  | *Type:*    ``text``

  Name of the Amazon S3 bucket used for storing snapshots.

  If the bucket does not yet exist, CrateDB will attempt to create a new bucket
  on Amazon S3.

.. _sql-create-repo-s3-base_path:

**base_path**
  | *Type:*    ``text``
  | *Default:* ``root directory``

  The bucket path to use for snapshots.

  The path is relative, so the ``base_path`` value must not start with a ``/``
  character.

.. _sql-create-repo-s3-compress:

**compress**
  | *Type:*    ``boolean``
  | *Default:* ``true``

  Whether CrateDB should compress the metadata part of the snapshot or not.

  CrateDB does not compress the actual table data.

.. _sql-create-repo-s3-chunk_size:

**chunk_size**
  | *Type:*    ``bigint`` or ``text``
  | *Default:* ``null``

  Defines the maximum size of any single file that comprises the snapshot. If
  set to ``null``, CrateDB will not split big files into smaller chunks.  You
  can specify the chunk size with units (e.g., ``1g``, ``5m``, or ``9k``). If
  no unit is specified, the unit defaults to bytes.

.. _sql-create-repo-s3-readonly:

**readonly**
  | *Type:*    ``boolean``
  | *Default:* ``false``

  If ``true``, the repository is read-only.

.. _sql-create-repo-s3-server_side_encryption:

**server_side_encryption**
  | *Type:*    ``boolean``
  | *Default:* ``false``

  If ``true``, files are server-side encrypted by AWS using the ``AES256``
  algorithm.

.. _sql-create-repo-s3-buffer_size:

**buffer_size**
  | *Type:*    ``text``
  | *Default:* ``5mb``
  | *Minimum:* ``5mb``

  If a chunk is smaller than ``buffer_size``, CrateDB will upload the chunk
  with a single request.

  If a chunk exceeds ``buffer_size``, CrateDB will split the chunk into
  multiple parts of ``buffer_size`` length and upload them separately.

.. _sql-create-repo-s3-max_retries:

**max_retries**
  | *Type:*    ``integer``
  | *Default:* ``3``

  The number of retries in case of errors.

.. _sql-create-repo-s3-use_throttle_retries:

**use_throttle_retries**
  | *Type:*    ``boolean``
  | *Default:* ``true``

  Whether CrateDB should throttle retries (i.e., should back off).

.. _sql-create-repo-s3-canned_acl:

**canned_acl**
  | *Type:*    ``text``
  | *Values:*  ``private``, ``public-read``, ``public-read-write``,
               ``authenticated-read``, ``log-delivery-write``,
               ``bucket-owner-read``, or ``bucket-owner-full-control``
  | *Default:* ``private``

  When CrateDB creates new buckets and objects, the specified `Canned ACL`_ is
  added.

.. _sql-create-repo-s3-storage_class:

**storage_class**
  | *Type:*    ``text``
  | *Values:*  ``standard``, ``reduced_redundancy`` or ``standard_ia``
  | *Default:* ``standard``

  The S3 storage class used for objects stored in the repository. This only 
  affects the S3 storage class used for newly created objects in the repository.

.. _sql-create-repo-azure:

``azure``
---------

An ``azure`` repository stores snapshots on the `Azure Blob storage`_ service.


.. _sql-create-repo-azure-params:

Parameters
''''''''''

.. _sql-create-repo-azure-account:

**account**
  | *Type:*    ``text``

  The Azure Storage account name.

  .. NOTE::

      CrateDB masks this parameter. You cannot query the parameter value from
      the :ref:`sys.repositories <sys-repositories>` table.

.. _sql-create-repo-azure-key:

**key**
  | *Type:*    ``text``

  The Azure Storage account secret key.

  .. NOTE::

      CrateDB masks this parameter. You cannot query the parameter value from
      the :ref:`sys.repositories <sys-repositories>` table.

.. _sql-create-repo-azure-endpoint:

**endpoint**
  | *Type:*    ``text``

  The Azure Storage account endpoint.

  .. TIP::

      You can use an `sql-create-repo-azure-endpoint`_ to address Azure Storage
      instances served on private endpoints.

  .. NOTE::

      ``endpoint`` cannot be used in combination with
      `sql-create-repo-azure-endpoint_suffix`_.

.. _sql-create-repo-azure-secondary_endpoint:

**secondary_endpoint**
  | *Type:*    ``text``

  The Azure Storage account secondary endpoint.

  .. NOTE::

      ``secondary_endpoint`` cannot be used if
      `sql-create-repo-azure-endpoint`_ is not specified.

.. _sql-create-repo-azure-endpoint_suffix:

**endpoint_suffix**
  | *Type:*    ``text``
  | *Default:* ``core.windows.net``

  The Azure Storage account endpoint suffix.

  .. TIP::

      You can use an `endpoint suffix`_ to force the use of a specific
      `Azure service region`_.

.. _sql-create-repo-azure-container:

**container**
  | *Type:*    ``text``
  | *Default:* ``crate-snapshots``

  The blob container name.

  .. NOTE::

      You must create the container before creating the repository.

.. _sql-create-repo-azure-base_path:

**base_path**
  | *Type:* ``text``
  | *Default:* ``root directory``

  The container path to use for snapshots.

.. _sql-create-repo-azure-compress:

**compress**
  | *Type:*    ``boolean``
  | *Default:* ``true``

  Whether CrateDB should compress the metadata part of the snapshot or not.

  CrateDB does not compress the actual table data.

.. _sql-create-repo-azure-chunk_size:

**chunk_size**
  | *Type:*    ``bigint`` or ``text``
  | *Default:* ``256mb``
  | *Maximum:* ``256mb``
  | *Minimum:* ``1b``

  Defines the maximum size of any single file that comprises the snapshot. If
  set to ``null``, CrateDB will not split big files into smaller chunks.  You
  can specify the chunk size with units (e.g., ``1g``, ``5m``, or ``9k``). If
  no unit is specified, the unit defaults to bytes.

.. _sql-create-repo-azure-readonly:

**readonly**
  | *Type:*    ``boolean``
  | *Default:* ``false``

  If ``true``, the repository is read-only.

.. _sql-create-repo-azure-location_mode:

**location_mode**
  | *Type:*    ``text``
  | *Values:*  ``primary_only``, ``secondary_only``, ``primary_then_secondary``,
               ``secondary_then_primary``
  | *Default:* ``primary_only``

  The location mode for storing blob data.

  .. NOTE::

      If you set ``location_mode`` to ``secondary_only``, ``readonly`` will be
      forced to ``true``.

.. _sql-create-repo-azure-max_retries:

**max_retries**
  | *Type:*    ``integer``
  | *Default:* ``3``

  The number of retries (in the case of failures) before considering the
  snapshot to be failed.

.. _sql-create-repo-azure-timeout:

**timeout**
  | *Type:*    ``text``
  | *Default:* ``30s``

  The client side timeout for any single request to Azure.

.. _sql-create-repo-azure-proxy_type:

**proxy_type**
  | *Type:*    ``text``
  | *Values:* ``http``, ``socks``, or ``direct``
  | *Default:* ``direct``

  The type of proxy to use when connecting to Azure.

.. _sql-create-repo-azure-proxy_host:

**proxy_host**
  | *Type:* ``text``

  The hostname of the proxy.

.. _sql-create-repo-azure-proxy_port:

**proxy_port**
  | *Type:* ``integer``
  | *Default:* ``0``

  The port number of the proxy.


.. _sql-create-repo-url:

``url``
-------

A ``url`` repository provides read-only access to an :ref:`fs
<sql-create-repo-fs>` repository via one of the :ref:`supported network access
protocols <repositories.url.supported_protocols>`.

You can use a ``url`` repository to :ref:`restore snapshots
<sql-restore-snapshot>`.


.. _sql-create-repo-url-params:

Parameters
''''''''''

.. _sql-create-repo-url-readonly:

**url**
  | *Type:*    ``text``

  The root URL of the :ref:`fs <sql-create-repo-fs>` repository.

  .. NOTE::

      The URL must match one of the URLs configured by the
      :ref:`repositories.url.allowed_urls <repositories.url.allowed_urls>`
      setting.

.. vale on


.. _Amazon Simple Storage Service: https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html
.. _Amazon Web Services: https://aws.amazon.com/
.. _attach the IAM to each EC2 instance: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html
.. _AWS API endpoint: https://docs.aws.amazon.com/general/latest/gr/rande.html
.. _AWS region: https://aws.amazon.com/about-aws/global-infrastructure/regional-product-services/
.. _Azure Blob storage: https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction
.. _Azure service region: https://azure.microsoft.com/en-us/global-infrastructure/geographies/
.. _Canned ACL: https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl
.. _endpoint suffix: https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string#create-a-connection-string-with-an-endpoint-suffix
.. _IAM roles: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html
.. _plugins: https://github.com/crate/crate/blob/master/devs/docs/plugins.rst
.. _regional endpoint: https://docs.aws.amazon.com/general/latest/gr/rande.html#regional-endpoints
