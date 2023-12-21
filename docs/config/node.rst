.. highlight:: sh
.. vale off

.. _conf-node-settings:

======================
Node-specific settings
======================

.. rubric:: Table of contents

.. contents::
   :local:

Basics
======

.. _cluster.name:

**cluster.name**
  | *Default:*    ``crate``
  | *Runtime:*   ``no``

  The name of the CrateDB cluster the node should join to.

.. _node.name:

**node.name**
  | *Runtime:* ``no``

  The name of the node. If no name is configured a random one will be
  generated.

  .. NOTE::

      Node names must be unique in a CrateDB cluster.

.. _node.store_allow_mmap:

**node.store.allow_mmap**
  | *Default:*    ``true``
  | *Runtime:*   ``no``

  The setting indicates whether or not memory-mapping is allowed.

Node types
==========

CrateDB supports different types of nodes.

The following settings can be used to differentiate nodes upon startup:

.. _node.master:

**node.master**
  | *Default:* ``true``
  | *Runtime:* ``no``

  Whether or not this node is able to get elected as *master* node in the
  cluster.

.. _node.data:

**node.data**
  | *Default:* ``true``
  | *Runtime:* ``no``

  Whether or not this node will store data.

Using different combinations of these two settings, you can create four
different types of node. Each type of node is differentiated by what types of
load it will handle.

Tabulating the truth values for ``node.master`` and ``node.data`` produces a
truth table outlining the four different types of node:

+---------------+-----------------------------+------------------------------+
|               | **Master**                  | **No master**                |
+---------------+-----------------------------+------------------------------+
| **Data**      | Handle all loads.           | Handles client requests and  |
|               |                             | query execution.             |
+---------------+-----------------------------+------------------------------+
| **No data**   | Handles cluster management. | Handles client requests.     |
+---------------+-----------------------------+------------------------------+

Nodes marked as ``node.master`` will only handle cluster management if they are
elected as the cluster master. All other loads are shared equally.


General
=======

.. _node.sql.read_only:

**node.sql.read_only**
  | *Default:* ``false``
  | *Runtime:* ``no``

  If set to ``true``, the node will only allow SQL statements which are
  resulting in read operations.


.. _statement_timeout:

**statement_timeout**
  | *Default:* ``0``
  | *Runtime:* ``yes``

  The maximum duration of any statement before it gets cancelled.

  This value is used as default value for the :ref:`statement_timeout session
  setting <conf-session-statement-timeout>`

  If ``0`` queries are allowed to run infinitely and don't get cancelled
  automatically.

.. NOTE::

   Updating this setting won't affect existing sessions, it will only take
   effect for new sessions.


Networking
==========

.. _conf_hosts:

Hosts
-----

.. _network.host:

**network.host**
  | *Default:*   ``_local_``
  | *Runtime:*   ``no``

  The IP address CrateDB will bind itself to. This setting sets both the
  `network.bind_host`_ and `network.publish_host`_ values.

.. _network.bind_host:

**network.bind_host**
  | *Default:*   ``_local_``
  | *Runtime:*   ``no``

  This setting determines to which address CrateDB should bind itself to.

.. _network.publish_host:

**network.publish_host**
  | *Default:*   ``_local_``
  | *Runtime:*   ``no``

  This setting is used by a CrateDB node to publish its own address to the rest
  of the cluster.

.. TIP::

    Apart from IPv4 and IPv6 addresses there are some special values that can
    be used for all above settings:

    =========================  =================================================
    ``_local_``                Any loopback addresses on the system, for example
                               ``127.0.0.1``.
    ``_site_``                 Any site-local addresses on the system, for
                               example ``192.168.0.1``.
    ``_global_``               Any globally-scoped addresses on the system, for
                               example ``8.8.8.8``.
    ``_[INTERFACE]_``          Addresses of a network interface, for example
                               ``_en0_``.
    =========================  =================================================

.. _conf_ports:

Ports
-----

.. _http.port:

**http.port**
  | *Runtime:*   ``no``

  This defines the TCP port range to which the CrateDB HTTP service will be
  bound to. It defaults to ``4200-4300``. Always the first free port in this
  range is used. If this is set to an integer value it is considered as an
  explicit single port.

  The HTTP protocol is used for the REST endpoint which is used by all clients
  except the Java client.

.. _http.publish_port:

**http.publish_port**
  | *Runtime:*   ``no``

  The port HTTP clients should use to communicate with the node. It is
  necessary to define this setting if the bound HTTP port (``http.port``) of
  the node is not directly reachable from outside, e.g. running it behind a
  firewall or inside a Docker container.

.. _transport.tcp.port:

**transport.tcp.port**
  | *Runtime:*   ``no``

  This defines the TCP port range to which the CrateDB transport service will
  be bound to. It defaults to ``4300-4400``. Always the first free port in this
  range is used. If this is set to an integer value it is considered as an
  explicit single port.

  The transport protocol is used for internal node-to-node communication.

.. _transport.publish_port:

**transport.publish_port**
  | *Runtime:*   ``no``

  The port that the node publishes to the cluster for its own discovery. It is
  necessary to define this setting when the bound tranport port
  (``transport.tcp.port``) of the node is not directly reachable from outside,
  e.g. running it behind a firewall or inside a Docker container.

.. _psql.port:

**psql.port**
  | *Runtime:*   ``no``

  This defines the TCP port range to which the CrateDB Postgres service will be
  bound to. It defaults to ``5432-5532``. Always the first free port in this
  range is used. If this is set to an integer value it is considered as an
  explicit single port.

Advanced TCP settings
---------------------

Any interface that uses TCP (Postgres wire, HTTP & Transport protocols) shares
the following settings:

.. _network.tcp.no_delay:

**network.tcp.no_delay**
  | *Default:* ``true``
  | *Runtime:* ``no``

  Enable or disable the `Nagle's algorithm`_ for buffering TCP packets.
  Buffering is disabled by default.

.. _network.tcp.keep_alive:

**network.tcp.keep_alive**
  | *Default:* ``true``
  | *Runtime:* ``no``

  Configures the ``SO_KEEPALIVE`` option for sockets, which determines
  whether they send TCP keepalive probes.

.. _network.tcp.reuse_address:

**network.tcp.reuse_address**
  | *Default:* ``true`` on non-windows machines and ``false`` otherwise
  | *Runtime:* ``no``

   Configures the ``SO_REUSEADDRS`` option for sockets, which determines
   whether they should reuse the address.

.. _network.tcp.send_buffer_size:

**network.tcp.send_buffer_size**
  | *Default:* ``-1``
  | *Runtime:* ``no``

  The size of the TCP send buffer (`SO_SNDBUF`_ socket option).
  By default not explicitly set.

.. _network.tcp.receive_buffer_size:

**network.tcp.receive_buffer_size**
  | *Default:* ``-1``
  | *Runtime:* ``no``

  The size of the TCP receive buffer  (`SO_RCVBUF`_ socket option).
  By default not explicitly set.

.. NOTE::

    Each setting in this section has its counterpart for HTTP and transport.
    To provide a protocol specific setting, remove ``network`` prefix and use
    either ``http`` or ``transport`` instead. For example, no_delay can be
    configured as ``http.tcp.no_delay`` and ``transport.tcp.no_delay``. Please
    note, that PG interface takes its settings from transport.

Transport settings
------------------

.. _transport.connect_timeout:

**transport.connect_timeout**
  | *Default:* ``30s``
  | *Runtime:* ``no``

  The connect timeout for initiating a new connection.

.. _transport.compress:

**transport.compress**
  | *Default:* ``false``
  | *Runtime:* ``no``

  Set to `true` to enable compression (DEFLATE) between all nodes.

.. _transport.ping_schedule:

**transport.ping_schedule**
  | *Default:* ``-1``
  | *Runtime:* ``no``

  Schedule a regular application-level ping message to ensure that transport
  connections between nodes are kept alive. Defaults to `-1` (disabled). It is
  preferable to correctly configure TCP keep-alives instead of using this
  feature, because TCP keep-alives apply to all kinds of long-lived connections
  and not just to transport connections.

Paths
=====

.. NOTE::

    Relative paths are relative to :ref:`CRATE_HOME <conf-env-crate-home>`.
    Absolute paths override this behavior.

.. _path.conf:

**path.conf**
  | *Default:* ``config``
  | *Runtime:* ``no``

  Filesystem path to the directory containing the configuration files
  ``crate.yml`` and ``log4j2.properties``.

.. _path.data:

**path.data**
  | *Default:* ``data``
  | *Runtime:* ``no``

  Filesystem path to the directory where this CrateDB node stores its data
  (table data and cluster metadata).

  Multiple paths can be set by using a comma separated list and each of these
  paths will hold full shards (instead of striping data across them). For
  example:

  .. code-block:: yaml

      path.data: /path/to/data1,/path/to/data2

  When CrateDB finds striped shards at the provided locations (from CrateDB
  <0.55.0), these shards will be migrated automatically on startup.

.. _path.logs:

**path.logs**
  | *Default:* ``logs``
  | *Runtime:* ``no``

  Filesystem path to a directory where log files should be stored.

  Can be used as a variable inside ``log4j2.properties``.

  For example:

  .. code-block::
     yaml

     appender:
       file:
         file: ${path.logs}/${cluster.name}.log

.. _path.repo:

**path.repo**
  | *Runtime:* ``no``

  A list of filesystem or UNC paths where repositories of type
  :ref:`sql-create-repo-fs` may be stored.

  Without this setting a CrateDB user could write snapshot files to any
  directory that is writable by the CrateDB process. To safeguard against this
  security issue, the possible paths have to be whitelisted here.

  See also :ref:`location <sql-create-repo-fs-location>` setting of repository
  type ``fs``.

.. SEEALSO::

    :ref:`blobs.path <blobs.path>`

Plug-ins
========

.. _plugin.mandatory:

**plugin.mandatory**
  | *Runtime:* ``no``

  A list of plug-ins that are required for a node to startup.

  If any plug-in listed here is missing, the CrateDB node will fail to start.

CPU
===

.. _processors:

**processors**
  | *Runtime:* ``no``

  The number of processors is used to set the size of the thread pools CrateDB
  is using appropriately. If not set explicitly, CrateDB will infer the number
  from the available processors on the system.

  In environments where the CPU amount can be restricted (like Docker) or when
  multiple CrateDB instances are running on the same hardware, the inferred
  number might be too high. In such a case, it is recommended to set the value
  explicitly.

Memory
======

.. _bootstrap.memory_lock:

**bootstrap.memory_lock**
  | *Default:* ``false``
  | *Runtime:* ``no``

  CrateDB performs poorly when the JVM starts swapping: you should ensure that
  it *never* swaps. If set to ``true``, CrateDB will use the ``mlockall``
  system call on startup to ensure that the memory pages of the CrateDB process
  are locked into RAM.

Garbage collection
==================

CrateDB logs if JVM garbage collection on different memory pools takes too
long. The following settings can be used to adjust these timeouts:

.. _monitor.jvm.gc.collector.young.warn:

**monitor.jvm.gc.collector.young.warn**
  | *Default:* ``1000ms``
  | *Runtime:* ``no``

  CrateDB will log a warning message if it takes more than the configured
  timespan to collect the *Eden Space* (heap).

.. _monitor.jvm.gc.collector.young.info:

**monitor.jvm.gc.collector.young.info**
  | *Default:* ``700ms``
  | *Runtime:* ``no``

  CrateDB will log an info message if it takes more than the configured
  timespan to collect the *Eden Space* (heap).

.. _monitor.jvm.gc.collector.young.debug:

**monitor.jvm.gc.collector.young.debug**
  | *Default:* ``400ms``
  | *Runtime:* ``no``

  CrateDB will log a debug message if it takes more than the configured
  timespan to collect the *Eden Space* (heap).

.. _monitor.jvm.gc.collector.old.warn:

**monitor.jvm.gc.collector.old.warn**
  | *Default:* ``10000ms``
  | *Runtime:* ``no``

  CrateDB will log a warning message if it takes more than the configured
  timespan to collect the *Old Gen* / *Tenured Gen* (heap).

.. _monitor.jvm.gc.collector.old.info:

**monitor.jvm.gc.collector.old.info**
  | *Default:* ``5000ms``
  | *Runtime:* ``no``

  CrateDB will log an info message if it takes more than the configured
  timespan to collect the *Old Gen* / *Tenured Gen* (heap).

.. _monitor.jvm.gc.collector.old.debug:

**monitor.jvm.gc.collector.old.debug**
  | *Default:* ``2000ms``
  | *Runtime:* ``no``

  CrateDB will log a debug message if it takes more than the configured
  timespan to collect the *Old Gen* / *Tenured Gen* (heap).

Authentication
==============


.. _host_based_auth:

Trust authentication
--------------------

.. _auth.trust.http_default_user:

**auth.trust.http_default_user**
  | *Default:* ``crate``
  | *Runtime:* ``no``

  The default user that should be used for authentication when clients connect
  to CrateDB via HTTP protocol and they do not specify a user via the
  ``Authorization`` request header.

.. _auth.trust.http_support_x_real_ip:

**auth.trust.http_support_x_real_ip**
  | *Default:* ``false``
  | *Runtime:* ``no``

  If enabled, the HTTP transport will trust the ``X-Real-IP`` header sent by
  the client to determine the client's IP address. This is useful when CrateDB
  is running behind a reverse proxy or load-balancer. For improved security,
  any ``_local_`` IP address (``127.0.0.1`` and ``::1``) defined in this header
  will be ignored.

.. warning::

    Enabling this setting can be a security risk, as it allows clients to
    impersonate other clients by sending a fake ``X-Real-IP`` header.


Host-based authentication
-------------------------

Authentication settings (``auth.host_based.*``) are node settings, which means
that their values apply only to the node where they are applied and different
nodes may have different authentication settings.

.. _auth.host_based.enabled:

**auth.host_based.enabled**
  | *Default:* ``false``
  | *Runtime:* ``no``

  Setting to enable or disable Host Based Authentication (HBA). It is disabled
  by default.

HBA entries
...........

The ``auth.host_based.config.`` setting is a group setting that can have zero,
one or multiple groups that are defined by their group key (``${order}``) and
their fields (``user``, ``address``, ``method``, ``protocol``, ``ssl``).

.. _$(order):

**${order}:**
  | An identifier that is used as a natural order key when looking up the host
  | based configuration entries. For example, an order key of ``a`` will be
  | looked up before an order key of ``b``. This key guarantees that the entry
  | lookup order will remain independent from the insertion order of the
  | entries.

The :ref:`admin_hba` setting is a list of predicates that users can specify to
restrict or allow access to CrateDB.

The meaning of the fields of the are as follows:

.. _auth.host_based.config.${order}.user:

**auth.host_based.config.${order}.user**
  | *Runtime:*  ``no``

  | Specifies an existing CrateDB username, only ``crate`` user (superuser) is
  | available. If no user is specified in the entry, then all existing users
  | can have access.

.. _auth.host_based.config.${order}.address:

**auth.host_based.config.${order}.address**
  | *Runtime:* ``no``

  | The client machine addresses that the client matches, and which are allowed
  | to authenticate. This field may contain an IPv4 address, an IPv6 address or
  | an IPv4 CIDR mask. For example: ``127.0.0.1`` or ``127.0.0.1/32``. It also
  | may contain a hostname or the special ``_local_`` notation which will match
  | both IPv4 and IPv6 connections from localhost. A hostname specification
  | that starts with a dot (.) matches a suffix of the actual hostname.
  | So .crate.io would match foo.crate.io but not just crate.io. If no address
  | is specified in the entry, then access to CrateDB is open for all hosts.

.. _auth.host_based.config.${order}.method:

**auth.host_based.config.${order}.method**
  | *Runtime:* ``no``

  | The authentication method to use when a connection matches this entry.
  | Valid values are ``trust``, ``cert``, and ``password``. If no method is
  | specified, the ``trust`` method is used by default.
  | See :ref:`auth_trust`, :ref:`auth_cert` and :ref:`auth_password` for more
  | information about these methods.

.. _auth.host_based.config.${order}.protocol:

**auth.host_based.config.${order}.protocol**
  | *Runtime:* ``no``

  | Specifies the protocol for which the authentication entry should be used.
  | If no protocol is specified, then this entry will be valid for all
  | protocols that rely on host based authentication see :ref:`auth_trust`).

.. _auth.host_based.config.${order}.ssl:

**auth.host_based.config.${order}.ssl**
  | *Default:* ``optional``
  | *Runtime:* ``no``

  | Specifies whether the client must use SSL/TLS to connect to the cluster.
  | If set to ``on`` then the client must be connected through SSL/TLS
  | otherwise is not authenticated. If set to ``off`` then the client must
  | *not* be connected via SSL/TLS otherwise is not authenticated. Finally
  | ``optional``, which is the value when the option is completely skipped,
  | means that the client can be authenticated regardless of SSL/TLS is used
  | or not.

Example of config groups:

.. code-block:: yaml

    auth.host_based.config:
      entry_a:
        user: crate
        address: 127.16.0.0/16
      entry_b:
        method: trust
      entry_3:
        user: crate
        address: 172.16.0.0/16
        method: trust
        protocol: pg
        ssl: on


.. _ssl_config:

Secured communications (SSL/TLS)
================================

Secured communications via SSL allows you to encrypt traffic between CrateDB
nodes and clients connecting to them. Connections are secured using Transport
Layer Security (TLS).

.. _ssl.http.enabled:

**ssl.http.enabled**
  | *Default:* ``false``
  | *Runtime:*  ``no``

  Set this to true to enable secure communication between the CrateDB node
  and the client through SSL via the HTTPS protocol.

.. _ssl.psql.enabled:

**ssl.psql.enabled**
  | *Default:* ``false``
  | *Runtime:*  ``no``

  Set this to true to enable secure communication between the CrateDB node
  and the client through SSL via the PostgreSQL wire protocol.

.. _ssl.transport.mode:

**ssl.transport.mode**
  | *Default:* ``legacy``
  | *Runtime:* ``no``

  For communication between nodes, choose:

  ``off``
    SSL cannot be used
  ``legacy``
    SSL is not used. If HBA is enabled, transport connections won't be verified
    Any reachable host can establish a connection.
  ``on``
    SSL must be used

.. _ssl.keystore_filepath:

**ssl.keystore_filepath**
  | *Runtime:* ``no``

  The full path to the node keystore file.

.. _ssl.keystore_password:

**ssl.keystore_password**
  | *Runtime:* ``no``

  The password used to decrypt the keystore file defined with
  ``ssl.keystore_filepath``.

.. _ssl.keystore_key_password:

**ssl.keystore_key_password**
  | *Runtime:* ``no``

  The password entered at the end of the ``keytool -genkey command``.

.. NOTE::

    Optionally trusted CA certificates can be stored separately from the
    node's keystore into a truststore for CA certificates.

.. _ssl.truststore_filepath:

**ssl.truststore_filepath**
  | *Runtime:* ``no``

  The full path to the node truststore file. If not defined, then only a
  keystore will be used.

.. _ssl.truststore_password:

**ssl.truststore_password**
  | *Runtime:* ``no``

  The password used to decrypt the truststore file defined with
  ``ssl.truststore_filepath``.

.. _ssl.resource_poll_interval:

**ssl.resource_poll_interval**
  | *Default:* ``5m``
  | *Runtime:* ``no``

  The frequency at which SSL files such as keystore and truststore are polled
  for changes.

Cross-origin resource sharing (CORS)
====================================

Many browsers support the `same-origin policy`_ which requires web applications
to explicitly allow requests across origins. The `cross-origin resource
sharing`_ settings in CrateDB allow for configuring these.

.. _http.cors.enabled:

**http.cors.enabled**
  | *Default:* ``false``
  | *Runtime:* ``no``

  Enable or disable `cross-origin resource sharing`_.

.. _http.cors.allow-origin:

**http.cors.allow-origin**
  | *Default:* ``<empty>``
  | *Runtime:* ``no``

  Define allowed origins of a request. ``*`` allows *any* origin (which can be
  a substantial security risk) and by prepending a ``/`` the string will be
  treated as a :ref:`regular expression <gloss-regular-expression>`. For
  example ``/https?:\/\/crate.io/`` will allow requests from
  ``https://crate.io`` and ``https://crate.io``. This setting disallows any
  origin by default.

.. _http.cors.max-age:

**http.cors.max-age**
  | *Default:* ``1728000`` (20 days)
  | *Runtime:* ``no``

  Max cache age of a preflight request in seconds.

.. _http.cors.allow-methods:

**http.cors.allow-methods**
  | *Default:* ``OPTIONS, HEAD, GET, POST, PUT, DELETE``
  | *Runtime:* ``no``

  Allowed HTTP methods.

.. _http.cors.allow-headers:

**http.cors.allow-headers**
  | *Default:* ``X-Requested-With, Content-Type, Content-Length``
  | *Runtime:* ``no``

  Allowed HTTP headers.

.. _http.cors.allow-credentials:

**http.cors.allow-credentials**
  | *Default:* ``false``
  | *Runtime:* ``no``

  Add the ``Access-Control-Allow-Credentials`` header to responses.

.. _`same-origin policy`: https://developer.mozilla.org/en-US/docs/Web/Security/Same-origin_policy
.. _`cross-origin resource sharing`: https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS

Blobs
=====

.. _blobs.path:

**blobs.path**
  | *Runtime:* ``no``

  Path to a filesystem directory where to store blob data allocated for this
  node.

  By default blobs will be stored under the same path as normal data. A
  relative path value is interpreted as relative to ``CRATE_HOME``.

.. _ref-configuration-repositories:

Repositories
============

Repositories are used to :ref:`backup <snapshot-restore>` a CrateDB cluster.

.. _repositories.url.allowed_urls:

**repositories.url.allowed_urls**
  | *Runtime:* ``no``

  This setting only applies to repositories of type :ref:`sql-create-repo-url`.

  With this setting a list of urls can be specified which are allowed to be
  used if a repository of type ``url`` is created.

  Wildcards are supported in the host, path, query and fragment parts.

  This setting is a security measure to prevent access to arbitrary resources.

  In addition, the supported protocols can be restricted using the
  :ref:`repositories.url.supported_protocols
  <repositories.url.supported_protocols>` setting.

.. _repositories.url.supported_protocols:

**repositories.url.supported_protocols**
  | *Default:* ``http``, ``https``, ``ftp``, ``file`` and ``jar``
  | *Runtime:* ``no``

  A list of protocols that are supported by repositories of type
  :ref:`sql-create-repo-url`.

  The ``jar`` protocol is used to access the contents of jar files. For more
  info, see the java `JarURLConnection documentation`_.

See also the :ref:`path.repo <path.repo>` Setting.

.. _`JarURLConnection documentation`: https://docs.oracle.com/javase/8/docs/api/java/net/JarURLConnection.html

Queries
=======

.. _indices.query.bool.max_clause_count:

**indices.query.bool.max_clause_count**
  | *Default:* ``8192``
  | *Runtime:* ``no``

  This setting limits the number of boolean clauses that can be generated by
  ``!= ANY()``, ``LIKE ANY()``, ``ILIKE ANY()``, ``NOT LIKE ANY()`` and
  ``NOT ILIKE ANY()`` :ref:`operators <gloss-operator>` on arrays in order to
  prevent users from executing queries that may result in heavy memory
  consumption causing nodes to crash with ``OutOfMemory`` exceptions. Throws
  ``TooManyClauses`` errors when the limit is exceeded.

  .. NOTE::

    You can avoid ``TooManyClauses`` errors by increasing this setting. The
    number of boolean clauses used can be larger than the elements of the array
    .

Legacy
=======

.. _legacy.table_function_column_naming:

**legacy.table_function_column_naming**
  | *Default:* ``false``
  | *Runtime:* ``no``

  Since CrateDB 5.0.0, if the table function is not aliased and is returning a
  single base data typed column, the table function name is used as the column
  name. This setting can be set in order to use the naming convention prior to
  5.0.0.

  The following table functions are affected by this setting:

  - :ref:`unnest <unnest>`
  - :ref:`regexp_matches <table-functions-regexp-matches>`
  - :ref:`generate_series <table-functions-generate-series>`

  When the setting is set and a single column is expected to be returned,
  the returned column will be named ``col1``, ``groups``, or ``col1``
  respectively.

  .. NOTE::

    Beware that if not all nodes in the cluster are consistently set or unset,
    the behaviour will depend on the node handling the query.

.. _conf-node-lang-js:

JavaScript language
===================

.. _lang.js.enabled:

**lang.js.enabled**
  | *Default:*  ``true``
  | *Runtime:*  ``no``

  Setting to enable or disable :ref:`JavaScript UDF <udf-js>` support.


.. _conf-node-attributes:

Custom attributes
=================

The ``node.attr`` namespace is a bag of custom attributes. Custom attributes
can be :ref:`used to control shard allocation
<conf-routing-allocation-awareness>`.

You can create any attribute you want under this namespace, like
``node.attr.key: value``. These attributes use the ``node.attr`` namespace to
distinguish them from core node attribute like ``node.name``.

Custom attributes are not validated by CrateDB, unlike core node attributes.

.. vale on


.. _plugins: https://github.com/crate/crate/blob/master/devs/docs/plugins.rst
.. _Nagle's algorithm: https://en.wikipedia.org/wiki/Nagle%27s_algorithm
.. _SO_RCVBUF: https://docs.oracle.com/javase/7/docs/api/java/net/StandardSocketOptions.html#SO_RCVBUF
.. _SO_SNDBUF: https://docs.oracle.com/javase/7/docs/api/java/net/StandardSocketOptions.html#SO_SNDBUF
