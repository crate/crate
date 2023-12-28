.. _admin_hba:

===============================
Host-Based Authentication (HBA)
===============================

This section explains how to configure CrateDB client connection and
authentication.

.. NOTE::

    The stock ``crate.yml`` shipped with CrateDB explicitly enables host based
    authentication and defines a set of basic authentication rules.

    If you do not want to use authentication, set ``auth.host_based.enabled``
    to ``false``. If authentication is disabled, :ref:`user management
    <administration_user_management>` remains active and you must specify the
    user ``crate`` (with an empty password) when connecting via the PostgreSQL
    protocol. HTTP clients do not have to specify a user because they use the
    :ref:`auth.trust.http_default_user <auth_trust_http_default_user>` if no
    user is provided.

    :ref:`Non-runtime cluster-wide settings <applying-cluster-settings>` must
    be configured the same on every node.

.. rubric:: Table of contents

.. contents::
   :local:


.. _admin_hba_cratedb:

Authentication against CrateDB
==============================

Client access and authentication is configured via the
``auth.host_based.config`` setting in the ``crate.yml`` file.

The general format of the ``auth.host_based.config`` setting is a map of remote
client access entries, where the key of the map defines the order in which the
entries are used, which permit authentication to CrateDB. Each entry may
contain no, one, or multiple fields. Allowed fields are ``user``, ``ip`` or
``cidr``, ``method``,  ``protocol`` and ``ssl``. The description of these
fields can be found in :ref:`host_based_auth`.

When a client sends an authentication request, CrateDB matches the provided
username, IP address,  protocol and connection scheme against these entries
to determine which authentication method is required. If no entry matches, the
client authentication request will be denied.

To support proxied clients to authenticate, the ``X-REAL-IP`` request header
can be used. For security reasons, this is disabled by default as it allows
clients to impersonate other clients. To enable this feature,
set :ref:`auth.trust.http_support_x_real_ip` to ``true``. If enabled, the
``X-REAL-IP`` request header has priority over the actual client IP address.

If ``auth.host_based`` is not set, the host based authentication is disabled.
In this case CrateDB **trusts all connections** and accepts the user provided by
the client given that this user exists.

If the setting ``auth.host_based`` is present and the configurations list does
not contain any entry, then no client can authenticate.

For example, a host based configuration can look like this:

.. code-block:: yaml

    auth:
      host_based:
        enabled: true
        config:
          0:
            user: mike
            address: 32.0.0.0/8
            method: trust
            protocol: pg
          a:
            user: barb
            address: 172.16.0.0
            protocol: pg
            ssl: on
          b:
            user: crate
            address: 32.0.0.0/8
            method: trust
          y:
            user: eleven
            protocol: pg
          e:
            user: dustin
            address: 172.16.0.0
            method: trust
            protocol: http
          f:
            user: trinity
            protocol: http
            address: 127.0.0.1
            ssl: off
          z:
            method: password

.. NOTE::

   In the ``auth.host_based.config`` setting, the order of the entries is
   defined by the natural order of the group keys of the setting. The
   authentication method of the first entry that matches the client user and
   address will be used. If the authentication attempt fails, subsequent
   entries will not be considered. The entry look-up order is determined by the
   ``order`` identifier of each entry.

In the example above:

``{user: mike, address: 32.0.0.0/8, method: trust, protocol: pg}`` means that
the user ``mike`` can authenticate to CrateDB over the PostgreSQL Wire Protocol
from any IP address ranging from ``32.0.0.0`` to ``32.255.255.255``, using the
``trust`` authentication method.

``{user: crate, address: 32.0.0.0/8, method: trust}`` means that the superuser
``crate`` can authenticate to CrateDB over the protocols for which
authentication is supported from any IP address in the range of ``32.0.0.0`` to
``32.255.255.255``,  using the ``trust`` authentication method.

``{user: barb, address: 172.16.0.0, protocol: pg, ssl: on}`` means that the
user ``barb`` can authenticate to CrateDB over the PostgreSQL Wire Protocol
from the ``172.16.0.0`` IP Address only if the connection is done over SSL/TLS.
Since no authentication method is specified, the ``trust`` method will be used
by default.

The entry: ``{user: eleven, protocol: pg}`` means that the user ``eleven`` can
authenticate to CrateDB over the PostgreSQL Wire Protocol from any IP address,
using the ``trust`` method.

``{user: dustin, address: 172.16.0.0, protocol: http, method: trust}`` means
that the user ``dustin`` can authenticate to CrateDB over HTTP protocol from
the ``172.16.0.0`` IP Address using the ``trust`` method.

``{user: trinity, address: 127.0.0.1, protocol: http, ssl: off}`` means that
the user ``trinity`` can authenticate to CrateDB over HTTP from the
``127.0.0.1`` IP Address only if no SSL/TLS connection is used. Since no
authentication method is specified, the ``trust`` method will be used by
default.

And finally the entry ``{method: password}`` means that any existing user (or
superuser) can authenticate to CrateDB from any IP address using the
``password`` method for both HTTP and PostgreSQL wire protocol.

.. NOTE::

   For general help managing users and roles, see :ref:`administration_user_management`.


.. _admin_hba_user:

Authenticating as a superuser
=============================

When CrateDB is started, the cluster contains one predefined superuser. This
user is called ``crate``.

To enable trust authentication for the superuser, ``crate`` must be specified in
the ``auth.host_based`` setting, like this:

.. code-block:: yaml

    auth:
      host_based:
        enabled: true
        config:
          0:
            user: crate


.. _admin_hba_admin_ui:

Authenticating to Admin UI
==========================

.. hide:

    cr> CREATE USER admin;
    CREATE OK, 1 row affected (... sec)

When trying to access the CrateDB Admin UI, authentication with the user
defined with the :ref:`auth.trust.http_default_user
<auth_trust_http_default_user>` setting (defaults to ``crate``) will be
attempted initially. If this authentication attempt fails, the browser will
open the standard popup window where the user is asked to fill in credentials.
Depending on the HBA configuration, it may be necessary to a username and
password, or, alternatively, a username only.

Users that log in to the Admin UI must be granted `DQL`` privileges at the
``CLUSTER`` level in order to be able to access the various monitoring
sections. For example::

    cr> GRANT DQL TO admin;
    GRANT OK, 1 row affected (... sec)

For more information, consult the :ref:`privileges section
<administration-privileges>`.

.. hide:

    cr> DROP USER admin;
    DROP OK, 1 row affected (... sec)


.. _admin_hba_node:

Node-to-node communication
==========================

You can use the :ref:`Host-Based Authentication <admin_hba>` mechanism for
node-to-node communication.

For example, if you wanted to configure a `multi-zone cluster`_, you should
enable certificate authentication like this:

.. code-block:: yaml

    auth:
      host_based:
        enabled: true
        config:
          0:
            protocol: transport
            ssl: on
            method: cert

.. NOTE::

    CrateDB only supports the :ref:`trust <auth_trust>` and :ref:`cert
    <auth_cert>` authentication methods for node-to-node communication.


.. _multi-zone cluster: https://crate.io/docs/crate/howtos/en/latest/clustering/multi-zone-setup.html
