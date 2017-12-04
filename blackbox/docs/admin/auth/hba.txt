.. _admin_hba:

===============================
Host Based Authentication (HBA)
===============================

This section explains how to configure CrateDB client connection and
authentication.

By default, the boolean setting ``auth.host_based.enabled`` is ``false`` and
therefore host based authentication is disabled. In this instance, the CrateDB
cluster allows any unauthenticated connections.

To allow authenticated access to CrateDB from specific hosts, you need to set
the ``auth.host_based.enabled`` setting in the ``crate.yml`` to ``true`` and
specify HBA entries in the ``auth.host_based.config`` group setting.

See: :ref:`applying-cluster-settings`.

.. NOTE::

   Host Based Authentication is an
   :ref:`enterprise feature <enterprise_features>`.

.. rubric:: Table of Contents

.. contents::
   :local:

Authentication Against CrateDB
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

For HTTP connections the ``X-REAL-IP`` request header has priority over the
actual client IP address in order to allow proxied clients to authenticate.

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
the user ``mike`` can authenticate to CrateDB over the Postgres Wire Protocol
from any IP address ranging from ``32.0.0.0`` to ``32.255.255.255``, using the
``trust`` authentication method.

``{user: crate, address: 32.0.0.0/8, method: trust}`` means that the superuser
``crate`` can authenticate to CrateDB over the protocols for which
authentication is supported from any IP address in the range of ``32.0.0.0`` to
``32.255.255.255``,  using the ``trust`` authentication method.

``{user: barb, address: 172.16.0.0, protocol: pg, ssl: on}`` means that the
user ``barb`` can authenticate to CrateDB over the Postgres Wire Protocol from
the ``172.16.0.0`` IP Address only if the connection is done over SSL/TLS.
Since no authentication method is specified, the ``trust`` method will be used
by default.

The entry: ``{user: eleven, protocol: pg}`` means that the user ``eleven`` can
authenticate to CrateDB over the Postgres Wire Protocol from any IP address,
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

   For general help managing users, see :ref:`administration_user_management`.

Authenticating as a Superuser
=============================

When CrateDB is started, the cluster contains one predefined superuser. This
user is called ``crate``.

To enable trust authentication for the superuser, ``crate`` must be specified in
the the ``auth.host_based`` setting, like this:

.. code-block:: yaml

    auth:
      host_based:
        enabled: true
        config:
          0:
            user: crate

Authenticating to Admin UI
==========================

.. hide:

    cr> CREATE USER admin;
    CREATE OK, 1 row affected (... sec)

When trying to access the CrateDB admin UI, authentication with the user
defined with the :ref:`auth.trust.http_default_user
<auth_trust_http_default_user>` setting (defaults to ``crate``) will be
attempted initially. If this authentication attempt fails, the browser will
open the standard popup window where the user is asked to fill in credentials.
Depending on the HBA configuration, it may be necessary to a username and
password, or, alternatively, a username only.

Users that log in to the admin UI must be granted `DQL`` privileges at the
``CLUSTER`` level in order to be able to access the various monitoring
sections. For example::

    cr> GRANT DQL TO admin;
    GRANT OK, 1 row affected (... sec)

For more information, consult the :ref:`privileges section
<administration-privileges>`.

.. hide:

    cr> DROP USER admin;
    DROP OK, 1 row affected (... sec)
