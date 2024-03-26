.. _auth_methods:

======================
Authentication Methods
======================

There are multiple ways to authenticate against CrateDB.

.. rubric:: Table of contents

.. contents::
   :local:

.. _auth_trust:

Trust method
============

When the ``trust`` authentication method is used, the server just takes the
username provided by the client as is without further validation. This is
useful for any setup where access is controlled by other means, like network
restrictions as implemented by :ref:`admin_hba`.

Trust authentication over PostgreSQL protocol
---------------------------------------------

The PostgreSQL Protocol requires a user for every connection which is sent by
all client implementations.

Trust authentication over HTTP
------------------------------

The HTTP implementation extracts the username from the
`HTTP Basic Authentication`_ request header.

Since a user is always required for trust authentication, it is possible to
specify a default user in case that the ``Authorization`` header is not set.
This is useful to allow clients which do not provide the possibility to set any
headers, for example a web browser connecting to the Admin UI.

.. _auth_trust_http_default_user:

The default user can be specified via the ``auth.trust.http_default_user``
setting like this:

.. code-block:: yaml

    auth:
      trust:
        http_default_user: dustin



.. NOTE::

   When user management is enabled, the user of the Admin UI needs to be
   granted the following privileges: ``DQL`` on ``sys.shards``, ``sys.nodes``,
   ``sys.node_checks``, ``sys.checks``, ``sys.cluster``, and ``sys.jobs_log``
   tables. As well as ``DQL`` on the ``doc`` schema.

   These ``DQL`` privileges are required by the Admin UI to display the
   cluster health, monitoring,  and checks, to list the available nodes
   in the cluster and to list the tables.

.. _auth_password:

Password authentication method
==============================

When the ``password`` authentication method is used, the client has to provide
a password additionally to the username.

For HTTP, the password must be encoded together with the username with
``BASE64_`` and sent together prefixed with ``Basic`` as string value for the
``Authorization`` HTTP header. See also: `HTTP Basic Authentication`_.

The password is sent from the client to the server in **clear text**, which
means that unless SSL is enabled, the password could potentially be read by
anyone sniffing the network.

**CrateDB does not store user passwords as clear text!**

CrateDB stores user passwords salted with a per-user salt and hashed using the
PBKDF2_ key derivation function and the `SHA-512 hash algorithm`_.

.. NOTE::

   CrateDB will never leak information about user existence in the case of
   failed authentication. If you're receiving an error trying to authenticate,
   first make sure that the user exists.

.. _auth_cert:

Client certificate authentication method
========================================

When the ``cert`` authentication method is used, the client has to connect to
CrateDB using SSL with a valid client certificate.

If connecting via HTTP where the username is optional, the ``common name`` will
be used as username. In case a username is already provided, it has to match
the ``common name`` of the certificate. Otherwise the authentication will fail.
See :ref:`auth_trust` on how to provide a username via HTTP.

The rule that the ``common name`` must match the provided username always
applies to the PostgreSQL wire protocol, as there the username isn't optional.

Please consult the relevant client documentations for instructions on how to
connect using SSL with client certificate.

.. _auth_jwt:

JWT authentication method
=========================

JWT authentication allows to delegate part of the authentication process to an
external service.

The external service is responsible for issuing a `JWT`_ access token for the
user. The user then provides this token, prefixed with ``Bearer`` in the
``Authorization`` HTTP header to CrateDB.
CrateDB will validate the token and match it to a user created with ``CREATE
USER`` with ``JWT`` properties that match those of the provided ``JWT`` token.

Token must contain the following claims:

``kid`` - `Key ID`_.

``iss`` - URL of the `JWK endpoint`_.

``aud`` - `aud`_.

``username`` - user name in a third party app.

``iss``, ``username`` and ``aud`` values must match the values created by
``CREATE USER`` statement. If ``aud`` has not been defined on the
``CREATE USER`` statement, the cluster id is used and must match the token's
``aud`` value. See :ref:`create-user-jwt` for details.

It's recommended to have ``exp`` (`expiration date`_ as epoch seconds) in the
header. If it's provided, the token's expiration date will be checked against
the local system's time in UTC.

Supported signing algorithms are RSA-256, RSA-384 and RSA-512.
The algorithm to verify the signature is decided on the JWK endpoint's ``alg``
value. If the ``alg`` value is not provided, RSA-256 is used (default).

It's recommended to have the ``alg`` (`Algorithm parameter`_)  in the header.
If it's provided both in the token and in the response from the JWK endpoint,
both values are compared and in case of a mismatch the token is rejected.

.. NOTE::

   JWT is supported only for the HTTP protocol. An :ref:`HBA <admin_hba>` entry
   for ``jwt`` MUST be combined with ``protocol: http``.

.. SEEALSO::

  :ref:`admin_hba`

  :ref:`admin_ssl`

.. _PBKDF2: https://en.wikipedia.org/wiki/PBKDF2
.. _SHA-512 hash algorithm: https://en.wikipedia.org/wiki/SHA-2
.. _HTTP Basic Authentication: https://en.wikipedia.org/wiki/Basic_access_authentication
.. _JWK endpoint: https://datatracker.ietf.org/doc/html/rfc7517
.. _BASE64: https://en.wikipedia.org/wiki/Base64
.. _JWT: https://datatracker.ietf.org/doc/html/rfc7519
.. _Key ID: https://datatracker.ietf.org/doc/html/rfc7517#section-4.5
.. _expiration date: https://www.rfc-editor.org/rfc/rfc7519#section-4.1.4
.. _Algorithm parameter: https://datatracker.ietf.org/doc/html/rfc7517#section-4.4
.. _aud: https://www.rfc-editor.org/rfc/rfc7519#section-4.1.3
