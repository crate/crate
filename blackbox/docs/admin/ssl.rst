.. _admin_ssl:

================================
Secured Communications (SSL/TLS)
================================

Secured communication allows you to encrypt traffic between the CrateDB node
and a client. This applies to connections using HTTP (i.e. `Admin UI
<https://crate.io/docs/crate/guide/getting_started/connect/admin_ui.html>`_,
`Crash <https://crate.io/docs/crate/guide/getting_started/connect/crash.html>`_,
:ref:`sql_http_endpoint`), the :ref:`postgres_wire_protocol` (i.e. JDBC, psql),
and :ref:`ingest_mqtt`.

Connections are secured using Transport Layer Security (TLS).

Note that once SSL is enabled for HTTP connections, only connections using
HTTPS are allowed. Same applies to secure MQTT connections.
This is in contrast to the PostgreSQL Wire Protocol, which still allows
non-encrypted connections when SSL is enabled. If you want to enforce SSL
usage, please consult the :ref:`admin_hba`.

.. NOTE::

   Secured Communications is an Enterprise Edition feature.

.. rubric:: Table of Contents

.. contents::
   :local:

SSL/TLS Configuration
=====================

To enable SSL a ``keystore`` and a few configuration changes are necessary.
These changes need to be made in the ``crate.yml`` file on *each* node that
should have secure communications enabled.

Skip to :ref:`ssl_generate_keystore` for a step-by-step instruction on how to
create a ``keystore``.

Once the ``keystore`` (and optional ``truststore``) is created, continue with
the following steps:

 - Set ``ssl.psql.enabled``, ``ssl.http.enabled`` or ``ssl.ingestion.mqtt.enabled``
   to ``true``.
 - :ref:`ssl_configure_keystore`
 - (Optional) :ref:`ssl_configure_truststore`


.. _ssl_configure_keystore:

Configure the KeyStore
----------------------

SSL/TLS needs a keystore. The keystore holds the node certificate(s) which
should be signed by a certificate authority (CA). A third-party CA or your
organization's existing CA can be used.

When a client connects to a node using SSL/TLS, the client receives the
certificate provided by the node and will determine if the node's certificate
is valid, trusted, and matches the hostname or IP address it is trying to
connect to.

.. CAUTION::

    Technically, it's possible to disable CA checks for certificates on the
    client. It is strongly recommended however to use certificates signed by
    an official CA or by a private CA (company PKI) that is also known to the
    client. This will help to ensure that establishing trust is as painless
    as possible.

See :ref:`ssl_generate_keystore` for information about how to create a keystore.

Once the keystore is prepared, define the absolute file path to the keystore
``.jks`` file on the node using ``ssl.keystore_filepath`` setting.

.. NOTE::

    Make sure that the keystore file has the right permissions and is
    accessible by the system user ``crate``.

Also, define the password needed to decrypt the keystore by using the
``ssl.keystore_password`` setting.

Use ``ssl.keystore_key_password`` setting to define the key password used when
creating the keystore.

For a full list of the settings needed to configure SSL/TLS, refer to
:ref:`SSL configuration reference <ssl_config>`.


.. _ssl_configure_truststore:

Configure a Separate Truststore
-------------------------------

Trusted CA certificates can be stored in a node's keystore or a separate
truststore can be used to store them.

If you want to use a separate truststore, create a node truststore and import
the CA certificate(s) you want to trust. Once the truststore is prepared,
define the absolute file path of the truststore ``.jks`` file on the node
using the ``ssl.truststore_filepath`` setting.

.. NOTE::

    Make sure that the truststore file has the right permissions and is
    accessible by the system user ``crate``.

Also define the password needed to decrypt the keystore by using the
``ssl.truststore_password`` setting.

For a full list of the settings needed to configure SSL/TLS, refer to
:ref:`SSL configuration reference <ssl_config>`.

Connecting to a CrateDB Node Using HTTPS
----------------------------------------

Connect to a CrateDB Node Using the Admin UI
............................................

Crate's HTTP endpoint remains unchanged. When you have turned on secure
communication, it will use HTTPS instead of plain HTTP. Simply point your
browser to the same URL you used before but changing the protocol to https:

For example, ``http://localhost:4200`` becomes ``https://localhost:4200``.
If you have not configured the CrateDB node's Keystore with a signed
certificate from a Certificate Authority (CA), then you will get something
like the following: ``NET::ERR_CERT_AUTHORITY_INVALID``. You either need to
get your certificate signed from one of the CAs included in your browser or
import your owned certificates into the browser. A third option is storing
an exception for the CrateDB node certification in your browser after
verifying that this is indeed a certificate you trust.

Connect to a CrateDB Node Using Crash
.....................................

You can connect to a CrateDB node using a secure communication::

    crash --hosts https://localhost:4200

To validate the provided certificates, please see the options
``--verify-ssl`` and ``--key-file``.

Connect to a CrateDB Node Using REST
....................................

Issue your REST requests to the node using the ``https://`` protocol. You
may have to configure your client to validate the received certificate
accordingly.


Connecting to a CrateDB Node Using PostgreSQL Wire Protocol With SSL/TLS
------------------------------------------------------------------------

Connect to a CrateDB Node Using JDBC
....................................

JDBC needs to validate the CrateDB node's identity by checking that the node
certificate is signed by a trusted authority. If the certificate is signed by
a certificate authority (CA) that is known to the Java runtime, there is
nothing further to do (as Java comes with copies of the most common CA's
certificates).

If you have a certificate that is signed by a CA not known to the Java
runtime, you need to configure a truststore which contains the node's
certificate and provide the path to the truststore file along with the
password when starting your Java application::

    java -Djavax.net.ssl.trustStore=mystore -Djavax.net.ssl.trustStorePassword=mypassword com.mycompany.MyApp

In case you face any issues extra debugging information is available by adding
``-Djavax.net.debug=ssl`` to your command line.

Last but not least, the connection parameter ``ssl=true`` must be added to the
connection URL so that the JDBC driver will try and establish an SSL
connection.

For further information, visit `jdbc ssl documentation`_.

Connect to a CrateDB Node Using ``psql``
........................................

By default, ``psql`` attempts to use ssl if available on the node. For further
information including the different SSL modes please visit the
`psql documentation`_.

.. _jdbc ssl documentation: https://jdbc.postgresql.org/documentation/head/ssl-client.html
.. _psql documentation: https://www.postgresql.org/docs/current/static/app-psql.html


Connecting to a CrateDB MQTT Endpoint With SSL/TLS
--------------------------------------------------

A CrateDB node configured to run as an :ref:`MQTT endpoint <ingest_mqtt>` can
only accept encrypted connections if SSL ingestion is enabled, namely if the
:ref:`ssl.ingestion.mqtt.enabled <ssl_ingestion_mqtt_enabled>` setting is
configured to ``true`` and the :ref:`ingestion.mqtt.port <ingestion_mqtt_port>`
setting is ``8883``.

Setting up a Keystore/Truststore With a Certificate Chain
=========================================================

In case you need to setup a Keystore or a Trustore, here are the commands
to get you started. All the commands use a validity of 36500 days
(about 100 years). You might want to use less.


.. _ssl_generate_keystore:

Generate Keystore With a Private Key
------------------------------------

The first step is to create a keystore with a private key using the RSA
algorithm. The "first and last name" is the common name (CN) which should
overlap with the URL the service it is used with.

Command::

    keytool -keystore keystore -genkey -keyalg RSA -alias server -validity 36500

Output::

    Enter keystore password:
    Re-enter new password:
    What is your first and last name?
      [Unknown]:  ssl.crate.io
    What is the name of your organizational unit?
      [Unknown]:  Cryptography Department
    What is the name of your organization?
      [Unknown]:  Crate.io GmbH
    What is the name of your City or Locality?
      [Unknown]:  Berlin
    What is the name of your State or Province?
      [Unknown]:  Berlin
    What is the two-letter country code for this unit?
      [Unknown]:  DE
    Is CN=ssl.crate.io, OU=Cryptography Department, O=Crate.io GmbH, L=Berlin, ST=Berlin, C=DE correct?
      [no]:  yes

    Enter key password for <server>
        (RETURN if same as keystore password):
    Re-enter new password:


Generate a Certificate Signing Request
--------------------------------------

To establish trust for this key, we need to sign it. This is done by generating
a certificate signing request.

If you have access to a certificate authority (CA), you can skip the next
steps and get the signed certificate from the CA using the signing request which
we will generate with the command below. If you don't have access to a CA, then
follow the optional steps after this step to establish your own CA.

Command::

    keytool -keystore keystore -certreq -alias server -keyalg RSA -file server.csr


Output::

    Enter keystore password:
    Enter key password for <server>


Optional: Use a Self-Signed Certificate to Act as a Certificate Authority (CA)
------------------------------------------------------------------------------

.. NOTE::

   Only follow these optional steps if you want to create your own
   Certificate Authority (CA). Otherwise, please request a signed
   certificate from one of the CAs bundled with Java.


Generate a Self-Signed Certificate
..................................

If you don't get your certificate signed from one of the official CAs,
you might want to create your own CA with a self-signed certificate.
The common name (CN) should overlap with the CN of the server key
generated in the first step. For example, ``ssl.crate.io`` overlaps
with ``*.crate.io``.

.. NOTE::

    In this step by step guide it is shown how to create a server certificate.
    If you want to create a client certificate the steps are almost the same
    with the exception of providing a common name that is equivalent to the
    crate username as described in :ref:`client certificate authentication
    method <auth_cert>`.

Command::

    openssl req -x509 -sha256 -nodes -days 36500 -newkey rsa:2048 \
        -keyout rootCA.key -out rootCA.crt


Output::

    Generating a 2048 bit RSA private key
    .......................................................................+++
    .............................................................+++
    writing new private key to 'rootCA.key'
    -----
    You are about to be asked to enter information that will be incorporated
    into your certificate request.
    What you are about to enter is what is called a Distinguished Name or a DN.
    There are quite a few fields but you can leave some blank
    For some fields there will be a default value,
    If you enter '.', the field will be left blank.
    -----
    Country Name (2 letter code) [AU]:AT
    State or Province Name (full name) [Some-State]:Vorarlberg
    Locality Name (eg, city) []:Dornbirn
    Organization Name (eg, company) [Internet Widgits Pty Ltd]:Crate.io
    Organizational Unit Name (eg, section) []:Cryptography Department
    Common Name (e.g. server FQDN or YOUR name) []:*.crate.io
    Email Address []:info@crate.io


Generate a Signed Cert
......................

In order that the server can prove itself to have a valid and trusted domain it
is required that the server certificate contains `subjectAltName`_.

Create a file called ``ssl.ext`` with the following content. In section
``[alt_names]`` list valid domain names of the server::

    authorityKeyIdentifier=keyid,issuer
    basicConstraints=CA:FALSE
    keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
    subjectAltName = @alt_names

    [alt_names]
    DNS.1 = www.example.com

Now you can generate a signed cert from our certificate signing request.

Command::

    openssl x509 -req -in server.csr -CA rootCA.crt -CAkey rootCA.key \
        -CAcreateserial -out server.crt -sha256 -days 36500

Output::

    Signature ok
    subject=/C=DE/ST=Berlin/L=Berlin/O=Crate.io GmbH/OU=Cryptography Department/CN=ssl.crate.io
    Getting CA Private Key

.. _subjectAltName: http://wiki.cacert.org/FAQ/subjectAltName

Import the CA Certificate Into the Keystore
...........................................

The CA needs to be imported to the Keystore for the certificate chain to be
available when we import our signed certificate.

Command::

    keytool -import -keystore keystore -file rootCA.crt -alias theCARoot

Output::

    Enter keystore password:
    Owner: EMAILADDRESS=info@crate.io, CN=*.crate.io, OU=Cryptography Department, O=Crate.io, L=Dornbirn, ST=Vorarlberg, C=AT
    Issuer: EMAILADDRESS=info@crate.io, CN=*.crate.io, OU=Cryptography Department, O=Crate.io, L=Dornbirn, ST=Vorarlberg, C=AT
    Serial number: f13562ec6184401e
    Valid from: Mon Jun 12 13:09:17 CEST 2017 until: Wed May 19 13:09:17 CEST 2117
    Certificate fingerprints:
         MD5:  BB:A1:79:53:FE:71:EC:61:2A:19:81:E8:0E:E8:C9:81
         SHA1: 96:66:C1:01:49:17:D1:19:FB:DB:83:86:50:3D:3D:AD:DA:F7:C6:A9
         SHA256: 69:82:C5:24:9A:A1:AE:DF:80:29:7A:26:92:C1:A5:9F:AF:7D:03:56:CC:C3:E9:73:3B:FD:85:66:35:D6:8A:9B
         Signature algorithm name: SHA256withRSA
         Version: 3

    Extensions:

    #1: ObjectId: 2.5.29.35 Criticality=false
    AuthorityKeyIdentifier [
    KeyIdentifier [
    0000: CD 29 4E 07 3D C3 7C D0   16 45 FB 0A CE 8D B4 98  .)N.=....E......
    0010: B7 A8 4C 79                                        ..Ly
    ]
    [EMAILADDRESS=info@crate.io, CN=*.crate.io, OU=Cryptography Department, O=Crate.io, L=Dornbirn, ST=Vorarlberg, C=AT]
    SerialNumber: [    f13562ec 6184401e]
    ]

    #2: ObjectId: 2.5.29.19 Criticality=false
    BasicConstraints:[
      CA:true
      PathLen:2147483647
    ]

    #3: ObjectId: 2.5.29.14 Criticality=false
    SubjectKeyIdentifier [
    KeyIdentifier [
    0000: CD 29 4E 07 3D C3 7C D0   16 45 FB 0A CE 8D B4 98  .)N.=....E......
    0010: B7 A8 4C 79                                        ..Ly
    ]
    ]

    Trust this certificate? [no]:  yes
    Certificate was added to keystore


Import CA Into Truststore
.........................

If we are using our own CA, we should also import the certificate to the
Truststore, such that it is available for clients which want to verify
signatures.

Command::

    keytool -import -keystore truststore -file rootCA.crt -alias theCARoot

Output::

    Enter keystore password:
    Re-enter new password:
    Owner: EMAILADDRESS=info@crate.io, CN=*.crate.io, OU=Cryptography Department, O=Crate.io, L=Dornbirn, ST=Vorarlberg, C=AT
    Issuer: EMAILADDRESS=info@crate.io, CN=*.crate.io, OU=Cryptography Department, O=Crate.io, L=Dornbirn, ST=Vorarlberg, C=AT
    Serial number: f13562ec6184401e
    Valid from: Mon Jun 12 13:09:17 CEST 2017 until: Wed May 19 13:09:17 CEST 2117
    Certificate fingerprints:
         MD5:  BB:A1:79:53:FE:71:EC:61:2A:19:81:E8:0E:E8:C9:81
         SHA1: 96:66:C1:01:49:17:D1:19:FB:DB:83:86:50:3D:3D:AD:DA:F7:C6:A9
         SHA256: 69:82:C5:24:9A:A1:AE:DF:80:29:7A:26:92:C1:A5:9F:AF:7D:03:56:CC:C3:E9:73:3B:FD:85:66:35:D6:8A:9B
         Signature algorithm name: SHA256withRSA
         Version: 3

    Extensions:

    #1: ObjectId: 2.5.29.35 Criticality=false
    AuthorityKeyIdentifier [
    KeyIdentifier [
    0000: CD 29 4E 07 3D C3 7C D0   16 45 FB 0A CE 8D B4 98  .)N.=....E......
    0010: B7 A8 4C 79                                        ..Ly
    ]
    [EMAILADDRESS=info@crate.io, CN=*.crate.io, OU=Cryptography Department, O=Crate.io, L=Dornbirn, ST=Vorarlberg, C=AT]
    SerialNumber: [    f13562ec 6184401e]
    ]

    #2: ObjectId: 2.5.29.19 Criticality=false
    BasicConstraints:[
      CA:true
      PathLen:2147483647
    ]

    #3: ObjectId: 2.5.29.14 Criticality=false
    SubjectKeyIdentifier [
    KeyIdentifier [
    0000: CD 29 4E 07 3D C3 7C D0   16 45 FB 0A CE 8D B4 98  .)N.=....E......
    0010: B7 A8 4C 79                                        ..Ly
    ]
    ]

    Trust this certificate? [no]:  yes
    Certificate was added to keystore


Import the Signed Certificate
-----------------------------

Now we have a signed certificate, signed by either from a official CA
or from our own CA. Let's import it to the keystore.

Command::

    keytool -import -keystore keystore -file server.crt -alias server

Output::

    Enter keystore password:
    Enter key password for <server>
    Certificate reply was installed in keystore


Configuring CrateDB
-------------------

Finally, you want to supply the keystore/truststore configuration in the
CrateDB config, see :ref:`ssl_config`.
