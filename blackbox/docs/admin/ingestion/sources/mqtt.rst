.. highlight:: psql
.. _ingest_mqtt:

=====================
MQTT Ingestion Source
=====================

.. sidebar:: Overview

   .. figure:: mqtt-01.png
      :alt: A diagram of CrateDB acting as an MQTT endpoint

MQTT_ (Message Queue Telemetry Transport) is a machine-to-machine communication
protocol particularly well suited for machine data and Internet of Things (IoT)
applications.

The MQTT ingestion source allows CrateDB to function as an MQTT endpoint for
data ingestion. Incoming messages can be written to user defined tables
according to :ref:`user defined rules <administration-ingestion-rules>`. These
tables can then be queried or polled at will by CrateDB clients.

Using CrateDB as an MQTT endpoint removes the need for intermediary brokers,
message queues, or MQTT subscribers doing message transformation or
persistence.

.. NOTE::

   The MQTT ingestion source an
   :ref:`enterprise feature <enterprise_features>`.

.. rubric:: Table of Contents

.. contents::
   :local:

.. _ingest_mqtt_config:

Configuration
=============

This ingestion source adds a few additional settings that you can configure.

These node settings can be :ref:`configured like usual <config>`, via the
``crate.yml`` configuration file or as command line parameters using the ``-C``
option.

Node Settings
-------------

.. NOTE::

   Node settings only affect the node they are configured on.

**ingestion.mqtt.enabled**
  | *Default:*   ``false``
  | *Runtime:*  ``no``

  Enables the MQTT_ ingestion source on this node.

.. _ingestion_mqtt_port:

**ingestion.mqtt.port**
  | *Default:*   ``1883``
  | *Runtime:*  ``no``

  TCP port on which the endpoint is exposed.

  Can either be a number, or a string defining a port range. The first free
  port of this range is used.

**ingestion.mqtt.timeout**
  | *Default:*   ``10s``
  | *Runtime:*  ``no``

  The default keep-alive timeout for establised connections.

  This timeout is used if the client does not specify a ``keepAlive`` option
  when sending the ``CONNECT`` message.

SSL Support
...........

SSL support is available for the MQTT connections and can be enabled using the
``ssl.ingestion.mqtt.enabled`` setting. When SSL is enabled for the MQTT
connection, consider changing ``ingestion.mqtt.port`` to ``8883`` which is
the TCP/IP registered port for MQTT over SSL.

Once CrateDB is configured to use encrypted MQTT connections, unencrypted
connections will not be supported anymore.

.. SEEALSO::

  :ref:`admin_ssl`

.. _ingest_mqtt_usage:

Usage
=====

Quality of Service
------------------

This ingestion source only implements MQTT Quality of Service (QoS) `level
one`_, meaning that messages will be delivered at least once to the endpoint
and stored in the database.

Other QoS levels are not supported and messages sent with those levels are
discared and an error is logged.

.. _level one: http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718101

Data Ingestion
--------------

The ingestion of data is controlled with :ref:`administration-ingestion-rules`.

Without any defined valid rule, no data will be ingested at all and messages
will not be acknowledged. The ``PUBLISH`` messages will receive the
corresponding ``PUBACK`` reply only after the message is stored in all matching
ingest rules target tables (as there can be multiple ingestion rules configured
for this source). If at least one matching ingestion rule fails to store the
incoming message, the client will not receive any reply and will have to resend
the request message.
Ingest rules execution failures will usually be temporary (eg. a mismatch
between a rule target table schema and ingestion source payload structure) and
the administrator will have the opportunity to fix the rule execution between
message retransmissions (eg. alter the target table schema).

The ``source_ident`` of this implementation is: ``mqtt``

Rule conditions and the target table must match the `MQTT data structure`_.

The default user for the ``INSERT`` operations in the ``target_table`` is the
superuser ``crate``.

.. _ingest_mqtt_data_structure:

MQTT Data Structure
...................

+------------+------------+-------------------------------------------------+
| Name       | Type       | Description                                     |
+============+============+=================================================+
| client_id  | STRING     | ID of the client that sent the MQTT message.    |
+------------+------------+-------------------------------------------------+
| packet_id  | STRING     | ``packet_id`` of the ``PUBLISH`` message.       |
+------------+------------+-------------------------------------------------+
| topic      | STRING     | ``topic`` of the ``PUBLISH`` message.           |
+------------+------------+-------------------------------------------------+
| ts         | TIMESTAMP  | Insert timestamp (``CURRENT_TIMESTAP``).        |
+------------+------------+-------------------------------------------------+
| payload    | OBJECT     | ``payload`` of the ``PUBLISH`` message.         |
|            |            | **Must be a valid JSON string!**                |
+------------+------------+-------------------------------------------------+

Example
.......

To start ingesting with the MQTT data ingestion source, first create a target
table matching the `MQTT data structure`_, like so::

    cr> CREATE TABLE mqtt_temperature (
    ...  "client_id" STRING,
    ...  "packet_id" INTEGER,
    ...  "topic" STRING,
    ...  "ts" TIMESTAMP,
    ...  "payload" OBJECT(IGNORED),
    ...  PRIMARY KEY ("client_id", "packet_id")
    ... )
    CREATE OK, 1 row affected (... sec)

The structure of this target table is very important, as it can prevent or
allow duplicates in case of message retransmission.

In this example, if a message is delivered multiple times, the message will
only be stored once in the ``mqtt_temperature`` table because the ``PRIMARY
KEY`` includes both the ``client_id`` and ``packet_id``. If the ``packet_id``
were to be omitted from the primary key, a message arriving at CrateDB multiple
times will be stored multiple times.

Once you have done that, you can create :ref:`ingestion rules
<administration-ingestion-rules>`, like so::

    cr> CREATE INGEST RULE temperature ON mqtt
    ...  WHERE topic like 'temperature/%'
    ...  INTO mqtt_temperature;
    CREATE OK, 1 row affected (... sec)

.. SEEALSO::

   `Getting Started With CrateDB as an MQTT Endpoint
   <https://crate.io/a/getting-started-cratedb-mqtt-endpoint/>`_ (November
   2017)

.. _MQTT: http://mqtt.org/
