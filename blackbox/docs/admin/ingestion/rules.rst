.. highlight:: psql

.. Hidden: CREATE TABLE::

    cr> create table mqtt_humidity (
    ... id long,
    ... data string,
    ... date timestamp
    ... );
    CREATE OK, 1 row affected (... sec)

.. _administration-ingestion-rules:

===============
Ingestion Rules
===============

Ingestion rules define where and how the incoming data of a specific
:ref:`ingestion source <administration-ingestion-sources>` should be routed and
stored.

.. rubric:: Table of Contents

.. contents::
   :local:

Creating Ingestion Rules
========================

Use the :ref:`create-ingest-rule` statement to create ingestion rules.

Suppose we have an ingestion source for an MQTT_ endpoint, that sends messages
containing temperature and humidity measurements of our open space in the
following data structure:

- ``client_id`` represents the id of the client sending the message
- ``payload`` a json string containing the measurements
- ``topic`` a string hierarchically structured

In our example, the topic can be either:

- ``openspace/temperature``
- ``openspace/humidity``

The first thing we need to do is to create a target table that uses the same
structure as our ingestion source::

    cr> CREATE TABLE mqtt_temperature (
    ...  "client_id" STRING,
    ...  "topic" STRING,
    ...  "payload" OBJECT(IGNORED),
    ...  PRIMARY KEY ("client_id")
    ... )
    CREATE OK, 1 row affected (... sec)

Let's say we would like to filter data streams by topic and write the
temperature messages to the ``mqtt_temperature`` table.

We could do this::

    cr> CREATE INGEST RULE mqtt_temperature_rule
    ... ON mqtt
    ... WHERE topic like '%temperature%'
    ... INTO mqtt_temperature;
    CREATE OK, 1 row affected (... sec)

In the ``WHERE CLAUSE`` of our example, ``topic`` stands for the message
topic of the MQTT_ endpoint's data structure, as detailed above.

Multiple rules can be created for the same ingestion source. For example, to
write the humidity data to the ``mqtt_humidity`` target table,
you could do this::

    cr> CREATE INGEST RULE mqtt_humidity_rule
    ... ON mqtt
    ... WHERE topic like '%humidity%'
    ... INTO mqtt_humidity;
    CREATE OK, 1 row affected (... sec)

.. NOTE::

   When the target table is dropped, all related rules become non functional.
   If the target table is recreated, the rules become functional again.

Listing Ingestion Rules
=======================

CrateDB exposes ingestion rules via the :ref:`Ingestion Rules Table
<information_schema_ingest>`.

Query the ``information_schema.ingestion_rules`` table to get information about
existing ingestion rules.

For example::

    cr> SELECT * FROM information_schema.ingestion_rules order by rule_name;
    +------------------------------+-----------------------+--------------+----------------------+
    | condition                    | rule_name             | source_ident | target_table         |
    +------------------------------+-----------------------+--------------+----------------------+
    | "topic" LIKE '%humidity%'    | mqtt_humidity_rule    | mqtt         | doc.mqtt_humidity    |
    | "topic" LIKE '%temperature%' | mqtt_temperature_rule | mqtt         | doc.mqtt_temperature |
    +------------------------------+-----------------------+--------------+----------------------+
    SELECT 2 rows in set (... sec)

Deleting Ingestion Rules
========================

Use the :ref:`drop-ingest-rule` statement to delete ingestion rules.

For example::

	cr> DROP INGEST RULE mqtt_temperature_rule;
	DROP OK, 1 row affected (... sec)

.. Hidden: DROP TABLE::

	cr> DROP INGEST RULE mqtt_humidity_rule;
	DROP OK, 1 row affected (... sec)

	cr> DROP TABLE mqtt_temperature;
	DROP OK, 1 row affected (... sec)

	cr> DROP TABLE mqtt_humidity;
	DROP OK, 1 row affected (... sec)

.. _MQTT: http://mqtt.org/
