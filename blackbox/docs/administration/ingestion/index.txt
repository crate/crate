.. _ingestion:

===================
Ingestion Framework
===================

.. sidebar:: Overview

   .. figure:: ingestion-01.png
      :alt: A diagram of the ingestion framework

As well as allowing you to ingest data via standard SQL inserts, CrateDB
provides an ingestion framework that allows you to ingest data through custom
event-based ingestion sources.

The CrateDB ingestion framework provides highly available and distributed
ingestion sources for custom protocols where thousands of events are being sent
simultaneously, such as :ref:`MQTT <ingest_mqtt>`. No intermediate queues or
brokers need to be set up.

Ingestion is configured via *ingestion rules*, which define how incoming data
gets routed and stored. For more, details see
:ref:`ingestion rules docs <administration-ingestion-rules>`.

Ingestion rules are bound to a specific *ingestion source*.

An :ref:`ingestion source <administration-ingestion-sources>` is a registered
source of external data. External data access is always implementation
specific. For example, an ingestion source could get data from an :ref:`MQTT
<ingest_mqtt>` endpoint, an HTTP webhook, or a module listening for kernel
events.

.. rubric:: Table of Contents

.. toctree::
    :maxdepth: 2
    :titlesonly:

    sources/index
    rules
