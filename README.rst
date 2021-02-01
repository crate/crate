.. image:: docs/_static/crate-logo.png
    :alt: CrateDB
    :target: https://crate.io/

----

.. image:: https://github.com/crate/crate/workflows/CrateDB%20SQL/badge.svg?branch=master
    :target: https://github.com/crate/crate/actions?query=workflow%3A%22CrateDB+SQL%22

.. image:: https://img.shields.io/badge/docs-latest-brightgreen.svg
    :target: https://crate.io/docs/en/latest/

.. image:: https://img.shields.io/badge/container-docker-green.svg
    :target: https://hub.docker.com/_/crate/

|


About
=====
CrateDB is a distributed SQL database that makes it simple to store and analyze
massive amounts of machine data in real-time.

CrateDB offers the scalability and flexibility typically associated with a
NoSQL database, is designed to run on inexpensive commodity servers and can be
deployed and run on any sort of network - from personal computers to
multi-region hybrid clouds.

The smallest CrateDB clusters can easily ingest tens of thousands of records
per second. The data can be queried, ad-hoc, in parallel across the whole
cluster in real time.


Features
========

- Standard SQL interface available via HTTP API and PostgreSQL wire protocol.
- Dynamic schemas, queryable objects, geospatial features, time series data support,
  and realtime full-text search providing functionality for handling both relational
  and document oriented nested data structures.
- Horizontally scalable, highly available and fault tolerant clusters that run very
  well in virtualized and containerised environments.
- Extremely fast distributed query execution.
- Auto-partitioning, auto-sharding, and auto-replication.
- Self-healing and auto-rebalancing.



Screenshots
===========

CrateDB provides an `Admin UI`_:

.. image:: crate-admin.gif
    :alt: Screenshots of the CrateDB admin UI


Try CrateDB
===========

The fastest way to try CrateDB out is by running::

    $ bash -c "$(curl -L try.crate.io)"

Or spin up the official `Docker image`_::

    $ docker run --publish 4200:4200 --publish 5432:5432 crate -Cdiscovery.type=single-node

Visit the `getting started`_ page to see all the available download and install options.

Once you're up and running, head over to the `introductory docs`_. In order to
connect to CrateDB from applications, see our list of `CrateDB clients and tools`_
or use the `CrateDB shell`_ in order to invoke ad-hoc commands.

For running CrateDB on Kubernetes, the `CrateDB Kubernetes Operator`_ and its
`CrateDB Kubernetes Operator Documentation`_ might be of interest.


Contributing
============

This project is primarily maintained by `Crate.io`_, but we welcome community
contributions!

See the `developer docs`_ and the `contribution docs`_ for more information.


Help
====

Looking for more help?

- Read the `project documentation`_ and try one of our `beginner tutorials`_,
  `how-to guides`_, or check out the `reference manual`_.
- Check out our `support channels`_.
- `Crate.io`_ also offers `CrateDB Cloud`_, a fully-managed CrateDB Database
  as a Service (DBaaS), see also `CrateDB Cloud Tutorials`_.


.. _Admin UI: https://crate.io/docs/crate/admin-ui/
.. _beginner tutorials: https://crate.io/docs/crate/tutorials/
.. _contribution docs: CONTRIBUTING.rst
.. _Crate.io: https://crate.io/
.. _CrateDB clients and tools: https://crate.io/docs/crate/clients-tools/
.. _CrateDB Cloud: https://crate.io/products/cratedb-cloud/
.. _CrateDB Cloud Tutorials: https://crate.io/docs/cloud/
.. _CrateDB Kubernetes Operator: https://github.com/crate/crate-operator
.. _CrateDB Kubernetes Operator Documentation: https://crate-operator.readthedocs.io/
.. _CrateDB shell: https://crate.io/docs/crate/crash/
.. _developer docs: devs/docs/index.rst
.. _Docker image: https://hub.docker.com/_/crate/
.. _getting started: https://crate.io/docs/crate/tutorials/en/latest/install-run/
.. _how-to guides: https://crate.io/docs/crate/howtos/
.. _introductory docs: https://crate.io/docs/crate/tutorials/
.. _project documentation: https://crate.io/docs/
.. _reference manual: https://crate.io/docs/crate/reference/
.. _support channels: https://crate.io/support/
