.. image:: blackbox/docs/_static/crate-logo.png
    :alt: CrateDB
    :target: https://crate.io/

----

.. image:: https://travis-ci.org/crate/crate.svg?branch=master
    :target: https://travis-ci.org/crate/crate

.. image:: https://img.shields.io/badge/docs-latest-brightgreen.svg
    :target: https://crate.io/docs/en/latest/

.. image:: https://img.shields.io/badge/container-docker-green.svg
    :target: https://hub.docker.com/_/crate/

|

CrateDB is a distributed SQL database that makes it simple to store and analyze massive amounts of machine data in real-time.

Features of CrateDB:

- Standard SQL plus dynamic schemas, queryable objects, geospatial features, time series data, first-class BLOB support, and realtime full-text search.
- Horizontally scalable, highly available, and fault tolerant clusters that run very well in virtualized and containerised environments.
- Extremely fast distributed query execution.
- Auto-partitioning, auto-sharding, and auto-replication.
- Self-healing and auto-rebalancing.

CrateDB offers the scalability and flexibility typically associated with a NoSQL database and is designed to run on inexpensive commodity servers and can be deployed and run across any sort of network. From personal computers to multi-region hybrid clouds.

The smallest CrateDB clusters can easily ingest tens of thousands of records per second. And this data can be queried, ad-hoc, in parallel across the whole cluster in real time.

Screenshots
===========

CrateDB provides an admin UI:

.. image:: crate-admin.gif
    :alt: Screenshots of the CrateDB admin UI
    :target: http://play.crate.io/

Try CrateDB
===========

The fastest way to try CrateDB out is by running::

    $ bash -c "$(curl -L try.crate.io)"

Or spin up the official `Docker image`_::

    $ docker run -p 4200:4200 crate

Visit the `getting started`_ page to see all the available download and install options.

Once you're up and running, head on over to `the introductory docs`_.

Contributing
============

This project is primarily maintained by Crate.io_, but we welcome community
contributions!

See the `developer docs`_ and the `contribution docs`_ for more information.

Help
====

Looking for more help?

- Read `the project documentation`_
- Check `StackOverflow`_ for common problems
- Chat with us on `Slack`_
- Get `paid support`_

.. _contribution docs: CONTRIBUTING.rst
.. _Crate.io: http://crate.io/
.. _developer docs: devs/docs/index.rst
.. _Docker image: https://hub.docker.com/_/crate/
.. _getting started: https://crate.io/docs/getting-started/
.. _paid support: https://crate.io/pricing/
.. _Slack: https://crate.io/docs/support/slackin/
.. _StackOverflow: https://stackoverflow.com/tags/crate
.. _the introductory docs: https://crate.io/docs/stable/hello.html
.. _the project documentation: https://crate.io/docs/
