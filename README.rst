========
Crate DB
========

Crate is a shared nothing, fully searchable, document oriented
cluster database.

http://en.wikipedia.org/wiki/Document-oriented_database

Differences to MongoDB, CouchDb
===============================

- Each node is the same, no need to have special types of nodes.

- No MapReduce or specialities needed for grouping or searching data.

- Full blown search engine built-in.

Differences to Mysql, Postgresql
================================

- Shared nothing, horizontal scalable.

- Document based instead of relational.

- No transactions supported.

- Dynamic Schema support, which means it is possible to add fields on
  the fly.

- Denormalized storage required.


BUILD
=====

Building a tarball is done by maven with the command::

    >>> mvn clean package

Resulting tarballs will reside in the folder ``releases``.


Crate settings
==============

Crate has some default settings, which of course can be overwritten.
The default cluster name is ``crate``. Deleting all indices is disabled
by default.

Custom crate settings can be specified in 3 different file formats in
the ``config`` directory:

- YAML: crate.yml
- JSON: crate.json
- Java Properties File: crate.properties


Installed plugins
=================

elasticsearch-cratedefaults-plugin
----------------------------------

Default settings plugin, included as sub-module in this package.


elasticsearch-inout-plugin
--------------------------

An Elasticsearch plugin which provides the ability to export data by
query on server side. For details see
https://github.com/crate/elasticsearch-inout-plugin


elasticsearch-timefacets-plugin
-------------------------------

A plugin bundle for time based facets. For details see
https://github.com/crate/elasticsearch-timefacets-plugin


Installed site plugins
======================

elasticsearch-head
------------------

A web front end for an Elasticsearch cluster.

http://localhost:9200/_plugin/head


bigdesk
-------

Live charts and statistics for Elasticsearch cluster.

http://localhost:9200/_plugin/bigdesk


segmentspy
----------

Elasticsearch plugin to watch segment dynamics (additions, merges,
deletes)

http://localhost:9200/_plugin/segmentspy
