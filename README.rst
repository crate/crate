========
Crate DB
========

Crate is a shared nothing, fully searchable, document oriented
cluster database.

http://en.wikipedia.org/wiki/Document-oriented_database

Crate depends on elasticsearch, a real-time search and analytics engine.
For details see
http://www.elasticsearch.org/


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


Installed plugins
=================

elasticsearch-crate-plugin
--------------------------

Crate plugin, included as sub-module in this package. This plugin
contains crate specific default settings and a rest endpoint reachable
at '/admin' displaying a system overview as html page.


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

crate-admin
-----------

A web based admin interface for crate.

http://localhost:9200/admin


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


Crate settings
==============

Crate settings can be specified in 3 different file formats in the
``config`` directory:

- YAML: crate.yml
- JSON: crate.json
- Java Properties File: crate.properties

The used default options are described in the delivered example
configuration which can be found at config/crate.yml

Any option can be configured either by a config file or as system
property. If using system properties the required prefix 'crate.' will
be ignored. (The following options has to be prefixed with 'es' instead
of crate: es.logger.prefix, es.pidfile, es.foreground, es.max-open-files)

For example, configuring the cluster name by using system properties
will work this way:

 $ crate -Dcrate.cluster.name=cluster

Which is exactly the same like setting the cluster name by the config
file:

 cluster.name = cluster

Settings will get applied in the following order where the latter one
will overwrite the prior one:

 1. internal defaults
 2. system properties
 3. options from config file located at -Dcrate.default.config
 4. options from config file located at -Dcrate.config
 5. options from config file located at default location
    (<CRATE-HOME>/config/config.yml) if -Dcrate.config is not
    defined
