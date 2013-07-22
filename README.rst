========
Crate DB
========

Crate is a shared nothing, fully searchable, document oriented
cluster database.

http://en.wikipedia.org/wiki/Document-oriented_database

Crate depends on elasticsearch, a real-time search and analytics engine.
For details see
http://www.elasticsearch.org/

Installed plugins
=================

elasticsearch-crate-plugin
--------------------------

Crate plugin, included as sub-module in this package. This plugin
contains Crate specific default settings and a rest endpoint reachable
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

A web based admin interface for Crate.

http://localhost:9200/admin


docs
----

The Crate documentation available under

http://localhost:9200/_plugin/docs

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


Installation
============

Download the latest version of Crate, unpack it and start it:

 $ bin/crate


Under *nix system, the command will start the process in the background.
To run it in the foreground, add the -f switch to it:

 $ bin/crate -f


Configuration
=============

The settings file is located at config/crate.yml
The used default options are documented in this file.
To load a settings file located somewhere else in the system
pass the option -Des.config=/path/to/config.yml to the start script.

Any option can be configured either by the config file or as system
property. If using system properties the required prefix 'es.' will
be ignored.

For example, configuring the cluster name by using system properties
will work this way:

 $ crate -Des.cluster.name=cluster

Which is exactly the same like setting the cluster name by the config
file:

 cluster.name = cluster

Settings will get applied in the following order where the latter one
will overwrite the prior one:

 1. internal defaults
 2. system properties
 3. options from config file
