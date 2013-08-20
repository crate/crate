=====================
Elasticsearch Plugins
=====================


Distinct Date Histogram Facet
=============================

This facet counts distinct values for string and numeric fields.

Example::

    {
        "query" : {
            "match_all" : {}
        },
        "facets" : {
            "distinct" : {
                "distinct_date_histogram" : {
                    "field" : "field_name",
                    "value_field" : "value_field_name",
                    "interval" : "day"
                }
            }
        }
    }

Result::

 "distinct":{
     "_type":"distinct_date_histogram",
     "entries":[
         "{"time":950400000,"count":2},
         "{"time":1555200000,"count":3}
     ],
     "count":4
 }

The "count" is the number of distinct values in the time period. The
outer "count" is the number of total distinct values.

Works like the "date_histogram" with these exceptions:

    - value_field is mandatory
    - value_field must be of type String or Numeric
    - no value_script


"Latest" Facet
==============

This facet collapses matching documents to ``key_field`` and uses only
the document with the highest value of ``ts_field``.
The result is always sorted on descending ``value_field``.

Example::

  {"query": { "match_all":{}},
   "facets": {
      "l": {
       "latest": {
        "size": 100,
        "start": 50,
        "key_field": "mykey",
        "value_field": "num_comments",
        "ts_field": "created_at"
      }
    }
   }}

Result::

  "facets" : {
    "l" : {
      "_type" : "latest",
      "total": 25,
      "entries" : [ {
        "value" : 52127,
        "key" : 5758683603492929880,
        "ts" : 1325577893000
      }, {
        "value" : 14980,
        "key" : 5758683371564695759,
        "ts" : 1325447138000
      }, {
        "value" : 10392,
        "key" : 5758683603492929669,
        "ts" : 1325577885000
      } ]
    }
  }

Restrictions of the "Latest" facet
----------------------------------

Documents need to be routed in a way that the same values of
``key_field`` are on the same shard. This can be accomplished by
setting the ``_routing`` attribute upon indexing. This is needed for
performance reasons, so the fields can be collapsed per shard.

Currently the ``key_field`` and ``ts_field`` need to be longs, while
the ``value_field`` is required to be of type Numeric.


Installation
============

* Clone this repo with git clone
  git@github.com:crate/elasticsearch-timefacets-plugin.git
* Checkout the tag (find out via git tag) you want to build with
  (possibly master is not for your elasticsearch version)
* Run: mvn clean package -DskipTests=true – this does not run any unit
  tests, as they take some time. If you want to run them, better run
  mvn clean package
* Install the plugin: /path/to/elasticsearch/bin/plugin -install
  elasticsearch-timefacets-plugin -url
  file:///$PWD/target/releases/elasticsearch-timefacets-plugin-$version.jar

Maven
=====

To use this project with maven follow the steps described at
https://github.com/lovelysystems/maven


Deployment
==========

The distributionManagement section in the pom contains the actual
repository urls on github. It will lead to an error if you try to
deploy to those urls, because these are no Maven API endpoints, where
maven could upload the artifacts.

So to deploy to the Lovely Systems Maven repository first clone
https://github.com/lovelysystems/maven to your local machine and set
the deployment target location on the commandline like this::

 mvn -DaltDeploymentRepository=repo::default::file:../maven/releases clean deploy

After deployment simply commit the changes in the maven repository
project and push.

This approach was take from the very useful blog entry at
http://cemerick.com/2010/08/24/hosting-maven-repos-on-github/

