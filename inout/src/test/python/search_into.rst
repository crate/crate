===========
Search Into
===========

Via the ``_search_into`` endpoint it is possible to put the result of
a given query directly into an index. It is inspired from the `Select
Into SQL-Statement
<http://www.w3schools.com/sql/sql_select_into.asp>`__

Here are some common use-cases that might be fulfilled by using this
endpoint.


Copy an existing index
======================

To copy a whole index, the ``_index`` field source needs to be defined as a
string literal like this::

    >>> fields = ["_id", "_source", ["_index", "'newindex'"]]

There are two special things happening here. When defining a field as an
array of 2 items, the first item is considered as the target-field name and
the second item is considered as the source value. The above example
defines a string literal (denoted by the surrounding apostrophes) as
the value of the ``_index`` field.

    >>> post("/users/_search_into?pretty=1", {"fields":fields})
    {
      "writes" : [ {
        "index" : "users",
        "shard" : 0,
        "node" : "...",
        "total" : 2,
        "succeeded" : 2,
        "failed" : 0
      }, {
        "index" : "users",
        "shard" : 1,
        "node" : "...",
        "total" : 2,
        "succeeded" : 2,
        "failed" : 0
      }...

Now a new index with the same content was created::


    >>> post("/newindex/_flush")
    {"ok":true,"_shards":{"total":1,"successful":1,"failed":0}}

    >>> post("/newindex/_refresh")
    {"ok":true,"_shards":{"total":1,"successful":1,"failed":0}}


    >>> get("/newindex/_search?pretty=1")
    {..."hits" : {
        "total" : 4,
        ...{
          "_index" : "newindex",
          "_type" : "d",
          "_id" : "1",
          "_score" : 1.0, "_source" : {"name":"car"}
        }...


Copy an existing index into another cluster
===========================================

To copy one index to another cluster set the option ``targetNodes`` in
the payload. The value of the option might be given as a single value in
the format <hostname>:<port> or as a list of such values.

Initially, the second cluster does not contain any indizes::

    >>> node2.get("/_status?pretty=1")
    {
      ...
      "indices" : { }
    }

    >>> payload = {"fields":["_id", "_source"],
    ...            "targetNodes":"localhost:9301"}

    >>> post("/users/_search_into?pretty=1", payload)
    {
      "writes" : [ {
        "index" : "users",
        "shard" : 0,
        "node" : "...",
        "total" : 2,
        "succeeded" : 2,
        "failed" : 0
      }, {
        "index" : "users",
        "shard" : 1,
        "node" : "...",
        "total" : 2,
        "succeeded" : 2,
        "failed" : 0
      } ],
      "total" : 0,
      "succeeded" : 0,
      "failed" : 0,
      "_shards" : {
        "total" : 2,
        "successful" : 2,
        "failed" : 0
      }
    }

Now the new index with the same content was created in the other
cluster::

    >>> node2.post("/users/_flush")
    {"ok":true,"_shards":{"total":1,"successful":1,"failed":0}}

    >>> node2.post("/users/_refresh")
    {"ok":true,"_shards":{"total":1,"successful":1,"failed":0}}

    >>> node2.get("/users/_search?pretty=1")
    {..."hits" : {
        "total" : 4,
        ...{
          "_index" : "users",
          "_type" : "d",
          "_id" : "1",
          "_score" : 1.0, "_source" : {"name":"car"}
        }...
