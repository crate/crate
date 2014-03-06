===========
Search Into
===========

Via the ``_search_into`` endpoint it is possible to put the result of
a given query directly into an index. It is inspired from the `Select
Into SQL-Statement
<http://www.w3schools.com/sql/sql_select_into.asp>`__

Here are some common use-cases that might be fulfilled by using this
endpoint.


.. doctest::
    :hide:

    >>> ep = Endpoint('http://localhost:44202')
    >>> ep2 = Endpoint('http://localhost:44203')


Copy an existing index
======================

To copy a whole index, the ``_index`` field source needs to be defined as a
string literal like this::

    >>> fields = ["_id", "_source", ["_index", "'newindex'"]]

There are two special things happening here. When defining a field as an
array of 2 items, the first item is considered as the target-field name and
the second item is considered as the source value. The above example
defines a string literal (denoted by the surrounding apostrophes) as
the value of the ``_index`` field::

    >>> ep.ppost("/users/_search_into?pretty=1", {"fields":fields})
    {
        ...
        "writes": [
            {
                "failed": 0, 
                "index": "users", 
                "node": "...", 
                "shard": 0, 
                "succeeded": 2, 
                "total": 2
            }, 
            {
                "failed": 0, 
                "index": "users", 
                "node": "...", 
                "shard": 1, 
                "succeeded": 2, 
                "total": 2
            }
            ...

Now a new index with the same content was created::


    >>> ep.ppost("/newindex/_flush")
    {
        "_shards": {
            "failed": 0,
            "successful": 1,
            "total": 1
        }
    }

    >>> ep.ppost("/newindex/_refresh")
    {
        "_shards": {
            "failed": 0,
            "successful": 1,
            "total": 1
        }
    }

    >>> ep.pget("/newindex/_search?pretty=1")
    {
        ...
        "hits": {
            "hits": [
                {
                    "_id": "...", 
                    "_index": "newindex", 
                    "_score": 1.0, 
                    "_source": {
                        "name": "car"
                    }, 
                    "_type": "d"
                }, 
                ...
            "total": 4
        }, 
        ...

Copy an existing index into another cluster
===========================================

To copy one index to another cluster set the option ``targetNodes`` in
the payload. The value of the option might be given as a single value in
the format <hostname>:<port> or as a list of such values.

Initially, the second cluster does not contain any indizes::

    >>> ep2.pget("/_status?pretty=1")
    {
        ...
        "indices": {}
    }

    >>> payload = {"fields":["_id", "_source"],
    ...            "targetNodes":"localhost:44303"}

    >>> ep.ppost("/users/_search_into?pretty=1", payload)
    {
        "_shards": {
            "failed": 0, 
            "successful": 2, 
            "total": 2
        }, 
        "failed": 0, 
        "succeeded": 0, 
        "total": 0, 
        "writes": [
            {
                "failed": 0, 
                "index": "users", 
                "node": "...", 
                "shard": 0, 
                "succeeded": 2, 
                "total": 2
            }, 
            {
                "failed": 0, 
                "index": "users", 
                "node": "...", 
                "shard": 1, 
                "succeeded": 2, 
                "total": 2
            }
        ]
    }

Now the new index with the same content was created in the other
cluster::

    >>> ep2.ppost("/users/_flush")
    {
        "_shards": {
            "failed": 0,
            "successful": 1,
            "total": 1
        }
    }

    >>> ep2.ppost("/users/_refresh")
    {
        "_shards": {
            "failed": 0,
            "successful": 1,
            "total": 1
        }
    }

    >>> ep2.pget("/users/_search?pretty=1")
    {
        ...
        "hits": {
            "hits": [
                {
                    "_id": "...", 
                    "_index": "users", 
                    "_score": 1.0, 
                    "_source": {
                        "name": "car"
                    }, 
                    "_type": "d"
                }, 
                ...
            "total": 4
        }, 
        ...
