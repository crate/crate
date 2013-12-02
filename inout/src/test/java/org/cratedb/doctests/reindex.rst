=======
Reindex
=======

.. doctest::
    :hide:

    >>> ep = Endpoint('http://localhost:9202')
    >>> ep2 = Endpoint('http://localhost:9203')


Via the ``_reindex`` endpoint it is possible to reindex one or all indexes
with a given query.

Reindex an existing Index
=========================

Create an index 'test' with a custom analyzer with the stop word 'guy'::

    >>> ep.pput("/test", {"settings": {"index": {"number_of_shards":1,
    ...                           "number_of_replicas":0,
    ...                           "analysis": {"analyzer": {"myan": {"type": "stop", "stopwords": ["guy"]}}}}}})
    {
        "acknowledged": true, 
        "ok": true
    }

Create the mapping for type 'a' and use the custom analyzer as index analyzer
and use a simple search analyzer::

    >>> ep.ppost("/test/a/_mapping", {"a": {"properties": {"name": {"type": "string", "index_analyzer": "myan", "search_analyzer": "simple", "store": "yes"}}}})
    {
        "acknowledged": true, 
        "ok": true
    }

Add a document::

    >>> ep.ppost("/test/a/1", {"name": "a nice guy"})
    {
        ...
        "ok": true
    }

    >>> ep.refresh()

Querying for a non stop word term delivers a result::

    >>> ep.ppost("/test/a/_search?pretty", {"query": {"text": {"name": "nice"}}})
    {
        ...
        "hits": {
            "hits": [
            ...
            "total": 1
            ...
    }

Querying for a stop word delivers no results::

    >>> ep.ppost("/test/a/_search?pretty", {"query": {"text": {"name": "guy"}}})
    {
        ...
        "hits": {
            "hits": [],
            ...
            "total": 0
            ...
    }

Now update the stop words configuration. To update settings the index has to
be closed first and then reopened::

    >>> ep.ppost("/test/_close", {})
    {
        "acknowledged": true, 
        "ok": true
    }

    >>> ep.pput("/test/_settings", {"analysis": {"analyzer": {"myan": {"type": "stop", "stopwords": ["nice"]}}}})
    {
        "acknowledged": true, 
        "ok": true
    }

    >>> ep.ppost("/test/_open", {})
    {
        "acknowledged": true, 
        "ok": true
    }

    >>> ep.refresh()

As the index has not been reindexed yet, the query for 'nice' still delivers
a result::

    >>> ep.ppost("/test/a/_search?pretty", {"query": {"text": {"name": "nice"}}})
    {
        ...
        "hits": {
            "hits": [
            ...
            "total": 1
            ...
    }

Now do a reindex on the index 'test'::

    >>> ep.ppost("/test/_reindex", {})
    {
        "_shards": {
            "failed": 0, 
            "successful": 1, 
            "total": 1
        }, 
        "failed": 0, 
        "succeeded": 0, 
        "total": 0, 
        "writes": [
            {
                "failed": 0, 
                "index": "test", 
                "node": "...", 
                "shard": 0, 
                "succeeded": 1, 
                "total": 1
            }
        ]
    }

    >>> ep.refresh()

No more result when querying for the new stop word 'nice'::

    >>> ep.ppost("/test/a/_search?pretty", {"query": {"text": {"name": "nice"}}})
    {
        ...
        "hits": {
            "hits": [],
            ...
            "total": 0
            ...
    }

The removed stop word 'guy' now delivers a result::

    >>> ep.ppost("/test/a/_search?pretty", {"query": {"text": {"name": "guy"}}})
    {
        ...
        "hits": {
            "hits": [
            ...
            "total": 1
            ...
    }
