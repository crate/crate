============
Blob Support
============

Upload a file that doesn't match the sha1 hash::

    >>> put('/test/_blobs/d937ea65641c23fadc83616309e5b0e11acc5806', data='asdf')
    HTTP Error 400: Bad Request

Try to get a non-existing file::

    >>> get('/test/_blobs/d937ea65641c23fadc83616309e5b0e11acc5806')
    HTTP Error 404: Not Found

Upload valid files::

    >>> small_data = 'a' * 1500
    >>> small_digest = sha1sum(small_data)
    >>> r = put(blob_url(small_digest), data=small_data)
    >>> print r
    HTTP Response 201

Note that the content length is specified in the response in order to
let keep alive clients know that they don't have to wait for data
after the put and may close the connection if appropriate::

    >>> r.headers
    Content-Length: 0

    >>> big_data = 'a' * (1024*600)
    >>> big_digest = sha1sum(big_data)
    >>> put(blob_url(big_digest), data=big_data)
    HTTP Response 201

    >>> big_data = ''

Uploading the same file again::

    >>> put(blob_url(small_digest), data=small_data)
    HTTP Error 409: Conflict

Uploading a file to an index with disabled blob support::

    >>> put(blob_url(small_digest, 'test_no_blobs'), data=small_data)
    HTTP Error 400: Bad Request

Get the files::

    >>> r = get(blob_url(big_digest))
    >>> len(r.content)
    614400

One of the nodes should redirect to the other::

    >>> r2 = node2.get(blob_url(big_digest))
    >>> bool(len(r2.redirects) > 0) != bool(len(r.redirects) > 0)
    True

    >>> r2.redirects and r2.redirects or r.redirects
    [(307, 'http://.../test/_blobs/f2356581794dac20797bff38ee2bdc4424d3f04a')]

    >>> sha1sum(r.content) == big_digest
    True

    >>> r = get(blob_url(small_digest))
    >>> sha1sum(r.content) == small_digest
    True

Use a head request to verify that a file exists without receiving the
content::

    >>> r = head(blob_url(small_digest))
    >>> r.headers
    Content-Length: 1500
    Accept-Ranges: bytes
    Expires: Thu, 31 Dec 2037 23:59:59 GMT
    Cache-Control: max-age=315360000

    >>> r2 = node2.head(blob_url(small_digest))
    >>> r2.headers
    Content-Length: 1500
    Accept-Ranges: bytes
    Expires: Thu, 31 Dec 2037 23:59:59 GMT
    Cache-Control: max-age=315360000

One of the head requests must be redirected::

    >>> bool(len(r2.redirects) > 0) != bool(len(r.redirects) > 0)
    True

    >>> r = head(blob_url(big_digest))
    >>> r.headers
    Content-Length: 614400
    Accept-Ranges: bytes
    Expires: Thu, 31 Dec 2037 23:59:59 GMT
    Cache-Control: max-age=315360000

.. note::

    The cache headers for blobs are static and basically allows
    clients to cache the response forever since the blob is immutable.q

File doesn't exist::

    >>> head(blob_url('0628aaf4c3dd704e95bdfea30e9c601862524350'))
    HTTP Error 404: Not Found

Delete existing files::

    >>> delete(blob_url(small_digest))
    HTTP Response 204

    >>> delete(blob_url(big_digest))
    HTTP Response 204

After deleting the files doesn't exist anymore::

    >>> head(blob_url(big_digest))
    HTTP Error 404: Not Found

    >>> head(blob_url(small_digest))
    HTTP Error 404: Not Found

Delete a non existing file causes a '404: Not Found'::

    >>> head(blob_url("any_non_existing_digest"))
    HTTP Error 404: Not Found


Requests that specify a byte-range will receive a partial reponse::

    >>> tiny_data = 'abcdefghijklmnopqrstuvwxyz'
    >>> tiny_digest = sha1sum(tiny_data)
    >>> r = put(blob_url(tiny_digest), data=tiny_data)
    >>> r.status_code
    201

    >>> headers = {'Range': 'bytes=8-'}
    >>> r = get(blob_url(tiny_digest), headers=headers)
    >>> r.headers
    Content-Length: 18
    Content-Range: bytes 8-25/26
    Accept-Ranges: bytes
    Expires: Thu, 31 Dec 2037 23:59:59 GMT
    Cache-Control: max-age=315360000

    >>> r.content
    'ijklmnopqrstuvwxyz'

    >>> headers = {'Range': 'bytes=0-1'}
    >>> r = get(blob_url(tiny_digest), headers=headers)
    >>> r.content
    'ab'

    >>> headers = {'Range': 'bytes=25-'}
    >>> r = get(blob_url(tiny_digest), headers=headers)
    >>> r.content
    'z'

A invalid range will result in 416 Requested Range not satisfiable::

    >>> headers = {'Range': 'bytes=40-58'}
    >>> r = get(blob_url(tiny_digest), headers=headers)
    >>> r.status_code
    416

Test that a file can be accessed in parallel::

    >>> import string
    >>> big_data = string.ascii_letters * 400
    >>> big_digest = sha1sum(big_data)
    >>> r = put(blob_url(big_digest), data=big_data)
    >>> r.status_code
    201

    >>> requests = [(blob_url(big_digest), {}, big_data)] * 40
    >>> mget(requests)
    True

Parallel access with range headers::

    >>> url = blob_url(big_digest)
    >>> requests = [
    ...     (url, {'Range': 'bytes=0-'}, big_data),
    ...     (url, {'Range': 'bytes=10-100'}, big_data[10:101]),
    ...     (url, {'Range': 'bytes=20-30'}, big_data[20:31]),
    ...     (url, {'Range': 'bytes=40-50'}, big_data[40:51]),
    ...     (url, {'Range': 'bytes=40-80'}, big_data[40:81]),
    ...     (url, {'Range': 'bytes=10-80'}, big_data[10:81]),
    ...     (url, {'Range': 'bytes=5-30'}, big_data[5:31]),
    ...     (url, {'Range': 'bytes=15-3000'}, big_data[15:3001]),
    ...     (url, {'Range': 'bytes=2000-10800'}, big_data[2000:10801]),
    ...     (url, {'Range': 'bytes=1500-20000'}, big_data[1500:20001]),
    ... ]
    >>> mget(requests)
    True

Upload some more files to test the blob size calculation further below::

    >>> small_data = 'a' * 1501
    >>> small_digest = sha1sum(small_data)
    >>> r = put(blob_url(small_digest), data=small_data)
    >>> print r
    HTTP Response 201

    >>> small_data = 'a' * 1502
    >>> small_digest = sha1sum(small_data)
    >>> r = put(blob_url(small_digest), data=small_data)
    >>> print r
    HTTP Response 201

    >>> small_data = 'a' * 1503
    >>> small_digest = sha1sum(small_data)
    >>> r = put(blob_url(small_digest), data=small_data)
    >>> print r
    HTTP Response 201

Statistics about the stored blobs like number of blobs or blob size can also be
retrieved using the `_status` endpoint::

    >>> url = '/test/_blobs/_status'
    >>> r = get(url)
    >>> print_json(r.content)
    {
        "_shards": {
            "failed": 0, 
            "successful": 2, 
            "total": 2
        },
        "indices": {
            "test": {
                "blobs": {
                    "count": 5, 
                    "primary_size": 25332, 
                    "size": 25332
                },
                "shards": {
                    "0": [
                        {
                            "blobs": {
                                "available_space": ..., 
                                "count": 1, 
                                "location": ".../indices/test/0/blobs",
                                "size": 1501
                            }, 
                            "routing": {
                                "index": "test", 
                                "node": "...",
                                "primary": true, 
                                "relocating_node": null, 
                                "shard": 0, 
                                "state": "STARTED"
                            }
                        }
                    ], 
                    "1": [
                        {
                            "blobs": {
                                "available_space": ..., 
                                "count": 4, 
                                "location": ".../indices/test/1/blobs",
                                "size": 23831
                            }, 
                            "routing": {
                                "index": "test", 
                                "node": "...",
                                "primary": true, 
                                "relocating_node": null, 
                                "shard": 1, 
                                "state": "STARTED"
                            }
                        }
                    ]
                }
            }
        }
    }

    >>> stats = json.loads(r.content)
    >>> blob_stats = stats['indices']['test']['shards']['0'][0]['blobs']
    >>> blob_stats['available_space'] > 0
    True

Requesting the stats of all blob enabled indices is also supported. Indices
that don't have blobs enabled are omitted::

    >>> url = '/_blobs/_status'
    >>> r = get(url)
    >>> print_json(r.content)
    {
        "_shards": {
            "failed": 0, 
            "successful": 4, 
            "total": 4
        }, 
        "indices": {
            "test": {
                ...
            },
            "test_blobs2": {
                ...
            }
        }
    }

Calling the endpoint on multiple indices is also supported::

    >>> url = '/test,test_blobs2/_blobs/_status'
    >>> r = get(url)
    >>> print_json(r.content)
    {
        "_shards": {
            "failed": 0, 
            "successful": 4, 
            "total": 4
        }, 
        "indices": {
            "test": {
                ...
            },
            "test_blobs2": {
                ...
            }
        }
    }

Direct access to an index that isn't blob enabled doesn't return anything::

    >>> url = '/test_no_blobs/_blobs/_status'
    >>> r = get(url)
    >>> print_json(r.content)
    {
        "_shards": {
            "failed": 0, 
            "successful": 0, 
            "total": 0
        }, 
        "indices": {}
    }


An empty file is handled just like any other file::

    >>> put('/test/_blobs/da39a3ee5e6b4b0d3255bfef95601890afd80709', data='')
    HTTP Response 201

    >>> put('/test/_blobs/da39a3ee5e6b4b0d3255bfef95601890afd80709', data='')
    HTTP Error 409: Conflict


Indexing a huge document on a non-blob index is still possible::

    >>> url = '/test_no_blobs/default/1'
    >>> data = {"content": "a" * (64 * 1024)}
    >>> put(url, data=json.dumps(data))
    HTTP Response 201
