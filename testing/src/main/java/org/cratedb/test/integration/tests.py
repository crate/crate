import doctest
import unittest
import sys
import os
import urllib2
import socket
import time
import json
from hashlib import sha1
from threading import Thread
from functools import partial
import logging

log = logging.getLogger('pytest')


def print_json(content):
    print(json.dumps(json.loads(content), indent=4, sort_keys=True))


def blob_url(digest, index="test"):
    return '/{}/_blobs/{}'.format(index, digest)


def sha1sum(content):
    return sha1(content).hexdigest()


class MemorizeHTTPRedirectHandler(urllib2.HTTPRedirectHandler):

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not hasattr(req, 'redirects'):
            req.redirects = []
        req.redirects.append((code, newurl))
        return urllib2.HTTPRedirectHandler.redirect_request(
            self, req, fp, code, msg, headers, newurl
        )


class Response(object):
    def __init__(self, req, headers=None):
        opener = urllib2.build_opener(MemorizeHTTPRedirectHandler())
        self.error = None
        try:
            result = opener.open(req)
            self.content = result.read()
            self._headers = result.headers
            self.headers_dict = result.headers.dict
            self.status_code = result.code
            if hasattr(req, 'redirects'):
                self.redirects = req.redirects
            else:
                self.redirects = []
        except urllib2.HTTPError as e:
            self.error = str(e)
            self.msg = e.msg
            self._headers = e.headers
            self.headers_dict = e.headers.dict
            self.status_code = e.code
            try:
                self.content = e.read()
            except socket.error:
                pass
        except urllib2.URLError as e:
            print("failed url: " + req.get_full_url())
            raise

    @property
    def headers(self):
        print(self._headers)

    def __repr__(self):
        return str(self)

    def __str__(self):
        if self.error and self.status_code >= 400:
            return 'HTTP Error {}: {}'.format(self.status_code, self.msg)
        return 'HTTP Response {}'.format(self.status_code)


class Endpoint(object):
    def __init__(self, base='http://localhost:9200'):
        self.base = base

    def _req(self, path, headers=None):
        url = self.base + path
        headers = headers or {}
        return urllib2.Request(url, headers=headers)

    def _exec_req(self, req):
        return Response(req)

    def get(self, path, headers=None):
        req = self._req(path, headers)
        return self._exec_req(req)

    def pget(self, *args):
        resp = self.get(*args)
        print_json(resp.content)

    def ppost(self, *args):
        resp = self.post(*args)
        print_json(resp.content)

    def post(self, path, data=''):
        req = self._req(path)
        if isinstance(data, dict):
            data = json.dumps(data)
        req.add_data(data)
        return self._exec_req(req)

    def pput(self, *args):
        resp = self.put(*args)
        print_json(resp.content)

    def put(self, path, data=''):
        req = self._req(path)
        if isinstance(data, dict):
            data = json.dumps(data)
        req.add_data(data)
        req.get_method = lambda: 'PUT'
        return self._exec_req(req)

    def head(self, path):
        req = self._req(path)
        req.get_method = lambda: 'HEAD'
        return self._exec_req(req)

    def delete(self, path):
        req = self._req(path)
        req.get_method = lambda: 'DELETE'
        return self._exec_req(req)

    def refresh(self):
        self.post("/_flush")
        self.post("/_refresh")


def mget(endpoint, requests):
    results = [None] * len(requests)
    threads = [None] * len(requests)

    def get(path, headers, expected, index):
        r = endpoint.get(path, headers)
        results[index] = (r.content == expected)

    for i, request in enumerate(requests):
        threads[i] = Thread(target=get,
                            args=(request[0], request[1], request[2], i))
        threads[i].start()
    while any((t.is_alive() for t in threads)):
        time.sleep(0.1)
    return all(results)


def setUp(test):
    ep = Endpoint()
    test.globs['Endpoint'] = Endpoint
    test.globs['get'] = ep.get
    test.globs['head'] = ep.head
    test.globs['post'] = ep.post
    test.globs['put'] = ep.put
    test.globs['refresh'] = ep.refresh
    test.globs['delete'] = ep.delete
    test.globs['sha1sum'] = sha1sum
    test.globs['remove'] = os.remove
    test.globs['blob_url'] = blob_url
    test.globs['mget'] = partial(mget, ep)
    test.globs['json'] = json
    test.globs['print_json'] = print_json
    ep2 = Endpoint('http://localhost:9201')
    test.globs['node2'] = ep2


def test_suite(fname):
    log.info("testing file: %s", fname)
    s = unittest.TestSuite((
        doctest.DocFileSuite(
            fname, setUp=setUp,
            optionflags=doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS,
            module_relative=False)
    ))
    return s


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner(failfast=True)
    res = runner.run(test_suite(sys.argv[1]))
    if res.failures or res.errors:
        sys.exit(1)
