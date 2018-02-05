#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import urllib3
import logging
import zipfile
import tarfile
import argparse
import tempfile
import urllib.request
from crate.testing.layer import CrateLayer
from crate.client import connect

http = urllib3.PoolManager()

CRATE_HTTP_PORT = '42222'
CRATE_TRANSPORT_PORT = '43333'
BASE_URL = "https://cdn.crate.io/downloads/releases/crate-{0}.tar.gz"

logging.basicConfig(level=logging.ERROR)
LOGGER = logging.getLogger('bwc')


INDICES = {
    'legacy_ip': '''
        CREATE TABLE legacy_ip (
            fqdn STRING,
            addr IP
        ) CLUSTERED INTO 1 SHARDS WITH (
            number_of_replicas=0,
            "translog.flush_threshold_ops"=0
        );
        INSERT INTO legacy_ip (fqdn, addr) VALUES ('localhost', '127.0.0.1');
        REFRESH TABLE legacy_ip;''',
    'legacy_geo_point': '''
        CREATE TABLE legacy_geo_point (
            id int primary key,
            p geo_point
        ) CLUSTERED INTO 1 SHARDS WITH (
            number_of_replicas=0,
            "translog.flush_threshold_ops"=0,
            "gateway.local.sync"=0
        );
        INSERT INTO legacy_geo_point (id, p) VALUES (1, 'POINT (10 10)');
        REFRESH TABLE legacy_geo_point;''',
    'object_template_mapping': '''
        CREATE TABLE object_template_mapping (
            ts TIMESTAMP,
            attr OBJECT(dynamic) AS (
                temp FLOAT
            ),
            month STRING
        ) PARTITIONED BY (
            month
        ) CLUSTERED INTO 1 SHARDS WITH (
            number_of_replicas=0,
            "translog.flush_threshold_ops"=0,
            "gateway.local.sync"=0
        );
        INSERT INTO object_template_mapping (ts, attr, month) VALUES (1480530180000, {temp=21.1}, '201611');
        REFRESH TABLE object_template_mapping;''',
    'legacy_string': '''
        -- we just need any table created with < 1.2, we want to test adding a string col to old tables
        CREATE TABLE legacy_string (
            s string
        );'''
}


def download_crate(url):
    LOGGER.info("Downloading CrateDB")
    try:
        fname, headers = urllib.request.urlretrieve(url)
    except urllib.error.HTTPError:
        LOGGER.error("Could not download CrateDB from %s", url)
        raise
    with tarfile.open(fname, "r:gz") as tar:
        tempdir = tempfile.mkdtemp()
        home_dir = tar.firstmember.path
        tar.extractall(tempdir)
        return os.path.join(tempdir, home_dir)


def compress_index(name, version, data_dir, output_dir):
    compress(data_dir, output_dir, 'bwc-{}-{}.zip'.format(name, version))


def compress(data_dir, output_dir, target):
    abs_output_dir = os.path.abspath(output_dir)
    target = os.path.join(abs_output_dir, target)
    if os.path.exists(target):
        os.remove(target)
    with zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipdir(data_dir, zipf, os.path.dirname(data_dir))


def zipdir(path, ziph, basePath):
    for root, dirs, files in os.walk(path):
        for file in files:
            filePath = os.path.join(root, file)
            inZipPath = filePath.replace(basePath, "", 1).lstrip("\\/")
            ziph.write(filePath, inZipPath)


def create_index(index_name, crate_home, output_dir):
    crate_layer = CrateLayer(
        'data',
        crate_home=crate_home,
        port=CRATE_HTTP_PORT,
        transport_port=CRATE_TRANSPORT_PORT,
        settings={
            'es.api.enabled': True,
            # The disk.watermark settings can be removed once crate-python > 0.21.1 has been released
            "cluster.routing.allocation.disk.watermark.low" : "100k",
            "cluster.routing.allocation.disk.watermark.high" : "10k",
            "cluster.routing.allocation.disk.watermark.flood_stage" : "1k",
        }
    )
    crate_layer.start()
    crate_http = 'localhost:{}'.format(CRATE_HTTP_PORT)
    try:
        with connect(crate_http) as conn:
            cur = conn.cursor()
            cmds = INDICES[index_name].split(';')
            for cmd in cmds[:-1]:
                LOGGER.info(cmd)
                cur.execute(cmd)
            cur.execute("select version['number'] from sys.nodes")
            version = cur.fetchone()[0]
            r = http.request('POST', crate_http + '/_flush')
            r.read()
            compress_index(index_name, version, crate_layer.wdPath(), output_dir)
    finally:
        crate_layer.stop()


def parse_config():
    parser = argparse.ArgumentParser(description='Builds a crate table for backwards compatibility tests')
    parser.add_argument('--index-name', '-n', required=True, choices=INDICES.keys(),
                        help='The name of the index that should be created')
    parser.add_argument('--output-dir', '-o', default='../sql/src/test/resources/indices/bwc',
                        help='The directory to write the zipped index into')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--crate-home', '-d', metavar='DIR',
                        help='The crate home directory')
    group.add_argument('--crate-version', '-v',
                        help='Download a specific crate version to create the legacy index')
    cfg = parser.parse_args()
    return cfg


def main():
    LOGGER.setLevel(logging.INFO)
    cfg = parse_config()
    crate_home = cfg.crate_home
    if crate_home is None:
        url = BASE_URL.format(cfg.crate_version)
        crate_home = download_crate(url)
    create_index(cfg.index_name, crate_home, cfg.output_dir)


if __name__ == '__main__':
    main()
