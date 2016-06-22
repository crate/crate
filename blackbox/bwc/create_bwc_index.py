#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import zipfile
from crate.testing.layer import CrateLayer
from crate.client import connect


CRATE_HTTP_PORT = '42222'
CRATE_TRANSPORT_PORT = '43333'


CREATE_INDEX_SQL = """
    CREATE TABLE legacy_geo_point (
        id int primary key,
        p geo_point
    ) CLUSTERED INTO 1 SHARDS WITH (
        number_of_replicas=0,
        "translog.flush_threshold_ops"=0,
        "gateway.local.sync"=0
    );
    INSERT INTO legacy_geo_point (id, p) VALUES (1, 'POINT (10 10)');
    REFRESH TABLE legacy_geo_point;
"""


def compress_index(version, data_dir, output_dir):
    compress(data_dir, output_dir, 'bwc-index-%s.zip' % version)


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


def create_index(cfg):
    crate_layer = CrateLayer(
        'data',
        crate_home=cfg.crate_home,
        port=CRATE_HTTP_PORT,
        transport_port=CRATE_TRANSPORT_PORT
    )
    crate_layer.start()
    try:
        with connect('localhost:' + CRATE_HTTP_PORT) as conn:
            cur = conn.cursor()
            cmds = CREATE_INDEX_SQL.split(';')
            for cmd in cmds[:-1]:
                cur.execute(cmd)
            cur.execute("select version['number'] from sys.nodes")
            version = cur.fetchone()[0]
            compress_index(version, crate_layer.wdPath(), cfg.output_dir)
    finally:
        crate_layer.stop()


def parse_config():
    parser = argparse.ArgumentParser(description='Builds a crate table for backwards compatibility tests')
    parser.add_argument('--crate-home', '-d', default='backwards', metavar='DIR',
                        help='The crate home directory')
    parser.add_argument('--output-dir', '-o', default='../sql/src/test/resources/indices/bwc',
                        help='The directory to write the zipped index into')
    cfg = parser.parse_args()
    return cfg


def main():
    cfg = parse_config()
    create_index(cfg)


if __name__ == '__main__':
    main()
