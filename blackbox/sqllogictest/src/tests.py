#!/usr/bin/env python3

import os
import re
import faulthandler
import logging
import pathlib
from concurrent.futures import ProcessPoolExecutor, as_completed
from testutils.ports import GLOBAL_PORT_POOL
from testutils.paths import crate_path, project_root
from crate.testing.layer import CrateLayer
from sqllogictest import run_file

CRATE_HTTP_PORT = GLOBAL_PORT_POOL.get()
CRATE_TRANSPORT_PORT = GLOBAL_PORT_POOL.get()
CRATE_PSQL_PORT = GLOBAL_PORT_POOL.get()

tests_path = pathlib.Path(os.path.abspath(os.path.join(
    project_root, 'blackbox', 'sqllogictest', 'testfiles', 'test')))

# Enable to be able to dump threads in case something gets stuck
faulthandler.enable()

# might want to change this to a blacklist at some point
FILE_WHITELIST = [re.compile(o) for o in [
    'select[1-5].test',
    'random/select/slt_good_\d+.test',
    'random/groupby/slt_good_\d+.test',
    'evidence/slt_lang_createview\.test',
    'evidence/slt_lang_dropview\.test'
]]

log = logging.getLogger('crate.testing.layer')
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
log.addHandler(ch)


def merge_logfiles(logfiles):
    with open('sqllogic.log', 'w') as fw:
        for logfile in logfiles:
            with open(logfile, 'r') as fr:
                content = fr.read()
                if content:
                    fw.write(logfile + '\n')
                    fw.write(content)
            os.remove(logfile)


def main():
    crate_layer = CrateLayer(
        'crate-sqllogic',
        crate_home=crate_path(),
        port=CRATE_HTTP_PORT,
        transport_port=CRATE_TRANSPORT_PORT,
        settings={
            'psql.port': CRATE_PSQL_PORT,
            # The disk.watermark settings can be removed once crate-python > 0.21.1 has been released
            "cluster.routing.allocation.disk.watermark.low": "100k",
            "cluster.routing.allocation.disk.watermark.high": "10k",
            "cluster.routing.allocation.disk.watermark.flood_stage": "1k",
        }
    )
    crate_layer.start()
    logfiles = []
    try:
        with ProcessPoolExecutor() as executor:
            futures = []
            for i, filename in enumerate(tests_path.glob('**/*.test')):
                filepath = tests_path / filename
                relpath = str(filepath.relative_to(tests_path))
                if not any(p.match(str(relpath)) for p in FILE_WHITELIST):
                    continue

                logfile = f'sqllogic-{os.path.basename(relpath)}-{i}.log'
                logfiles.append(logfile)
                future = executor.submit(
                    run_file,
                    filename=str(filepath),
                    host='localhost',
                    port=str(CRATE_PSQL_PORT),
                    log_level=logging.WARNING,
                    log_file=logfile,
                    failfast=True,
                    schema=f'x{i}'
                )
                futures.append(future)
            for future in as_completed(futures):
                future.result()
    finally:
        crate_layer.stop()
        # instead of having dozens file merge to one which is in gitignore
        merge_logfiles(logfiles)


if __name__ == "__main__":
    main()
