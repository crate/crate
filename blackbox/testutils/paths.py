# -*- coding: utf-8; -*-
#
# Licensed to Crate.io GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.

from os.path import dirname, join
from pathlib import Path
from cr8.run_crate import get_crate
from subprocess import run
import multiprocessing

here = dirname(__file__)  # blackbox/testutils
project_root = dirname(dirname(here))


def project_path(*parts):
    return join(project_root, *parts)


def docs_path(*parts):
    return join(project_root, 'docs', *parts)


def crate_path():
    root = Path(project_root)
    app_build = root / "app" / "target"
    tarball = next(app_build.glob("crate-*.tar.gz"), None)
    if not tarball:
        cpus = multiprocessing.cpu_count()
        mvnw = root / "mvnw"
        run([str(mvnw), "-T", str(cpus), "package", "-DskipTests=true"], cwd=root)
        tarball = next(app_build.glob("crate-*.tar.gz"), None)
    uri = tarball.as_uri()
    return get_crate(uri)
