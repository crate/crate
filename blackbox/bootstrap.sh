#!/bin/bash

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


DIR=$(dirname "$0")

if hash python3.9 2> /dev/null; then
    PYTHON=python3.9

# Fall back to `python3` in case there is no `python3.9` alias.
elif hash python3 2> /dev/null; then
    PYTHON=python3

else
    echo 'python3.7+ required'
    exit 1
fi

$PYTHON -m venv "$DIR/.venv"
if [ ! -f "$DIR/.venv/bin/pip" ]; then
    wget https://bootstrap.pypa.io/get-pip.py
    "$DIR/.venv/bin/python" get-pip.py
    rm -f get-pip.py
fi

"$DIR/.venv/bin/pip" install --upgrade pip setuptools wheel
"$DIR/.venv/bin/pip" install --upgrade --requirement "$DIR/requirements.txt"
