#!/bin/bash

# travis and jenkins have python3.4 installed
if hash python3.4 2> /dev/null; then
    python3.4 -m venv .venv --without-pip
    curl -s https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    .venv/bin/python get-pip.py
elif hash python3 2> /dev/null; then
    # fallback for dev machines, this should be >= 3.3
    python3 -m venv .venv
else
    echo 'python3 required'
    exit 1
fi

.venv/bin/python -m pip install -U pip setuptools
.venv/bin/python -m pip install -r requirements.txt
