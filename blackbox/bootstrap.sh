#!/bin/bash

if hash python3.6 2> /dev/null; then
    PYTHON=python3.6 
elif hash python3 2> /dev/null; then
    # fallback to python3 in case there is no python3.6 alias; should be 3.6
    PYTHON=python3
else
    echo 'python3.6 required'
    exit 1
fi

$PYTHON -m venv .venv
if [ ! -f .venv/bin/pip ]; then
    wget https://bootstrap.pypa.io/get-pip.py
    ./.venv/bin/python get-pip.py
    rm -f get-pip.py
fi

.venv/bin/pip install -U pip setuptools wheel
.venv/bin/pip install -r requirements.txt
