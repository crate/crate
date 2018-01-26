#!/bin/bash

if hash python3.6 2> /dev/null; then
    python3.6 -m venv .venv
elif hash python3 2> /dev/null; then
    # fallback to python3 in case there is no python3.6 alias; should be 3.6
    python3 -m venv .venv
else
    echo 'python3.6 required'
    exit 1
fi

.venv/bin/pip install -U pip setuptools wheel
.venv/bin/pip install -r requirements.txt
