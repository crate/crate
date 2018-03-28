#!/bin/bash

DIR=$(dirname "$0")

if hash python3.6 2> /dev/null; then
    PYTHON=python3.6 
elif hash python3 2> /dev/null; then
    # fallback to python3 in case there is no python3.6 alias; should be 3.6
    PYTHON=python3
else
    echo 'python3.6 required'
    exit 1
fi

$PYTHON -m venv $DIR/.venv
if [ ! -f $DIR/.venv/bin/pip ]; then
    wget https://bootstrap.pypa.io/get-pip.py
    $DIR/.venv/bin/python get-pip.py
    rm -f get-pip.py
fi

$DIR/.venv/bin/pip install -U pip setuptools wheel
$DIR/.venv/bin/pip install -r $DIR/requirements.txt
