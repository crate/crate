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

case $OSTYPE in
darwin*)
    $PYTHON -m venv .venv --without-pip
    wget https://bootstrap.pypa.io/get-pip.py
    ./.venv/bin/python get-pip.py
    rm -f get-pip.py
    ;;
*)
    $PYTHON -m venv .venv
    ;;
esac
.venv/bin/pip install -U pip setuptools wheel
.venv/bin/pip install -r requirements.txt
