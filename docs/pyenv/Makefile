SHELL = /bin/sh
# find a python2.7 version, first look in /opt/python-2.7, then check for python2 in $PATH and if that fails use python in $PATH
PYTHON := $(shell (test -f /opt/python-2.7/bin/python && echo /opt/python-2.7/bin/python || (which python2 &> /dev/null && which python2 || which python)))

bin/sphinx-build: bin/python
	./bin/pip install -r requirements.txt

virtualenv.py:
	curl --retry 3 -sSO https://raw.github.com/pypa/virtualenv/1.9.1/virtualenv.py

bin/python: virtualenv.py
	$(PYTHON) virtualenv.py .

clean:
	rm -rf bin include lib *.pyc

distclean: clean
	rm -rf virtualenv.py*

.PHONY: clean
