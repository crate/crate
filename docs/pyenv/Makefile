SHELL = /bin/sh
# use python2 if it exists, otherwise assume that python is a python2
# sphinx doesn't work with python3 yet.
# see http://www.python.org/dev/peps/pep-0394/
PYTHON := $(shell (which python2 &> /dev/null && which python2 || which python))

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
