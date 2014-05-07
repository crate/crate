SHELL = /bin/sh

ifeq ($(UNAME), Darwin)
PYTHON = /opt/local/bin/python2.7
else
PYTHON = /usr/local/bin/python2.7
endif

BUILDOUT_VERSION=$(shell grep zc.buildout versions.cfg |cut -d '=' -f2)

all: buildout sphinx

buildout: bin/buildout
	./bin/buildout -N

bin/buildout:
	$(PYTHON) bootstrap.py --version=$(BUILDOUT_VERSION)

sphinx: buildout
	./bin/sphinx

clean:
	rm -rf out/

.PHONY: clean
