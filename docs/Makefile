SHELL = /bin/sh

VERSION := $(shell ls -1 ../app/build/distributions/crate*.tar.gz|sort|tail -n1 |grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+')

export PYTHONPATH := $(PYTHONPATH):$(shell pwd)/crate-python/tmp/src

build: out/html out/text

out/html: pyenv crate-python out
	./pyenv/bin/sphinx-build -b html -E . out/html

out/text: pyenv crate-python out
	./pyenv/bin/sphinx-build -b text -E . out/text

out tmp:
	mkdir $@

pyenv:
	$(MAKE) -C pyenv

clean:
	rm -rf out tmp

distclean: clean
	$(MAKE) -C pyenv distclean
	$(MAKE) -C crate-python distclean

tmp/crate: ../app/build/distributions/crate-$(VERSION).tar.gz tmp
	tar -C tmp -zxv -f ../app/build/distributions/crate-$(VERSION).tar.gz
	mv tmp/crate-$(VERSION) tmp/crate

test: pyenv tmp/crate
	./pyenv/bin/zope-testrunner -v --auto-color --path=src


crate-python: pyenv
	$(MAKE) -C crate-python


.PHONY: build pyenv clean test crate-python
