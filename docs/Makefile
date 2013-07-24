SHELL = /bin/sh

VERSION := $(shell ls -1 ../releases/crate*.tar.gz|sort|tail -n1 |grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+')

build: out/html out/text

out/html: pyenv out
	./pyenv/bin/sphinx-build -b html -E . out/html

out/text: pyenv out
	./pyenv/bin/sphinx-build -b text -E . out/text

out tmp:
	mkdir $@

pyenv:
	$(MAKE) -C pyenv

clean:
	rm -rf out tmp

distclean: clean
	$(MAKE) -C pyenv distclean

tmp/crate: ../releases/crate-$(VERSION).tar.gz tmp
	tar -C tmp -zxv -f ../releases/crate-$(VERSION).tar.gz
	mv tmp/crate-$(VERSION) tmp/crate

test: pyenv tmp/crate
	./pyenv/bin/zope-testrunner -v --auto-color --path=src

.PHONY: build pyenv clean test
