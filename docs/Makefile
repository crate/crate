SHELL = /bin/sh

VERSION := $(shell ls -1 ../app/build/distributions/crate*.tgz|sort|tail -n1 |grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+')

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

tmp/crate: ../app/build/distributions/crate-$(VERSION).tgz tmp
	tar -C tmp -zxv -f ../app/build/distributions/crate-$(VERSION).tgz
	mv tmp/crate-$(VERSION) tmp/crate

test: pyenv tmp/crate
	./pyenv/bin/zope-testrunner -v --auto-color --path=src

.PHONY: build pyenv clean test
