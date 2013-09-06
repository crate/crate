SHELL = /bin/sh

VERSION = 0.0.2

build: prepare
	cp tmp/src/crate/client/index.txt .
	../pyenv/bin/sphinx-apidoc -s txt -o . tmp/src

download:
	curl --retry 3 -sSO -n https://download.lovelysystems.com/eggs/lovely/crate-$(VERSION).tar.gz

prepare: download
	tar -zxv -f crate-$(VERSION).tar.gz
	rm -rf tmp && mv crate-$(VERSION) tmp

clean:
	rm -rf tmp crate* *.txt

distclean: clean

.PHONY: clean
