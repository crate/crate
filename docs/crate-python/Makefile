SHELL = /bin/sh
export PYTHONPATH := $(PYTHONPATH):$(shell pwd)/crate-python/tmp/src

VERSION = 0.0.2

build: prepare

build: prepare

download:
	curl --retry 3 -sSO -n https://download.lovelysystems.com/eggs/lovely/crate-$(VERSION).tar.gz

prepare: download
	tar -zxv -f crate-$(VERSION).tar.gz
	rm -rf tmp && mv crate-$(VERSION) tmp
	cp tmp/src/crate/client/index.txt .

clean:
	rm -rf tmp crate* index.txt

.PHONY: clean
