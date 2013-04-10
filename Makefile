all: prepare

prepare: lib
	mvn prepare-package

lib:
	mkdir lib

clean:
	rm -rf lib

.PHONY: prepare clean

