SHELL = /bin/sh

html: pyenv out
	./pyenv/bin/sphinx-build -b html -E . out/html

out:
	mkdir out

pyenv:
	$(MAKE) -C pyenv

clean:
	rm -rf out

distclean: clean
	$(MAKE) -C pyenv distclean

.PHONY: build pyenv clean

