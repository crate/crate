SHELL = /bin/sh

bin/sphinx-build: bin/python
	./bin/pip install -r requirements.txt

virtualenv.py:
	curl --retry 3 -sSO https://raw.github.com/pypa/virtualenv/1.9.1/virtualenv.py

bin/python: virtualenv.py
	python virtualenv.py .

clean:
	rm -rf bin include lib *.pyc

distclean: clean
	rm -rf virtualenv.py*

.PHONY: clean
