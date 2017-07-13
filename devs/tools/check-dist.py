#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" script to verify that the distribution doesn't contain duplicate classes """

import os
import zipfile
from argparse import ArgumentParser
from collections import defaultdict


def classes(lib_dir):
    """ walks the given directory and returns all java classes found.

    jar files are opened and the containing classes will also be returned.

    returns a generator that will return tuples of (filename, class) where
    filename is the file that contained the class.
    """

    for root, dirs, filenames in os.walk(lib_dir):
        for f in filenames:
            _, ext = os.path.splitext(f)
            fullname = os.path.abspath(os.path.join(root, f))
            if ext == '.class':
                yield (f, f)
            elif ext == '.jar':
                with zipfile.ZipFile(fullname, 'r') as z:
                    for name in z.namelist():
                        if os.path.splitext(name)[1] == '.class':
                            yield (f, name)


def filter_broken_deps(classes):
    broken = (
        # apache-log4j-extras contains duplicate classes
        # see https://bz.apache.org/bugzilla/show_bug.cgi?id=55289
        'org/apache/log4j',
        # ES hack/fork because of perf reasons
        # see https://github.com/elastic/elasticsearch/issues/12829
        # and https://github.com/elastic/elasticsearch/pull/11932
        'org/joda/time/base/BaseDateTime'
    )
    return ((f, name) for f, name in classes if not name.startswith(broken))


def main():
    p = ArgumentParser(
        'prints all classes that occur more than once in the given directory')
    p.add_argument('directory', type=str)
    args = p.parse_args()

    d = defaultdict(list)
    for filename, classname in filter_broken_deps(classes(args.directory)):
        d[classname].append(filename)

    num_duplicates = 0
    for k, v in d.items():
        if len(v) > 1:
            num_duplicates += 1
            print('Class {} is in: [{}]'.format(k, ', '.join(v)))
    if num_duplicates > 0:
        raise SystemExit(
            'Failed. Found {} classes that occur more than once'.format(num_duplicates))


if __name__ == '__main__':
    main()
