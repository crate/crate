#!/usr/bin/env python3

"""
Use to apply patches from ES upstream with:

    git apply --reject \
        <(curl -L https://github.com/elastic/elasticsearch/pull/<NUMBER>.patch | ./devs/tools/adapt-es-path.py)
"""

import sys


def main():
    for line in sys.stdin:
        print(
            line
            .rstrip()
            .replace('--- a/server/', '--- a/es/es-server/')
            .replace('+++ b/server/', '+++ b/es/es-server/')
        )


if __name__ == "__main__":
    main()
