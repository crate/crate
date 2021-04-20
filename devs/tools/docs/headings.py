#!/usr/bin/python3

r"""
Summarize a document's heading structure.

Synopsis:

    $ ./headings.py FILENAME [ ROOT ] [ STRIP ]

Scan a file (`FILENAME`) for headings marked up using the heading structure
markup documented in the Crate.io RST style guide. For RST style guide details,
see <https://github.com/crate/crate-docs/blob/main/style/rst.rst>.

NOTE: The RST markup style is sometimes used in non-RST files (e.g.,
`crate.yml`). This script can be used on those files too.

This program discards the root heading (i.e., document title) and summarizes
the remaining heading structure (printed to STDOUT) with heading levels
indicated by visual indentation.

If you specify a value for `ROOT`, the first subheading that matches that value
will be taken as the root heading and the program will only summarize the
direct descendants of that subheading.

If you specify a value for `STRIP`, the program will strip that value from the
left-hand side of every line. You can use this argument to process headings
that are commented out (e.g., set `STRIP` to `#`).

TIP: If two different files contain a heading structure that needs to be kept
in sync (e.g., config files that correspond one-to-one with sections in the
documentation), you can use this tool to help you with that.

For example:

    $ ./headings.py \
          ../../../docs/config/node.rst \
          'Node-specific settings' > node-settings-rst.txt

    $ ./headings.py \
          ../../../app/src/main/dist/config/crate.yml \
          'Node-specific settings' > node-settings-yml.txt


    $ diff node-settings-rst.txt node-settings-yml.txt
"""

import re
import sys

OUT_INDENT_PER_LEVEL = "  "


def exit_help():
    """Print help message and exit."""
    print(__doc__.strip())
    sys.exit(0)


try:
    target_filename = sys.argv[1]
except IndexError:
    exit_help()

try:
    root_heading = sys.argv[2]
    root_re = re.compile("^ *{} *$".format(re.escape(root_heading)))
except IndexError:
    root_heading = None

try:
    left_strip = sys.argv[3]
    left_strip_re = re.compile("^ *{} *".format(re.escape(left_strip)))
except IndexError:
    left_strip = None

# NOTE: We could be strict with spaces when constructing regular expressions,
# but they can be useful for debugging purposes (allowing you to indent
# headings for better visual distinction, and so on)

# Heading underscores with corresponding heading level
# Cf. <https://github.com/crate/crate-docs/blob/main/style/rst.rst>
heading_levels = {
    "=": 1,
    "-": 2,
    "'": 3,
    ".": 4,
    "`": 5,
}

heading_levels_re = {}
for symbol, level in heading_levels.items():
    symbol_re = re.compile("^ *({}+) *$".format(re.escape(symbol)))
    heading_levels_re[symbol_re] = heading_levels[symbol]

# Read lines and handle `STRIP` functionality
source_lines = []
with open(target_filename, "r") as target_file:
    for line_str in target_file.readlines():
        if left_strip is not None:
            try:
                line_str = left_strip_re.split(line_str)[1]
            except IndexError:
                continue
        source_lines.append(line_str.strip())

# Make a list of headings and their corresponding heading level
heading_re = re.compile("[a-zA-Z0-9]+")
headings = []
for i, current_line in enumerate(source_lines):
    if not heading_re.match(current_line):
        continue
    try:
        next_line = source_lines[i + 1]
    except IndexError:
        continue
    # Try to match next line against heading level patterns
    for symbol_re, level in heading_levels_re.items():
        re_match = symbol_re.fullmatch(next_line)
        if re_match:
            headings.append([level, current_line.strip()])
            continue

# Always discard the true root heading (i.e., document title)
headings = headings[1:]

# Handle `ROOT` functionality
root_level = 0
if root_heading is not None:
    i_first = None
    i_last = None
    # Scan the list of headings for the heading we will consider the root of
    # the heading tree
    for i, item in enumerate(headings):
        level, heading_text = item
        i_last = i + 1
        if level <= root_level:
            break
        if root_re.match(heading_text):
            i_first = i + 1
            root_level = level
    if root_level == 0:
        print("ERROR: No subheading matching `ROOT` value")
        sys.exit(1)
    headings = headings[i_first:i_last]

# Summarize the heading structure
heading_tree_str = ""
for level, heading_text in headings:
    # Adjust level to root heading baseline
    level = level - root_level
    # Indent headings for every level below the first level
    indent = OUT_INDENT_PER_LEVEL * (level - 1)
    print("{}{}".format(indent, heading_text))
