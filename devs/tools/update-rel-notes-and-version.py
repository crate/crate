#!/usr/bin/env python3

# Licensed to Crate.io GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.

""" script to prepare the "Release <version>" commit of a CrateDB release

Given a version like 6.4.1 it creates a ``release-6.4.1`` branch off
``origin/6.4``, containing a single "Release 6.4.1" commit which:

- finalizes ``docs/appendices/release-notes/6.4.1.rst``: removes the
  " - Unreleased" title suffix, the ".. comment" instructions and the "in
  development" note, and adds a "Released on <today>." line instead

- clears the snapshot flag of the ``V_6_4_1`` constant in
  ``server/src/main/java/org/elasticsearch/Version.java``

The branch is pushed to origin and the pull request is opened.

Usage::

    ./devs/tools/update-rel-notes-and-version.py 6.4.1
"""

import re
import datetime
import sys

from release_helpers import (NOTES_DIR, VERSION_JAVA, commit_and_push, create_branch,
                            fetch_and_check, open_pull_request, repo_root,
                            version_arg)


def patch_release_notes(text, version, released_on):
    """Turn the "Unreleased" release notes of ``version`` into released notes"""
    title = f"Version {version}"
    unreleased_title = re.compile(
        r"^=+\n" + re.escape(f"{title} - Unreleased") + r"\n=+\n",
        re.MULTILINE,
    )
    # The ".. comment" lines describe the very steps performed here, the note
    # below them states that the version isn't released yet. Both go away.
    in_development = re.compile(
        r"^(?:\.\. comment.*\n)+"
        r"\n*"
        r"\.\. NOTE::\n"
        r"\n"
        r"(?:[ \t]+\S.*\n)+",
        re.MULTILINE,
    )

    if unreleased_title.search(text) is None:
        if re.search(r"^Released on ", text, re.MULTILINE):
            raise ValueError(f"the {version} release notes are already released")
        raise ValueError(f"no '{title} - Unreleased' title in the {version} release notes")

    marker = "=" * len(title)
    text = unreleased_title.sub(f"{marker}\n{title}\n{marker}\n", text, count=1)

    match = in_development.search(text)
    if match is None or "isn't released yet" not in match.group(0):
        raise ValueError(f"no 'in development' note in the {version} release notes")
    return f"{text[:match.start()]}Released on {released_on}.\n{text[match.end():]}"


def patch_version_java(text, version):
    """Clear the snapshot flag of the ``V_<version>`` constant"""
    constant = "V_" + version.replace(".", "_")
    declaration = re.compile(
        r"^(\s*public static final Version "
        + re.escape(constant)
        + r" = new Version\([^,]+, )(true|false)(,)",
        re.MULTILINE,
    )
    match = declaration.search(text)
    if match is None:
        raise ValueError(f"no {constant} constant in {VERSION_JAVA}")
    if match.group(2) == "false":
        raise ValueError(f"{constant} in {VERSION_JAVA} is already not a snapshot")

    current = re.search(
        r"^\s*public static final Version CURRENT = (\S+);", text, re.MULTILINE)
    if current is not None and current.group(1) != constant:
        print(f"warning: CURRENT is {current.group(1)}, not {constant}", file=sys.stderr)

    return f"{text[:match.start()]}{match.group(1)}false{match.group(3)}{text[match.end():]}"


def main():
    version = version_arg(__doc__)
    base = ".".join(version.split(".")[:2])
    branch = f"release-{version}"
    released_on = datetime.datetime.now(tz=datetime.UTC).date()

    root = repo_root(__file__)
    fetch_and_check(root, base, branch)
    previous_branch = create_branch(root, branch, base)

    paths = (f"{NOTES_DIR}/{version}.rst", VERSION_JAVA)
    patches = (lambda text: patch_release_notes(text, version, released_on),
               lambda text: patch_version_java(text, version))
    for path, patch in zip(paths, patches):
        file = root / path
        if not file.is_file():
            sys.exit(f"{path} does not exist on origin/{base}")
        try:
            file.write_text(patch(file.read_text()))
        except ValueError as e:
            sys.exit(str(e))
        print(f"Updated {path}")

    commit_and_push(root, branch, f"Release {version}")
    open_pull_request(root, base, branch, f"Release {version}")


if __name__ == "__main__":
    main()
