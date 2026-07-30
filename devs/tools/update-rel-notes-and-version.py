#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

Given a version like 5.4.1 it creates a ``release-5.4.1`` branch off
``origin/5.4``, containing a single "Release 5.4.1" commit which:

- finalizes ``docs/appendices/release-notes/5.4.1.rst``: removes the
  " - Unreleased" title suffix, the ".. comment" instructions and the "in
  development" note, and adds a "Released on <today>." line instead

- clears the snapshot flag of the ``V_5_4_1`` constant in
  ``server/src/main/java/org/elasticsearch/Version.java``

The branch is pushed to origin and a link to open the pull request is printed.

Usage::

    ./devs/tools/update-rel-notes-and-version.py 5.4.1
"""

import re
import subprocess
import sys
from argparse import ArgumentParser
from datetime import date
from pathlib import Path
from urllib.parse import quote

VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")
NOTES_DIR = "docs/appendices/release-notes"
VERSION_JAVA = "server/src/main/java/org/elasticsearch/Version.java"


def fail(message):
    sys.stdout.flush()  # keep the message in order with the progress output
    print(f"error: {message}", file=sys.stderr)
    sys.exit(1)


def warn(message):
    sys.stdout.flush()
    print(f"warning: {message}", file=sys.stderr)


def run(*args, cwd, quiet=True):
    result = subprocess.run(
        args,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE if quiet else None,
        stderr=subprocess.PIPE if quiet else None,
    )
    if result.returncode != 0:
        details = (result.stderr or result.stdout or "").strip()
        fail(f"`{' '.join(args)}` failed" + (f": {details}" if details else ""))
    return (result.stdout or "").strip()


def ref_exists(root, ref):
    return subprocess.run(
        ("git", "rev-parse", "--verify", "--quiet", ref),
        cwd=root,
        stdout=subprocess.DEVNULL,
    ).returncode == 0


def discard_branch(root, branch, previous):
    """Delete ``branch`` and restore the checkout, to be used on failure"""
    run("git", "checkout", "--force", previous, cwd=root)
    run("git", "branch", "--delete", "--force", branch, cwd=root)


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
        warn(f"CURRENT is {current.group(1)}, not {constant}")

    return f"{text[:match.start()]}{match.group(1)}false{match.group(3)}{text[match.end():]}"


def parse_args():
    parser = ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("version", help="version to release, e.g. 5.4.1")
    args = parser.parse_args()
    if VERSION_RE.match(args.version) is None:
        fail(f"invalid version '{args.version}', expected <major>.<minor>.<patch>")
    return args


def main():
    version = parse_args().version
    base = ".".join(version.split(".")[:2])
    branch = f"release-{version}"
    released_on = date.today().isoformat()

    root = Path(run("git", "rev-parse", "--show-toplevel", cwd=Path(__file__).resolve().parent))

    if run("git", "status", "--porcelain", cwd=root):
        fail("working directory not clean, commit or stash your changes first")

    print("Fetching origin...")
    run("git", "fetch", "origin", cwd=root)
    if not ref_exists(root, f"refs/remotes/origin/{base}"):
        fail(f"origin/{base} does not exist, is {version} released from a {base} branch?")
    for ref in (f"refs/heads/{branch}", f"refs/remotes/origin/{branch}"):
        if ref_exists(root, ref):
            fail(f"{ref} already exists, delete it or finish that release first")

    print(f"Creating branch {branch} from origin/{base}...")
    previous_branch = run("git", "rev-parse", "--abbrev-ref", "HEAD", cwd=root)
    if previous_branch == "HEAD":  # detached, remember the commit instead
        previous_branch = run("git", "rev-parse", "HEAD", cwd=root)
    run("git", "checkout", "-b", branch, f"origin/{base}", cwd=root, quiet=False)

    paths = (f"{NOTES_DIR}/{version}.rst", VERSION_JAVA)
    patches = (lambda text: patch_release_notes(text, version, released_on),
               lambda text: patch_version_java(text, version))
    for path, patch in zip(paths, patches):
        file = root / path
        if not file.is_file():
            discard_branch(root, branch, previous_branch)
            fail(f"{path} does not exist on origin/{base}")
        try:
            file.write_text(patch(file.read_text()))
        except ValueError as e:
            discard_branch(root, branch, previous_branch)
            fail(str(e))
        print(f"Updated {path}")

    run("git", "add", "--", *paths, cwd=root)
    run("git", "commit", "-m", f"Release {version}", cwd=root, quiet=False)
    print(f"Pushing {branch} to origin...")
    run("git", "push", "--set-upstream", "origin", branch, cwd=root, quiet=False)

    repo = run("gh", "repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner", cwd=root)
    title = quote(f"Release {version}")
    print(f"""
Open the pull request:

    https://github.com/{repo}/compare/{base}...{branch}?expand=1&title={title}

Once it is merged, tag the release with ./devs/tools/create_tag.sh and add a
version bump commit (see devs/docs/release.rst).""")


if __name__ == "__main__":
    main()
