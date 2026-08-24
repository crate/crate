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

""" tools to prepare CrateDB release commits

Two sub-commands, each taking a version like 6.5.1 and opening a pull
request with a single commit:

- ``bump``: creates a ``bump-<version>`` branch off ``origin/<major>.<minor>``,
  containing a "Bump version to <version>-SNAPSHOT" commit which:

  - sets the version in all ``pom.xml`` files to ``<version>`` by running
    ``./mvnw versions:set``

  - adds a ``V_<version>`` constant with the snapshot flag set to
    ``server/src/main/java/org/elasticsearch/Version.java`` and makes it
    ``CURRENT``

  - adds an "Unreleased" ``docs/appendices/release-notes/<version>.rst`` and
    lists it in ``docs/appendices/release-notes/index.rst``

  - updates the version of the reindex example in
    ``docs/admin/system-information.rst``

- ``create``: creates a ``release-<version>`` branch off
  ``origin/<major>.<minor>``, containing a "Release <version>" commit which:

  - finalizes ``docs/appendices/release-notes/<version>.rst``: removes the
    " - Unreleased" title suffix, the ".. comment" instructions and the "in
    development" note, and adds a "Released on <today>." line instead

  - clears the snapshot flag of the ``V_<version>`` constant in
    ``server/src/main/java/org/elasticsearch/Version.java``

Usage::

    ./devs/tools/release.py bump 6.5.1
    ./devs/tools/release.py create 6.4.1
"""

import datetime
import re
import subprocess
import sys
from argparse import ArgumentParser
from pathlib import Path
from textwrap import fill
from typing import Callable

VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")
NOTES_DIR = "docs/appendices/release-notes"
VERSION_JAVA = "server/src/main/java/org/elasticsearch/Version.java"
INDEX_RST = f"{NOTES_DIR}/index.rst"
SYSTEM_INFORMATION_RST = "docs/admin/system-information.rst"
DOCS_URL = "https://cratedb.com/docs/crate/reference/en/latest"

NOTES_TEMPLATE = """\
.. _version_{version}:

{marker}
Version {version} - Unreleased
{marker}

.. comment 1. Remove the " - Unreleased" from the header above and adjust the ==
.. comment 2. Remove the NOTE below and replace with: "Released on 20XX-XX-XX."
.. comment    (without a NOTE entry, simply starting from col 1 of the line)
.. NOTE::

    In development. {version} isn't released yet. These are the release notes for
    the upcoming release.

.. NOTE::

    If you are upgrading a cluster, you must be running CrateDB {minimum} or higher
    before you upgrade to {version}.

    We recommend that you upgrade to the latest {previous_series} release before moving to
    {version}.

{rolling_upgrade}
    Before upgrading, you should `back up your data`_.

.. WARNING::

    Tables that were created before CrateDB {previous_major}.x will not function with {major}.x
    and must be recreated before moving to {major}.x.x.

    You can recreate tables using ``COPY TO`` and ``COPY FROM`` or by
    `inserting the data into a new table`_.

.. _back up your data: {docs_url}/admin/snapshots.html
.. _inserting the data into a new table: {docs_url}/admin/system-information.html#tables-need-to-be-recreated

.. rubric:: Table of contents

.. contents::
   :local:


{series_reference}

Fixes
=====

None
"""


def run(*args, cwd, capture_output=True):
    """Run a given command.

    If ``capture_output``, the output of the command is captured and returned.
    """
    if not capture_output:
        subprocess.check_call(args, cwd=cwd)
        return ""
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def repo_root(script):
    """Root of the checkout the given script file lives in"""
    return Path(run("git", "rev-parse", "--show-toplevel", cwd=Path(script).resolve().parent))


def ref_exists(root, ref):
    return subprocess.run(
        ("git", "rev-parse", "--verify", "--quiet", ref),
        cwd=root,
        stdout=subprocess.DEVNULL,
    ).returncode == 0


def fetch_and_check(root, base, branch):
    """Verify the checkout is ready to create ``branch`` off ``origin/base``"""
    if run("git", "status", "--porcelain", cwd=root):
        sys.exit("working directory not clean, commit or stash your changes first")
    print("Fetching origin...")
    run("git", "fetch", "origin", cwd=root)
    if not ref_exists(root, f"refs/remotes/origin/{base}"):
        sys.exit(f"origin/{base} does not exist, is there a {base} release branch?")
    for ref in (f"refs/heads/{branch}", f"refs/remotes/origin/{branch}"):
        if ref_exists(root, ref):
            sys.exit(f"{ref} already exists, delete it or finish that release first")


def create_branch(root, branch, base):
    """Check out ``branch`` at ``origin/base``, returning the previous branch"""
    print(f"Creating branch {branch} from origin/{base}...")
    previous = run("git", "rev-parse", "--abbrev-ref", "HEAD", cwd=root)
    if previous == "HEAD":  # detached, remember the commit instead
        previous = run("git", "rev-parse", "HEAD", cwd=root)
    run("git", "checkout", "-b", branch, f"origin/{base}", cwd=root, capture_output=False)
    return previous


def commit_and_push(root, branch, message):
    run("git", "add", "--all", cwd=root)
    run("git", "commit", "-m", message, cwd=root, capture_output=False)
    print(f"Pushing {branch} to origin...")
    run("git", "push", "--set-upstream", "origin", branch, cwd=root, capture_output=False)


def open_pull_request(root, base, branch, title):
    """Create the pull request of ``branch`` against ``base``"""
    print(f"Creating the pull request of {branch}...")
    run("gh", "pr", "create", "--base", base, "--head", branch,
        "--title", title, "--body", "", cwd=root, capture_output=False)


def apply_patch(path, patch_file: Callable[[str], str]):
    """Rewrite ``path`` in place with ``patch_file(text)``"""
    if not path.is_file():
        sys.exit(f"{path} does not exist")
    try:
        path.write_text(patch_file(path.read_text()))
    except ValueError as e:
        sys.exit(str(e))
    print(f"Updated {path}")


def render_release_notes(version, previous_notes):
    """Render the "Unreleased" release notes of ``version``

    The upgrade requirements cannot be derived from the version, they are taken
    from ``previous_notes``, the notes of the previous patch version of the same
    series.
    """
    major, minor, _ = version.split(".")
    series = f"{major}.{minor}"

    def extract(name, pattern):
        match = re.search(pattern, previous_notes, re.MULTILINE | re.DOTALL)
        if match is None:
            raise ValueError(f"cannot tell the {name} from the previous release notes")
        return match

    minimum = extract("minimum version", r"you must be running CrateDB (\S+) or higher").group(1)
    previous_series = extract("previous series", r"upgrade to the latest (\S+) release").group(1)
    # the whole rolling upgrade paragraph is carried over, only the version it
    # upgrades to changes; some series add sentences about upgrade restrictions
    rolling_upgrade = extract("rolling upgrade support",
                              r"^( *A rolling upgrade from .+? to )\S+( is supported\..*?)"
                              r"(?=\n *Before upgrading)").expand(rf"\g<1>{version}\g<2>")

    title = f"Version {version} - Unreleased"
    reference = (f"See the :ref:`version_{series}.0` release notes for a full list of "
                 f"changes in the {series} series.")
    return NOTES_TEMPLATE.format(
        version=version,
        marker="=" * len(title),
        major=major,
        previous_major=int(major) - 1,
        docs_url=DOCS_URL,
        series_reference=fill(reference, width=80),
        minimum=minimum,
        previous_series=previous_series,
        rolling_upgrade=rolling_upgrade,
    )


def patch_index(text, version, previous):
    """List ``version`` above ``previous`` in the release notes index"""
    entry = f"    {version}\n"
    if entry in text:
        raise ValueError(f"{version} is already listed in {INDEX_RST}")
    previous_entry = f"    {previous}\n"
    if previous_entry not in text:
        raise ValueError(f"{previous} is not listed in {INDEX_RST}")
    return text.replace(previous_entry, entry + previous_entry, 1)


def patch_system_information(text, version, previous):
    """Update the version of the reindex example, keeping the table aligned"""
    cell = re.compile(r"^(\s*\| )(\d+\.\d+\.\d+)( +)\|$", re.MULTILINE)
    matches = cell.findall(text)
    if len(matches) != 1:
        raise ValueError(f"expected one version cell in {SYSTEM_INFORMATION_RST}, "
                         f"found {len(matches)}")
    _, found, padding = matches[0]
    if found != previous:
        raise ValueError(f"the reindex example in {SYSTEM_INFORMATION_RST} shows {found}, "
                         f"not {previous}")
    width = len(found) + len(padding)
    return cell.sub(lambda m: f"{m.group(1)}{version.ljust(width)}|", text, count=1)


def add_version_constant(text, version, previous):
    """Add a snapshot ``V_<version>`` constant and make it ``CURRENT``"""
    constant = "V_" + version.replace(".", "_")
    previous_constant = "V_" + previous.replace(".", "_")
    if re.search(rf"^\s*public static final Version {constant} = ", text, re.MULTILINE):
        raise ValueError(f"{constant} already exists in {VERSION_JAVA}")

    declaration = re.compile(
        r"^\s*public static final Version " + previous_constant
        + r" = new Version\((?P<id>\w+), (?P<snapshot>true|false), (?P<lucene>.+)\);$",
        re.MULTILINE)
    match = declaration.search(text)
    if match is None:
        raise ValueError(f"no {previous_constant} constant in {VERSION_JAVA}")
    if match.group("snapshot") == "true":
        raise ValueError(f"{previous_constant} in {VERSION_JAVA} is still a snapshot, "
                         f"release {previous} first")

    # ids look like 9_04_01_99, only the patch part changes
    id_parts = match.group("id").split("_")
    id_parts[2] = f"{int(version.split('.')[2]):02d}"
    added = (f"    public static final Version {constant} = "
             f"new Version({'_'.join(id_parts)}, true, {match.group('lucene')});")
    text = f"{text[:match.end()]}\n{added}{text[match.end():]}"

    current = re.compile(
        r"^(\s*public static final Version CURRENT = )" + previous_constant + r";$",
        re.MULTILINE)
    if current.search(text) is None:
        raise ValueError(f"CURRENT is not {previous_constant} in {VERSION_JAVA}")
    return current.sub(rf"\g<1>{constant};", text, count=1)


def set_pom_version(root, version, previous):
    """Set the version in all pom.xml files, using the maven versions plugin"""
    pom = (root / "pom.xml").read_text()
    match = re.search(r"^    <version>(\S+)</version>$", pom, re.MULTILINE)
    if match is None:
        raise ValueError("cannot find the project version in pom.xml")
    if match.group(1) != previous:
        raise ValueError(f"pom.xml is at {match.group(1)}, not at {previous}")

    print(f"Setting the pom.xml version to {version}...")
    run("./mvnw", "--quiet", "versions:set", f"-DnewVersion={version}", cwd=root, capture_output=False)


def bump(version):
    major, minor, patch = (int(part) for part in version.split("."))
    if patch == 0:
        sys.exit(f"{version} is not a patch version, bump to x.y.0 versions manually")
    base = f"{major}.{minor}"
    previous = f"{base}.{patch - 1}"
    branch = f"bump-{version}"

    root = repo_root(__file__)
    fetch_and_check(root, base, branch)
    create_branch(root, branch, base)

    notes = f"{NOTES_DIR}/{version}.rst"
    try:
        if (root / notes).is_file():
            raise ValueError(f"{notes} already exists")
        previous_notes = root / f"{NOTES_DIR}/{previous}.rst"
        if not previous_notes.is_file():
            raise ValueError(f"{previous_notes.name} does not exist on origin/{base}")

        updates = {notes: render_release_notes(version, previous_notes.read_text())}
        for path, patch_file in (
            (INDEX_RST, patch_index),
            (SYSTEM_INFORMATION_RST, patch_system_information),
            (VERSION_JAVA, add_version_constant),
        ):
            updates[path] = patch_file((root / path).read_text(), version, previous)

        set_pom_version(root, version, previous)
        for path, content in updates.items():
            (root / path).write_text(content)
            print(f"Updated {path}")
    except Exception as e:
        sys.exit(str(e))

    commit_and_push(root, branch, f"Bump version to {version}-SNAPSHOT")
    open_pull_request(root, base, branch, f"Bump version to {version}-SNAPSHOT")


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


def create(version):
    base = ".".join(version.split(".")[:2])
    branch = f"release-{version}"
    released_on = datetime.datetime.now(tz=datetime.UTC).date()

    root = repo_root(__file__)
    fetch_and_check(root, base, branch)
    create_branch(root, branch, base)

    apply_patch(root / f"{NOTES_DIR}/{version}.rst",
                lambda text: patch_release_notes(text, version, released_on))
    apply_patch(root / VERSION_JAVA, lambda text: patch_version_java(text, version))

    commit_and_push(root, branch, f"Release {version}")
    open_pull_request(root, base, branch, f"Release {version}")


def main():
    parser = ArgumentParser(description=__doc__.strip().splitlines()[0])
    subparsers = parser.add_subparsers(dest="command", required=True)

    bump_parser = subparsers.add_parser(
        "bump", help='prepare the "Bump version to <version>-SNAPSHOT" commit')
    bump_parser.add_argument("version", help='a valid semantic version')

    create_parser = subparsers.add_parser(
        "create", help='prepare the "Release <version>" commit')
    create_parser.add_argument("version", help='a valid semantic version')

    args = parser.parse_args()
    if VERSION_RE.match(args.version) is None:
        sys.exit(f"invalid version '{args.version}', expected <major>.<minor>.<patch>")

    if args.command == "bump":
        bump(args.version)
    else:
        create(args.version)


if __name__ == "__main__":
    main()
